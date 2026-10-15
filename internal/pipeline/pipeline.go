// Package pipeline binds a NATS consumer to a sink.Sink: it receives
// messages one at a time, batches them, hands each batch to the sink, and
// acks/naks/terms the underlying messages based on the outcome. It is the
// only package that knows about both NATS and sink.Sink — sinks themselves
// stay broker-agnostic.
//
// An Orchestrator pulls from one shared durable JetStream consumer (see
// natsconn.EnsureConsumer) via jetstream.Consumer.Consume — a background
// pull loop invoking a callback. JetStream itself load-balances pulls
// across however many Orchestrators (in this process or any other replica)
// call in concurrently.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config"
	"condense.zeliot.in/nats-kafka-sink-connector/internal/sink"

	"github.com/nats-io/nats.go/jetstream"
)

type Orchestrator struct {
	consumer jetstream.Consumer

	js               jetstream.JetStream
	sink             sink.Sink
	natsSourceConfig config.NatsSourceConfig

	itemCh chan batchItem
}

// New creates an Orchestrator that pulls from the shared durable JetStream
// consumer (see natsconn.EnsureConsumer).
func New(consumer jetstream.Consumer, js jetstream.JetStream, sk sink.Sink, natsSourceConfig config.NatsSourceConfig) *Orchestrator {
	return &Orchestrator{
		consumer:         consumer,
		js:               js,
		sink:             sk,
		natsSourceConfig: natsSourceConfig,
	}
}

// Run starts consuming via jetstream.Consumer.Consume and blocks until ctx
// is cancelled or the consumer itself reports an unrecoverable error, then
// lets whatever batch is in flight or still buffered finish flushing
// (bounded by cfg.ShutdownGrace) before returning.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.itemCh = make(chan batchItem, o.natsSourceConfig.BatchMaxMessages)

	handler := func(msg jetstream.Msg) { o.handleMessage(ctx, msg) }
	consumeCtx, err := o.consumer.Consume(handler,
		jetstream.PullMaxMessages(o.natsSourceConfig.BatchMaxMessages),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			log.Printf("pipeline: consume error: %v", err)
		}),
	)
	if err != nil {
		return fmt.Errorf("pipeline: start consuming: %w", err)
	}

	timer := time.NewTimer(o.natsSourceConfig.BatchMaxWait)
	defer timer.Stop()

	var batch []batchItem
	runErr := o.consumeUntilDone(ctx, consumeCtx, timer, &batch)

	// The consumer's own lifecycle isn't tied to ctx, so it must be stopped
	// explicitly before returning.
	consumeCtx.Stop()

	// Whatever had already been received but not yet batched is unacked
	// either way, so finish it rather than leaving it for redelivery.
	o.drainPending(&batch)
	if len(batch) > 0 {
		flushCtx, cancel := context.WithTimeout(context.Background(), o.natsSourceConfig.ShutdownGrace)
		defer cancel()
		o.flush(flushCtx, batch)
	}

	return runErr
}

// consumeUntilDone runs the batching loop: it accumulates messages received
// on o.itemCh into batch, flushing it once it reaches cfg.BatchMaxMessages or
// cfg.BatchMaxWait has elapsed since the timer was last reset, until ctx is
// cancelled or the consumer itself stops.
func (o *Orchestrator) consumeUntilDone(ctx context.Context, consumeCtx jetstream.ConsumeContext, timer *time.Timer, batch *[]batchItem) error {
	for {
		select {
		case item := <-o.itemCh:
			*batch = append(*batch, item)
			if len(*batch) >= o.natsSourceConfig.BatchMaxMessages {
				o.flush(ctx, *batch)
				*batch = nil
				stopTimer(timer)
				timer.Reset(o.natsSourceConfig.BatchMaxWait)
			}

		case <-timer.C:
			if len(*batch) > 0 {
				o.flush(ctx, *batch)
				*batch = nil
			}
			timer.Reset(o.natsSourceConfig.BatchMaxWait)

		case <-consumeCtx.Closed():
			return nil

		case <-ctx.Done():
			return nil
		}
	}
}

// drainPending appends every message already queued on o.itemCh to batch
// without blocking, for a final flush during shutdown.
func (o *Orchestrator) drainPending(batch *[]batchItem) {
	for {
		select {
		case item := <-o.itemCh:
			*batch = append(*batch, item)
		default:
			return
		}
	}
}

func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

type batchItem struct {
	msg    jetstream.Msg
	record *sink.Record
}

// handleMessage is the per-message callback invoked for every message this
// Orchestrator receives. It's queued for the batching loop in Run.
func (o *Orchestrator) handleMessage(ctx context.Context, msg jetstream.Msg) {
	item := batchItem{msg: msg, record: newRecord(msg)}
	select {
	case o.itemCh <- item:
	case <-ctx.Done():
	}
}

func newRecord(msg jetstream.Msg) *sink.Record {
	meta := sink.Metadata{Subject: msg.Subject(), Headers: map[string][]string(msg.Headers())}
	if md, err := msg.Metadata(); err == nil && md != nil {
		meta.StreamSequence = md.Sequence.Stream
		meta.NumDelivered = md.NumDelivered
		meta.Timestamp = md.Timestamp
	}
	return sink.NewRecord(msg.Data(), meta)
}

func (o *Orchestrator) flush(ctx context.Context, items []batchItem) {
	records := make([]*sink.Record, len(items))
	for i, it := range items {
		records[i] = it.record
	}

	err := o.sink.Write(ctx, records)
	if err == nil {
		for _, it := range items {
			ack(it.msg)
		}
		return
	}

	var partial *sink.PartialError
	if errors.As(err, &partial) {
		for i, it := range items {
			if cause, failed := partial.FailedIndices[i]; failed {
				o.handleFailure(it, cause)
			} else {
				ack(it.msg)
			}
		}
		return
	}

	log.Printf("pipeline: sink write failed for %d message(s), retrying whole batch: %v", len(items), err)
	for _, it := range items {
		o.handleFailure(it, err)
	}
}

func (o *Orchestrator) handleFailure(it batchItem, cause error) {
	meta, metaErr := it.msg.Metadata()
	if metaErr == nil && o.natsSourceConfig.MaxDeliver > 0 && meta.NumDelivered >= uint64(o.natsSourceConfig.MaxDeliver) {
		log.Printf("pipeline: dropping message after %d delivery attempts: %v", meta.NumDelivered, cause)
		if err := it.msg.Term(); err != nil {
			log.Printf("pipeline: term failed: %v", err)
		}
		return
	}

	var delivered uint64
	if metaErr == nil {
		delivered = meta.NumDelivered
	}
	if err := it.msg.NakWithDelay(backoffFor(delivered)); err != nil {
		log.Printf("pipeline: nak failed: %v", err)
	}
}

func ack(msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		log.Printf("pipeline: ack failed: %v", err)
	}
}

func backoffFor(numDelivered uint64) time.Duration {
	const maxBackoff = 30 * time.Second
	d := time.Duration(numDelivered) * time.Second
	if d < time.Second {
		return time.Second
	}
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

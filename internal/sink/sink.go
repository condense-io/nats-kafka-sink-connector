// Package sink defines the boundary between the NATS-specific pull/batch
// pipeline and whatever downstream system a batch gets written to. Nothing
// in this package imports nats.go: a Sink implementation never sees a jetstream.Msg and can't ack/nak
// anything — that stays the pipeline's job — which is what keeps this
// interface swappable across brokers, not just across sinks.
package sink

import (
	"context"
	"fmt"
	"time"
)

// Metadata carries transport information a Sink may use (for example, to
// derive an idempotency key) without depending on the transport itself.
type Metadata struct {
	Subject        string
	StreamSequence uint64
	NumDelivered   uint64
	Headers        map[string][]string
	Timestamp      time.Time
}

// Record is one message handed to a Sink. This connector never decodes the
// payload — Kafka is a byte-for-byte pass-through, so Raw is all a Sink
// needs.
type Record struct {
	Raw  []byte
	Meta Metadata
}

func NewRecord(raw []byte, meta Metadata) *Record {
	return &Record{Raw: raw, Meta: meta}
}

// Sink persists a batch of records to a downstream system.
type Sink interface {
	// Write persists an entire batch. A nil return means every record in
	// the batch is durably committed. A plain (non-*PartialError) error
	// means nothing in the batch should be considered committed. Sinks
	// that can distinguish per-record outcomes (e.g. an unordered bulk
	// write where 999 of 1000 documents land) should return *PartialError
	// instead, so the caller only retries the records that actually failed.
	Write(ctx context.Context, batch []*Record) error

	// Name identifies the sink for logs/metrics.
	Name() string

	// Ready reports whether the sink can currently accept writes (e.g. a
	// ping to the backing store). Consulted by the readiness probe.
	Ready(ctx context.Context) error

	// Close releases any resources held by the sink.
	Close(ctx context.Context) error
}

// PartialError reports that some, but not all, records in a batch failed.
// FailedIndices maps a batch index to the error that occurred for it;
// every index absent from the map is considered committed.
type PartialError struct {
	FailedIndices map[int]error
}

func (e *PartialError) Error() string {
	return fmt.Sprintf("partial batch failure: %d record(s) failed", len(e.FailedIndices))
}

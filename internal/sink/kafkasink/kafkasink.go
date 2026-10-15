// Package kafkasink implements sink.Sink for Kafka, using franz-go.
//
// Every write is a plain produce;
// idempotency is limited to what franz-go's idempotent producer gives for
// free (deduping retries of the same send via a broker-tracked sequence
// number) plus the message Key carrying the same stable identity if the target topic is configured with
// cleanup.policy=compact — it is not an entity-level "latest value wins"
// upsert. This connector is at-least-once, with occasional Kafka-side
// duplicates on that redelivery-during-produce edge case
package kafkasink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config"
	"condense.zeliot.in/nats-kafka-sink-connector/internal/sink"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// writeTimeout bounds how long Write waits for delivery confirmation on a
// batch. Without this, an unreachable broker (or one that's silently
// stuck retrying, e.g. against an internal-only advertised address) leaves
// Write blocked forever: franz-go retries indefinitely by default, so a
// promise that never fires never surfaces as an error and never gets
// logged — the batch just hangs with no visible symptom other than a
// growing NATS backlog. This is a fixed constant rather than a passed-in
// ctx deadline because during normal (non-shutdown) operation the
// pipeline's ctx has no deadline of its own — relying on ctx alone would
// mean the exact silent-hang failure mode this constant exists to prevent.
const writeTimeout = 30 * time.Second

type Sink struct {
	client           *kgo.Client
	topic            string
	kafkaKeyStrategy string
}

// New builds a Sink. batchMaxMessages sizes the producer's in-flight record
// buffer — see newKafkaClient — and is passed separately from cfg because it
// comes from the NATS-side batching config (NatsSourceConfig.BatchMaxMessages),
// not from anything Kafka-specific.
func New(cfg config.KafkaSinkConfig, batchMaxMessages int) (*Sink, error) {
	client, err := newKafkaClient(cfg, batchMaxMessages)
	if err != nil {
		return nil, fmt.Errorf("kafkasink: %w", err)
	}

	return &Sink{
		client:           client,
		topic:            cfg.KafkaTopic,
		kafkaKeyStrategy: cfg.KafkaKeyStrategy,
	}, nil
}

// maxBufferedBytes caps the client's produce buffer by size, independent of
// maxBufferedRecords below — a safety net so a single batch of
// larger-than-typical messages can't blow past the pod's memory limit even
// while still under the record-count cap. Revisit alongside the pod's
// memory limit if actual message sizes turn out much larger than assumed
// here.
const maxBufferedBytes = 256 * 1024 * 1024

func newKafkaClient(cfg config.KafkaSinkConfig, batchMaxMessages int) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(cfg.KafkaBootstrapServers, ",")...),
		// The pipeline hands the sink one BatchMaxMessages-sized batch at a
		// time and waits for it to fully complete before starting the next,
		// so at most one such batch is ever outstanding. Sized to twice that
		// rather than exactly that: Write produces the whole batch up front,
		// so a buffer of exactly one batch is full by the time the last
		// records are produced, and those Produce calls then block until
		// earlier records have drained into produce requests. The headroom
		// removes that self-inflicted backpressure (franz-go's own default
		// is a flat 10000).
		kgo.MaxBufferedRecords(2 * batchMaxMessages),
		kgo.MaxBufferedBytes(maxBufferedBytes),
		// Produce uncompressed: nothing else in this ecosystem compresses,
		// and on a CPU-constrained pod the cycles snappy (franz-go's default)
		// costs are worth more than the bandwidth it saves.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}

	if cfg.KafkaSecurityProtocol == "SASL_PLAINTEXT" {
		mechanism, err := scramMechanism(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	if cfg.KafkaTlsEnabled {
		tlsConfig, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	return kgo.NewClient(opts...)
}

// buildTLSConfig assembles a *tls.Config from the KAFKA_TLS_* settings.
// Leaving the CA unset falls back to the system's default trust store
// (correct for managed Kafka with publicly-signed broker certificates); a
// client cert/key pair is only added for mutual TLS, and is only present
// here at all because LoadKafkaSinkConfig already validated that they're
// set together, not just one.
func buildTLSConfig(cfg config.KafkaSinkConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.KafkaTlsInsecureSkipVerify}

	if cfg.KafkaTlsCaString != "" {
		caPEM, err := loadPEM(cfg.KafkaTlsCaString)
		if err != nil {
			return nil, fmt.Errorf("kafkasink: read Kafka TLS CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("kafkasink: KAFKA_TLS_CA_FILE/KAFKA_TLS_CA does not contain a valid PEM certificate")
		}
		tlsConfig.RootCAs = pool
	}

	if cfg.KafkaTlsMode == "MUTUAL_TLS" && cfg.KafkaTlsCertString != "" {
		certPEM, err := loadPEM(cfg.KafkaTlsCertString)
		if err != nil {
			return nil, fmt.Errorf("kafkasink: read Kafka TLS client certificate: %w", err)
		}
		keyPEM, err := loadPEM(cfg.KafkaTlsKeyString)
		if err != nil {
			return nil, fmt.Errorf("kafkasink: read Kafka TLS client key: %w", err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("kafkasink: parse Kafka TLS client certificate/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// loadPEM reads PEM content from filePath if set, otherwise from inline,
// treating inline as raw PEM with any escaped "\n" sequences unescaped
// (env vars commonly can't carry literal newlines).
func loadPEM(inline string) ([]byte, error) {
	return []byte(strings.ReplaceAll(inline, "\\n", "\n")), nil
}

func scramMechanism(cfg config.KafkaSinkConfig) (sasl.Mechanism, error) {
	auth := scram.Auth{User: cfg.KafkaUsername, Pass: cfg.KafkaPassword}
	switch cfg.KafkaSCRAMAuthMechanism {
	case "SHA256":
		return auth.AsSha256Mechanism(), nil
	case "SHA512":
		return auth.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafkasink: unsupported KAFKA_SCRAM_AUTH_MECHANISM %q (want SHA256 or SHA512)", cfg.KafkaSCRAMAuthMechanism)
	}
}

func (s *Sink) Name() string { return "kafka" }

func (s *Sink) Ready(ctx context.Context) error {
	return s.client.Ping(ctx)
}

func (s *Sink) Close(ctx context.Context) error {
	if err := s.client.Flush(ctx); err != nil {
		log.Printf("kafkasink: flush before close: %v", err)
	}
	s.client.Close()
	return nil
}

// Write produces every record in the batch concurrently and waits for all
// delivery outcomes before returning, bounded by writeTimeout. Each
// record's own promise closure captures its batch index directly, rather
// than trying to correlate results back through kgo.ProduceResults — that
// type's order reflects delivery-completion order, not input order
// (confirmed against franz-go's source: ProduceSync's promise appends to
// its results slice as each record's own delivery completes), so
// index-based correlation there would silently mismatch results under
// concurrent completions across partitions.
func (s *Sink) Write(ctx context.Context, batch []*sink.Record) error {
	if len(batch) == 0 {
		return nil
	}

	// Bounds the Produce calls below too, not just the wait after them:
	// franz-go blocks Produce when the client's buffer is full, and that
	// block is otherwise governed only by the caller's ctx — which, per the
	// writeTimeout doc comment, has no deadline of its own during normal
	// operation. Without this, a full buffer (e.g. a genuine broker outage)
	// could wedge this call, and every other worker sharing this client,
	// indefinitely rather than surfacing as a bounded failure.
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	var (
		wg          sync.WaitGroup
		mu          sync.Mutex
		failed      = make(map[int]error)
		outstanding = make(map[int]bool, len(batch))
	)

	// Populated fully before any Produce call below so that a fast broker
	// firing early delivery callbacks (each of which deletes from this map
	// under mu) can never race against this loop still inserting into it —
	// inserting and deleting the same map from different goroutines without
	// both sides holding mu is a data race even if it doesn't always crash.
	for i := range batch {
		outstanding[i] = true
	}

	wg.Add(len(batch))
	for i, rec := range batch {
		key := idempotencyKey(s.kafkaKeyStrategy, rec.Meta)
		record := &kgo.Record{
			Topic:   s.topic,
			Key:     []byte(key),
			Value:   rec.Raw,
			Headers: kafkaHeaders(rec.Meta),
		}

		idx := i
		s.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
			defer wg.Done()
			mu.Lock()
			delete(outstanding, idx)
			if err != nil {
				failed[idx] = err
			}
			mu.Unlock()
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// The underlying produce attempts aren't necessarily aborted by
		// this — franz-go may still be retrying them in the background,
		// and their promises may still fire later into a Write call that
		// has already returned. That's harmless (the map is just no
		// longer read by anyone) and consistent with this connector's
		// existing at-least-once posture: we stop waiting, tell the
		// pipeline these are failed so NATS redelivers them, and accept
		// the same possible-duplicate tradeoff already documented for
		// the shutdown path above.
		mu.Lock()
		stuck := len(outstanding)
		for idx := range outstanding {
			failed[idx] = fmt.Errorf("no delivery confirmation within %s (broker unreachable or overloaded)", writeTimeout)
		}
		mu.Unlock()
		log.Printf("kafkasink: %d/%d message(s) had no delivery confirmation within %s — broker may be unreachable or the advertised listener address may not be routable from this pod; treating as failed for NATS redelivery", stuck, len(batch), writeTimeout)
	}

	switch {
	case len(failed) == 0:
		return nil
	case len(failed) == len(batch):
		return fmt.Errorf("kafkasink: all %d message(s) in batch failed: %w", len(batch), anyError(failed))
	default:
		return &sink.PartialError{FailedIndices: failed}
	}
}

// kafkaHeaders copies every NATS header onto the Kafka record and adds
// Nats-Message-Sequence unconditionally — even for messages published
// without any headers of their own — since it's what downstream dedup
// keys off when the target topic isn't compacted (see README's Kafka key
// strategy section).
func kafkaHeaders(meta sink.Metadata) []kgo.RecordHeader {
	headers := make([]kgo.RecordHeader, 0, len(meta.Headers)+1)
	for k, values := range meta.Headers {
		for _, v := range values {
			headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}
	headers = append(headers, kgo.RecordHeader{Key: "Nats-Message-Sequence", Value: fmt.Appendf(nil, "%d", meta.StreamSequence)})
	return headers
}

func idempotencyKey(kafkaKeyStrategy string, meta sink.Metadata) string {
	if kafkaKeyStrategy == "SUBJECT_NAME" {
		return meta.Subject
	} else {
		return ""
	}
}

func anyError(failed map[int]error) error {
	for _, err := range failed {
		return err
	}
	log.Println("kafkasink: anyError called with an empty map — this is a bug")
	return nil
}

package natsconn

import (
	"context"
	"fmt"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config"

	"github.com/nats-io/nats.go/jetstream"
)

// EnsureConsumer creates or updates the single ordinary durable JetStream
// pull consumer this connector reads from via Consumer.Consume (see
// pipeline.Orchestrator.Run) — named after ConsumerGroupName, on
// StreamName, filtered to StreamSubjects. AckPolicy is always
// AckExplicitPolicy: the pipeline acks, naks, or terms every message
// individually based on the sink write's outcome, so any other policy
// would silently break that at-least-once handling.
//
// The consumer isn't scoped to one pod: every replica calls Consume on the
// very same durable consumer, and JetStream itself load-balances pull
// requests across however many replicas are pulling concurrently — there's
// no partitioning and no per-replica identity, so there's nothing to clean
// up on shutdown.
//
// CreateOrUpdateConsumer is idempotent and safe to call from every replica
// concurrently: they all compute the same config from the same env vars, so
// concurrent calls either create the consumer once or converge on updating
// it to the same settings.
func EnsureConsumer(ctx context.Context, js jetstream.JetStream, natsSourceConfig config.NatsSourceConfig) (jetstream.Consumer, error) {
	var consumer jetstream.Consumer
	err := withRetry(ctx, 5, func() error {
		c, err := js.CreateOrUpdateConsumer(ctx, natsSourceConfig.StreamName, jetstream.ConsumerConfig{
			Durable:       natsSourceConfig.ConsumerName,
			FilterSubject: natsSourceConfig.StreamSubjects,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       natsSourceConfig.AckWait,
			MaxDeliver:    natsSourceConfig.MaxDeliver,
			MaxAckPending: natsSourceConfig.MaxAckPending,
			DeliverPolicy: jetstream.DeliverPolicy(natsSourceConfig.DeliverPolicy),
		})
		if err != nil {
			return fmt.Errorf("create or update durable consumer %q on stream %q: %w", natsSourceConfig.ConsumerName, natsSourceConfig.StreamName, err)
		}
		consumer = c
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("natsconn: ensure consumer: %w", err)
	}

	return consumer, nil
}

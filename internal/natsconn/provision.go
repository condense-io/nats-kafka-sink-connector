package natsconn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config"

	"github.com/nats-io/nats.go/jetstream"
)

// permanentError marks an error from withRetry's fn as non-retryable —
// retrying it again could never succeed (e.g. the server has definitively
// told us a resource doesn't exist), so withRetry returns the wrapped error
// immediately instead of burning through its remaining attempts.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// withRetry retries fn up to maxAttempts times with exponential backoff
// (2s, 4s, 8s, capped at 15s between attempts), for absorbing transient
// NATS unavailability at startup — e.g. many replicas restarting together
// and all hitting the JetStream management API's default 5s per-call
// timeout around the same moment. It returns immediately, without
// retrying, if fn returns a *permanentError, and stops early if ctx is
// cancelled.
func withRetry(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}

		var perm *permanentError
		if errors.As(err, &perm) {
			return perm.err
		}
		if attempt == maxAttempts {
			break
		}

		const maxBackoff = 15 * time.Second
		backoff := time.Duration(1<<uint(attempt)) * time.Second // 2s, 4s, 8s, 16s...
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		log.Printf("natsconn: attempt %d/%d failed: %v — retrying in %s", attempt, maxAttempts, err, backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// VerifyStream looks up the target stream and confirms it already covers
// every subject this connector expects to consume. The stream itself is
// provisioned out of band by a separate management service/UI — this
// connector never creates or updates it, only verifies it's in the shape
// it needs before starting the consumer. A missing stream or an uncovered
// subject is treated as a startup-fatal misconfiguration: the caller is
// expected to return this error up through main's normal shutdown path
// (which still closes the NATS connection cleanly) rather than run a
// consumer that will never receive anything.
func VerifyStream(ctx context.Context, js jetstream.JetStream, natsSourceConfig config.NatsSourceConfig) (jetstream.JetStream, error) {
	var stream jetstream.Stream
	err := withRetry(ctx, 5, func() error {
		s, err := js.Stream(ctx, natsSourceConfig.StreamName)
		if err != nil {
			if errors.Is(err, jetstream.ErrStreamNotFound) {
				return &permanentError{fmt.Errorf("natsconn: stream %q does not exist on the NATS server — it must be created by the stream owner before this connector can start", natsSourceConfig.StreamName)}
			}
			return fmt.Errorf("natsconn: look up stream %q: %w", natsSourceConfig.StreamName, err)
		}
		stream = s
		return nil
	})
	if err != nil {
		return nil, err
	}

	streamSubjects := stream.CachedInfo().Config.Subjects

	if !coveredByAny(natsSourceConfig.StreamSubjects, streamSubjects) {
		return nil, fmt.Errorf("natsconn: stream %q does not cover expected subject %q (stream subjects: %v) — ask the stream owner to add it", natsSourceConfig.StreamName, natsSourceConfig.StreamSubjects, streamSubjects)
	}

	return js, nil
}

// coveredByAny reports whether want is matched by at least one of the
// stream's subject filters.
func coveredByAny(want string, streamSubjects []string) bool {
	for _, s := range streamSubjects {
		if subjectsOverlap(want, s) {
			return true
		}
	}
	return false
}

// subjectsOverlap reports whether any concrete subject could match both a
// and b, treating "*" as a single-token wildcard and ">" as a
// matches-the-rest wildcard — the same tokens NATS itself gives special
// meaning to in a subject filter.
func subjectsOverlap(a, b string) bool {
	ta := strings.Split(a, ".")
	tb := strings.Split(b, ".")
	i, j := 0, 0
	for i < len(ta) && j < len(tb) {
		if ta[i] == ">" || tb[j] == ">" {
			return true
		}
		if ta[i] != "*" && tb[j] != "*" && ta[i] != tb[j] {
			return false
		}
		i++
		j++
	}
	return i == len(ta) && j == len(tb)
}

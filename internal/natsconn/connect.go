// Package natsconn owns the single NATS connection and JetStream context
// used by the whole connector, plus idempotent stream/consumer provisioning.
package natsconn

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// ConnState tracks NATS connection health so the health package can expose
// it on /readyz without polling nc.Status() (which has a race window
// between a poll and a real disconnect). It's updated only from the
// connection-state callbacks registered in Connect.
type ConnState struct {
	connected atomic.Bool
}

func (s *ConnState) Connected() bool { return s.connected.Load() }
func (s *ConnState) set(v bool)      { s.connected.Store(v) }

// Connect opens the one NATS connection this process uses for its lifetime
// and wraps it in a JetStream context. Callers must Close the returned
// connection during shutdown.
func Connect(natsSourceConfig config.NatsSourceConfig) (*nats.Conn, jetstream.JetStream, *ConnState, error) {
	state := &ConnState{}
	opts := []nats.Option{
		nats.Name(natsSourceConfig.NatsClientName),
		nats.Timeout(10 * time.Second),
		nats.MaxReconnects(-1), // ride out NATS blips instead of crash-looping the pod
		nats.ReconnectWait(2 * time.Second),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			state.set(false)
			log.Printf("natsconn: disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			state.set(true)
			log.Printf("natsconn: reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			state.set(false)
			log.Println("natsconn: connection closed")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			log.Printf("natsconn: async error (subject=%v): %v", subjectOf(sub), err)
		}),
	}

	if natsSourceConfig.NatsTlsEnabled {
		tlsConfig, err := setNatsTlsConfig(natsSourceConfig)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("natsconn: set TLS config: %w", err)
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	opts, err := setNatsAuthConfig(opts, natsSourceConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("natsconn: set auth config: %w", err)
	}

	nc, err := nats.Connect(natsSourceConfig.NatsUrl, opts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("natsconn: connect: %w", err)
	}
	state.set(true)

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, nil, fmt.Errorf("natsconn: jetstream: %w", err)
	}

	return nc, js, state, nil
}

// setNatsTlsConfig configures NATS TLS based on the provided configuration from the environment variables.
func setNatsTlsConfig(cfg config.NatsSourceConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{}
	if cfg.NatsTlsEnabled && cfg.NatsTlsCaFileContent != "" {
		caPEM := strings.ReplaceAll(cfg.NatsTlsCaFileContent, "\\n", "\n") // if env stores escaped newlines

		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM([]byte(caPEM)); !ok {
			return nil, fmt.Errorf("failed to parse NATS CA PEM from env")
		}

		tlsConfig.RootCAs = pool
		// no client cert => certCB is nil
		//opts = append(opts, nats.ClientTLSConfig(nil, rootCAsCB))
		if cfg.NatsTlsMode == "MUTUAL_TLS" && cfg.NatsTlsCertFileContent != "" && cfg.NatsTlsKeyFileContent != "" {
			certPEM := strings.ReplaceAll(cfg.NatsTlsCertFileContent, "\\n", "\n") // if env stores escaped newlines
			keyPEM := strings.ReplaceAll(cfg.NatsTlsKeyFileContent, "\\n", "\n")
			cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
			if err != nil {
				return nil, fmt.Errorf("failed to parse NATS client certificate and key: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}
	tlsConfig.InsecureSkipVerify = cfg.NatsTlsInsecureSkipVerify

	return tlsConfig, nil
}

// setNatsAuthConfig configures NATS authentication based on the provided configuration from the environment variables.
func setNatsAuthConfig(opts []nats.Option, cfg config.NatsSourceConfig) ([]nats.Option, error) {
	switch cfg.NatsAuthMechanism {
	case "TOKEN":
		if cfg.NatsToken != "" {
			opts = append(opts, nats.Token(cfg.NatsToken))
		}
	case "USERNAME_PASSWORD":
		if cfg.NatsUsername != "" && cfg.NatsPassword != "" {
			opts = append(opts, nats.UserInfo(cfg.NatsUsername, cfg.NatsPassword))
		}
	case "JWT_NKEY_FILE":
		if cfg.NatsCredsFile != "" {
			opts = append(opts, nats.UserCredentials(cfg.NatsCredsFile))
		} else if cfg.NatsJwtNKeyCreds != "" {
			opts = append(opts, nats.UserCredentialBytes([]byte(cfg.NatsJwtNKeyCreds)))
		}
	case "JWT_NKEY":
		if cfg.NatsJwt != "" && cfg.NatsNKey != "" {
			opts = append(opts, nats.UserJWTAndSeed(cfg.NatsJwt, cfg.NatsNKey))
		}
	case "NKEY":
		if cfg.NatsUserSeed != "" {
			seed := []byte(cfg.NatsUserSeed)
			kp, err := nkeys.FromSeed(seed)
			if err != nil {
				return nil, fmt.Errorf("failed to create NKEY from seed: %w", err)
			}
			publicKey, err := kp.PublicKey()
			if err != nil {
				return nil, fmt.Errorf("failed to get public key from NKEY: %w", err)
			}
			signatureHandler := func(nonce []byte) ([]byte, error) {
				return kp.Sign(nonce)
			}
			opts = append(opts, nats.Nkey(publicKey, signatureHandler))
		}
	}
	return opts, nil
}

// Close drains the connection so any pending outbound work flushes before
// the underlying network connection closes.
func Close(nc *nats.Conn) {
	if nc == nil {
		return
	}
	if err := nc.Drain(); err != nil {
		log.Printf("natsconn: drain failed, closing directly: %v", err)
		nc.Close()
	}
}

func subjectOf(sub *nats.Subscription) string {
	if sub == nil {
		return ""
	}
	return sub.Subject
}

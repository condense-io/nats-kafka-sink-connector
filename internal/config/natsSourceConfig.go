package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config/utils"
)

type NatsSourceConfig struct {
	NatsUrl        string
	NatsClientName string

	//NATS Auth Mechanism can be one of "JWT_NKEY_FILE", "TOKEN", or "USERNAME_PASSWORD". If set, the corresponding credentials must be provided via the appropriate env vars below. If unset, no auth is used.
	NatsAuthMechanism string

	NatsUsername string
	NatsPassword string

	NatsJwtNKeyCreds string
	NatsCredsFile    string

	NatsJwt  string
	NatsNKey string

	NatsToken    string
	NatsUserSeed string

	//NATS TLS Settings
	NatsTlsEnabled       bool
	NatsTlsCaFileContent string

	NatsTlsMode               string
	NatsTlsInsecureSkipVerify bool
	NatsTlsCertFileContent    string
	NatsTlsKeyFileContent     string
	NatsTlsServerName         string

	// StreamName is provisioned out of band, by a separate management
	// service/UI — this connector only looks it up and verifies it covers
	// StreamSubjects, it never creates or modifies it.
	StreamName     string
	StreamSubjects string

	// ConsumerName names the single durable JetStream pull consumer
	// this connector creates (idempotently) on StreamName — see
	// natsconn.EnsureConsumer. Every replica binds to this same durable and
	// pulls from it via jetstream.Consumer.Consume; JetStream itself
	// load-balances pull requests across however many replicas are pulling
	// concurrently.
	ConsumerName string

	BatchMaxMessages  int
	BatchMaxWait      time.Duration
	AckWait           time.Duration
	MaxDeliver        int
	MaxAckPending     int
	ShutdownGrace     time.Duration
	NatsDeliverPolicy string
	DeliverPolicy     int

	HealthAddr string
	LogLevel   string

	// PprofEnabled exposes Go's profiling endpoints on the health server.
	// Off by default — see health.registerPprof for the cost and exposure
	// this carries.
	PprofEnabled bool
}

func LoadNatsSourceConfig() (NatsSourceConfig, error) {

	var errs []string
	req := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, key+" is required")
		}
		return v
	}

	natsSourceConfig := NatsSourceConfig{}

	natsSourceConfig = NatsSourceConfig{
		NatsUrl:        req("NATS_URL"),
		NatsCredsFile:  os.Getenv("NATS_CREDS_FILE"),
		NatsClientName: utils.GetEnv("NATS_CLIENT_NAME", "consumer-service"),

		StreamName:     req("STREAM_NAME"),
		StreamSubjects: req("STREAM_SUBJECTS"),
		ConsumerName:   req("CONSUMER_NAME"),

		HealthAddr:        utils.GetEnv("HEALTH_ADDR", ":8080"),
		LogLevel:          utils.GetEnv("LOG_LEVEL", "info"),
		NatsDeliverPolicy: utils.GetEnv("NATS_DELIVER_POLICY", "all"),
	}

	if len(natsSourceConfig.StreamSubjects) == 0 {
		errs = append(errs, "STREAM_SUBJECTS is required (used to verify the pre-provisioned stream covers what this connector expects)")
	}

	natsSourceConfig.NatsAuthMechanism = utils.GetEnv("NATS_AUTH_MECHANISM", "")
	if natsSourceConfig.NatsAuthMechanism == "USERNAME_PASSWORD" {
		natsSourceConfig.NatsUsername = req("NATS_USERNAME")
		natsSourceConfig.NatsPassword = req("NATS_PASSWORD")
		if natsSourceConfig.NatsCredsFile != "" {
			errs = append(errs, "set only one of NATS_AUTH_MECHANISM or NATS_CREDS_FILE, not both")
		}
	} else if natsSourceConfig.NatsAuthMechanism == "JWT_NKEY_FILE" {
		natsSourceConfig.NatsJwtNKeyCreds = os.Getenv("NATS_CREDS")
		if natsSourceConfig.NatsCredsFile == "" && natsSourceConfig.NatsJwtNKeyCreds == "" {
			errs = append(errs, "NATS_CREDS_FILE or NATS_CREDS is required for JWT_NKEY_FILE authentication")
		}
	} else if natsSourceConfig.NatsAuthMechanism == "TOKEN" {
		natsSourceConfig.NatsToken = req("NATS_TOKEN")
	} else if natsSourceConfig.NatsAuthMechanism == "JWT_NKEY" {
		natsSourceConfig.NatsJwt = os.Getenv("NATS_JWT")
		natsSourceConfig.NatsNKey = os.Getenv("NATS_NKEY")
		if natsSourceConfig.NatsJwt == "" || natsSourceConfig.NatsNKey == "" {
			errs = append(errs, "NATS_JWT and NATS_NKEY are required for JWT_NKEY authentication")
		}
	} else if natsSourceConfig.NatsAuthMechanism == "NKEY" {
		natsSourceConfig.NatsUserSeed = os.Getenv("NATS_USER_SEED")
		if natsSourceConfig.NatsUserSeed == "" {
			errs = append(errs, "NATS_USER_SEED is required for NKEY authentication")
		}
	} else if natsSourceConfig.NatsAuthMechanism == "NONE" {
		// No authentication is required for "none" mechanism.
	} else if natsSourceConfig.NatsAuthMechanism != "" {
		errs = append(errs, fmt.Sprintf("unsupported NATS_AUTH_MECHANISM %q (want \"JWT_NKEY_FILE\", \"JWT_NKEY\", \"TOKEN\", \"USERNAME_PASSWORD\", or \"none\")", natsSourceConfig.NatsAuthMechanism))
	}

	if natsSourceConfig.NatsCredsFile != "" && natsSourceConfig.NatsAuthMechanism != "JWT_NKEY_FILE" {
		errs = append(errs, "NATS_CREDS_FILE is set but NATS_AUTH_MECHANISM is not \"JWT_NKEY_FILE\"")
	}

	natsSourceConfig.NatsTlsEnabled = utils.GetEnvBool("NATS_TLS_ENABLED", false)
	natsSourceConfig.NatsTlsCaFileContent = os.Getenv("NATS_TLS_CA")
	if natsSourceConfig.NatsTlsEnabled {
		if natsSourceConfig.NatsTlsCaFileContent == "" {
			errs = append(errs, "NATS_TLS_CA is required when NATS_TLS_ENABLED=true")
		}
	}

	natsSourceConfig.NatsTlsMode = utils.GetEnv("NATS_TLS_MODE", "")

	if natsSourceConfig.NatsTlsEnabled {
		switch natsSourceConfig.NatsTlsMode {
		case "ONE_WAY_TLS":
			if os.Getenv("NATS_TLS_CERT") != "" || os.Getenv("NATS_TLS_KEY") != "" {
				log.Println("config: NATS_TLS_CERT/NATS_TLS_KEY are set but NATS_TLS_MODE is not MUTUAL_TLS — ignoring them")
			}
		case "MUTUAL_TLS":
			natsSourceConfig.NatsTlsCertFileContent = os.Getenv("NATS_TLS_CERT")
			natsSourceConfig.NatsTlsKeyFileContent = os.Getenv("NATS_TLS_KEY")
			if !natsSourceConfig.NatsTlsEnabled {
				errs = append(errs, "NATS_TLS_MODE=MUTUAL_TLS requires NATS_TLS_ENABLED=true")
			}
			if natsSourceConfig.NatsTlsCertFileContent == "" {
				errs = append(errs, "NATS_TLS_CERT is required for mTLS when NATS_TLS_MODE=MUTUAL_TLS")
			}
			if natsSourceConfig.NatsTlsKeyFileContent == "" {
				errs = append(errs, "NATS_TLS_KEY is required for mTLS when NATS_TLS_MODE=MUTUAL_TLS")
			}

		default:
			errs = append(errs, fmt.Sprintf("NATS_TLS_MODE must be ONE_WAY_TLS or MUTUAL_TLS, got %q", natsSourceConfig.NatsTlsMode))
		}
	}

	natsSourceConfig.NatsTlsInsecureSkipVerify = utils.GetEnvBool("NATS_TLS_INSECURE_SKIP_VERIFY", false)

	var err error
	if natsSourceConfig.BatchMaxMessages, err = utils.GetEnvInt("BATCH_MAX_MESSAGES", 1000); err != nil {
		errs = append(errs, err.Error())
	}
	if natsSourceConfig.BatchMaxWait, err = utils.GetEnvDuration("BATCH_MAX_WAIT", 2*time.Second); err != nil {
		errs = append(errs, err.Error())
	}
	if natsSourceConfig.AckWait, err = utils.GetEnvDuration("ACK_WAIT", 60*time.Second); err != nil {
		errs = append(errs, err.Error())
	}
	if natsSourceConfig.MaxDeliver, err = utils.GetEnvInt("MAX_DELIVER", 5); err != nil {
		errs = append(errs, err.Error())
	}
	if natsSourceConfig.MaxAckPending, err = utils.GetEnvInt("MAX_ACK_PENDING", 50000); err != nil {
		errs = append(errs, err.Error())
	}
	if natsSourceConfig.ShutdownGrace, err = utils.GetEnvDuration("SHUTDOWN_GRACE_PERIOD", 25*time.Second); err != nil {
		errs = append(errs, err.Error())
	}

	natsSourceConfig.PprofEnabled = utils.GetEnvBool("PPROF_ENABLED", false)

	if natsSourceConfig.NatsDeliverPolicy == "" || natsSourceConfig.NatsDeliverPolicy != "all" && natsSourceConfig.NatsDeliverPolicy != "last" && natsSourceConfig.NatsDeliverPolicy != "new" {
		errs = append(errs, "NATS_DELIVER_POLICY must be either \"all\", \"last\" or \"new\"")
	}
	switch natsSourceConfig.NatsDeliverPolicy {
	case "all":
		natsSourceConfig.DeliverPolicy = 0 // jetstream.DeliverAllPolicy
	case "last":
		natsSourceConfig.DeliverPolicy = 1 // jetstream.DeliverLastPolicy
	case "new":
		natsSourceConfig.DeliverPolicy = 2 // jetstream.DeliverNewPolicy
	}

	if len(errs) > 0 {
		return natsSourceConfig, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}

	log.Println("NATS source configuration loaded successfully")
	log.Println("NATS source configuration details:")
	log.Printf("  NATS_URL: %s", natsSourceConfig.NatsUrl)
	log.Printf("  NATS_AUTH_MECHANISM: %s", natsSourceConfig.NatsAuthMechanism)
	log.Printf("  NATS_CREDS_FILE: %s", natsSourceConfig.NatsCredsFile)
	log.Printf("  NATS_TLS_ENABLED: %t", natsSourceConfig.NatsTlsEnabled)
	log.Printf("  NATS_TLS_CA: %s", natsSourceConfig.NatsTlsCaFileContent)
	log.Printf("  BATCH_MAX_MESSAGES: %d", natsSourceConfig.BatchMaxMessages)
	log.Printf("  BATCH_MAX_WAIT: %s", natsSourceConfig.BatchMaxWait)
	log.Printf("  ACK_WAIT: %s", natsSourceConfig.AckWait)
	log.Printf("  MAX_DELIVER: %d", natsSourceConfig.MaxDeliver)
	log.Printf("  MAX_ACK_PENDING: %d", natsSourceConfig.MaxAckPending)
	log.Printf("  SHUTDOWN_GRACE_PERIOD: %s", natsSourceConfig.ShutdownGrace)
	log.Printf("  NATS_CONSUME_POLICY: %s", natsSourceConfig.NatsDeliverPolicy)
	log.Printf("  PPROF_ENABLED: %t", natsSourceConfig.PprofEnabled)
	return natsSourceConfig, nil
}

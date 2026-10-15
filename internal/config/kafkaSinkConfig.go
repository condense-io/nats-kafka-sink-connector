package config

import (
	"fmt"
	"log"
	"os"
	"strings"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config/utils"
)

type KafkaSinkConfig struct {
	KafkaBootstrapServers string
	KafkaTopic            string

	// KafkaSecurityProtocol selects how the client authenticates to Kafka —
	// exactly one of "NONE" (default, no authentication) or
	// "SASL_PLAINTEXT" (SASL/SCRAM, requires KafkaUsername/KafkaPassword).
	KafkaSecurityProtocol   string
	KafkaSCRAMAuthMechanism string
	KafkaUsername           string
	KafkaPassword           string

	// KafkaTlsEnabled turns on TLS for the connection to the brokers,
	// independently of KafkaSecurityProtocol — e.g. SASL_PLAINTEXT with TLS
	// enabled carries SASL over an encrypted connection (what Kafka itself
	// calls SASL_SSL), while NONE with TLS enabled is server-only TLS
	// with no SASL layer (Kafka's SSL). Must be the exact string "true" —
	// this isn't parsed as a general boolean.
	KafkaTlsEnabled  bool
	KafkaTlsCaString string

	KafkaTlsMode       string
	KafkaTlsCertString string
	KafkaTlsKeyString  string

	// KafkaTlsInsecureSkipVerify disables broker certificate verification.
	// Only ever intended for testing against a self-signed dev cluster —
	// never enable this in production, as it makes the connection
	// vulnerable to on-path tampering.
	KafkaTlsInsecureSkipVerify bool

	// KafkaKeyStrategy is derived, not user-configurable: it's
	// "SUBJECT_NAME" (key the produced record on the originating NATS
	// subject) whenever StreamSubjects contains a wildcard — i.e. more than
	// one distinct concrete subject can be consumed, so the subject is
	// worth keying on for downstream partitioning/compaction — otherwise
	// "NULL_KEY", since every message already shares one literal subject
	// and keying on it would just pile everything onto a single partition.
	KafkaKeyStrategy string
}

// LoadKafkaSinkConfig reads Kafka sink configuration from the environment.
// streamSubjects is the already-loaded NatsSourceConfig.StreamSubjects,
// needed only to derive KafkaKeyStrategy.
func LoadKafkaSinkConfig(streamSubjects string) (KafkaSinkConfig, error) {

	var errs []string
	req := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, key+" is required")
		}
		return v
	}

	kafkaSinkConfig := KafkaSinkConfig{
		KafkaBootstrapServers: req("KAFKA_BOOTSTRAP_SERVERS"),
		KafkaTopic:            req("KAFKA_TOPIC"),
	}

	kafkaSinkConfig.KafkaSecurityProtocol = strings.ToUpper(utils.GetEnv("KAFKA_SECURITY_PROTOCOL", "NONE"))
	switch kafkaSinkConfig.KafkaSecurityProtocol {
	case "NONE":
	case "SASL_PLAINTEXT":
		kafkaSinkConfig.KafkaUsername = req("KAFKA_USERNAME")
		kafkaSinkConfig.KafkaPassword = req("KAFKA_PASSWORD")
		kafkaSinkConfig.KafkaSCRAMAuthMechanism = strings.ToUpper(utils.GetEnv("KAFKA_SCRAM_AUTH_MECHANISM", "SHA512"))
		if kafkaSinkConfig.KafkaSCRAMAuthMechanism != "SHA256" && kafkaSinkConfig.KafkaSCRAMAuthMechanism != "SHA512" {
			errs = append(errs, fmt.Sprintf("KAFKA_SCRAM_AUTH_MECHANISM must be one of SHA256|SHA512, got %q", kafkaSinkConfig.KafkaSCRAMAuthMechanism))
		}
	default:
		errs = append(errs, fmt.Sprintf("KAFKA_SECURITY_PROTOCOL must be one of NONE|SASL_PLAINTEXT, got %q", kafkaSinkConfig.KafkaSecurityProtocol))
	}

	kafkaSinkConfig.KafkaTlsEnabled = utils.GetEnvBool("KAFKA_TLS_ENABLED", false)
	kafkaSinkConfig.KafkaTlsMode = utils.GetEnv("KAFKA_TLS_MODE", "")
	kafkaSinkConfig.KafkaTlsCaString = os.Getenv("KAFKA_TLS_CA")
	kafkaSinkConfig.KafkaTlsCertString = os.Getenv("KAFKA_TLS_CERT")
	kafkaSinkConfig.KafkaTlsKeyString = os.Getenv("KAFKA_TLS_KEY")
	kafkaSinkConfig.KafkaTlsInsecureSkipVerify = utils.GetEnvBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false)

	hasTlsCert := kafkaSinkConfig.KafkaTlsCertString != ""
	hasTlsKey := kafkaSinkConfig.KafkaTlsKeyString != ""
	if kafkaSinkConfig.KafkaTlsEnabled {
		switch kafkaSinkConfig.KafkaTlsMode {
		case "ONE_WAY_TLS":
			if hasTlsCert || hasTlsKey {
				log.Println("config: KAFKA_TLS_CERT/KAFKA_TLS_KEY are set but KAFKA_TLS_MODE is not MUTUAL_TLS — ignoring them")
			}
		case "MUTUAL_TLS":
			if !hasTlsCert || !hasTlsKey {
				errs = append(errs, "KAFKA_TLS_CERT and KAFKA_TLS_KEY are both required when KAFKA_TLS_MODE=MUTUAL_TLS")
			}
		default:
			errs = append(errs, fmt.Sprintf("KAFKA_TLS_MODE must be ONE_WAY_TLS or MUTUAL_TLS, got %q", kafkaSinkConfig.KafkaTlsMode))
		}
	} else if kafkaSinkConfig.KafkaTlsCaString != "" || hasTlsCert || hasTlsKey || kafkaSinkConfig.KafkaTlsMode != "" {
		errs = append(errs, "KAFKA_TLS_* settings are set but KAFKA_TLS_ENABLED is not true")
	}

	if strings.Contains(streamSubjects, ".*") || strings.Contains(streamSubjects, ".>") {
		kafkaSinkConfig.KafkaKeyStrategy = "SUBJECT_NAME"
	} else {
		kafkaSinkConfig.KafkaKeyStrategy = "NULL_KEY"
	}

	if len(errs) > 0 {
		return kafkaSinkConfig, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	log.Println("Kafka sink configuration loaded successfully")
	log.Println("Kafka sink configuration details:")
	log.Printf("  KafkaBootstrapServers: %s", kafkaSinkConfig.KafkaBootstrapServers)
	log.Printf("  KafkaTopic: %s", kafkaSinkConfig.KafkaTopic)
	log.Printf("  KafkaSecurityProtocol: %s", kafkaSinkConfig.KafkaSecurityProtocol)
	log.Printf("  KafkaTlsEnabled: %t", kafkaSinkConfig.KafkaTlsEnabled)
	if kafkaSinkConfig.KafkaTlsEnabled {
		log.Printf("  KafkaTlsCaConfigured: %t", kafkaSinkConfig.KafkaTlsCaString != "")
		log.Printf("  KafkaTlsMode: %s", kafkaSinkConfig.KafkaTlsMode)
		log.Printf("  KafkaTlsClientCertConfigured: %t", kafkaSinkConfig.KafkaTlsMode == "MUTUAL_TLS")
		log.Printf("  KafkaTlsInsecureSkipVerify: %t", kafkaSinkConfig.KafkaTlsInsecureSkipVerify)
	}
	log.Printf("  KafkaKeyStrategy: %s", kafkaSinkConfig.KafkaKeyStrategy)
	return kafkaSinkConfig, nil
}

// main is the composition root: load config, connect once, wire the
// pipeline together, run until asked to stop, then shut down in order.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"condense.zeliot.in/nats-kafka-sink-connector/internal/config"
	"condense.zeliot.in/nats-kafka-sink-connector/internal/health"
	"condense.zeliot.in/nats-kafka-sink-connector/internal/natsconn"
	"condense.zeliot.in/nats-kafka-sink-connector/internal/pipeline"
	"condense.zeliot.in/nats-kafka-sink-connector/internal/sink/kafkasink"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	natsSourceConfig, err := config.LoadNatsSourceConfig()
	if err != nil {
		return err
	}
	kafkaSinkConfig, err := config.LoadKafkaSinkConfig(natsSourceConfig.StreamSubjects)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-quit
		log.Printf("main: received %s, starting graceful shutdown", sig)
		cancel()
	}()

	nc, js, connState, err := natsconn.Connect(natsSourceConfig)
	if err != nil {
		return err
	}
	defer natsconn.Close(nc)

	stream, err := natsconn.VerifyStream(ctx, js, natsSourceConfig)
	if err != nil {
		return err
	}

	consumer, err := natsconn.EnsureConsumer(ctx, stream, natsSourceConfig)
	if err != nil {
		return err
	}

	sk, err := kafkasink.New(config.KafkaSinkConfig{
		KafkaBootstrapServers:      kafkaSinkConfig.KafkaBootstrapServers,
		KafkaTopic:                 kafkaSinkConfig.KafkaTopic,
		KafkaSecurityProtocol:      kafkaSinkConfig.KafkaSecurityProtocol,
		KafkaSCRAMAuthMechanism:    kafkaSinkConfig.KafkaSCRAMAuthMechanism,
		KafkaUsername:              kafkaSinkConfig.KafkaUsername,
		KafkaPassword:              kafkaSinkConfig.KafkaPassword,
		KafkaTlsEnabled:            kafkaSinkConfig.KafkaTlsEnabled,
		KafkaTlsCaString:           kafkaSinkConfig.KafkaTlsCaString,
		KafkaTlsCertString:         kafkaSinkConfig.KafkaTlsCertString,
		KafkaTlsKeyString:          kafkaSinkConfig.KafkaTlsKeyString,
		KafkaTlsInsecureSkipVerify: kafkaSinkConfig.KafkaTlsInsecureSkipVerify,
		KafkaKeyStrategy:           kafkaSinkConfig.KafkaKeyStrategy,
	}, natsSourceConfig.BatchMaxMessages)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), natsSourceConfig.ShutdownGrace)
		defer closeCancel()
		if err := sk.Close(closeCtx); err != nil {
			log.Printf("main: sink close error: %v", err)
		}
	}()

	healthSrv := health.NewServer(natsSourceConfig.HealthAddr, natsSourceConfig.PprofEnabled,
		func(ctx context.Context) error {
			if !connState.Connected() {
				return fmt.Errorf("nats: not connected")
			}
			return nil
		},
		sk.Ready,
	)

	pipelineConfig := config.NatsSourceConfig{
		BatchMaxMessages: natsSourceConfig.BatchMaxMessages,
		BatchMaxWait:     natsSourceConfig.BatchMaxWait,
		AckWait:          natsSourceConfig.AckWait,
		MaxDeliver:       natsSourceConfig.MaxDeliver,
		MaxAckPending:    natsSourceConfig.MaxAckPending,
		ShutdownGrace:    natsSourceConfig.ShutdownGrace,
		DeliverPolicy:    natsSourceConfig.DeliverPolicy,
	}

	healthDone := make(chan error, 1)
	go func() { healthDone <- healthSrv.Start(ctx) }()

	log.Printf("main: connector started (stream=%s consumer=%s sink=%s batch=%d)",
		natsSourceConfig.StreamName, natsSourceConfig.ConsumerName, sk.Name(), natsSourceConfig.BatchMaxMessages)

	orch := pipeline.New(consumer, stream, sk, pipelineConfig)
	orchErr := orch.Run(ctx)
	if orchErr != nil {
		log.Print(orchErr)
	}

	<-healthDone
	return orchErr
}

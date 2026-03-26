package eventbus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type KafkaEventBus struct {
	producer sarama.SyncProducer
	logger   *zap.Logger
	mu       sync.Mutex
	subs     []*kafkaConsumer
	brokers_ []string
}

type kafkaConsumer struct {
	cancel context.CancelFunc
}

type backoffCfg struct {
	max, current time.Duration
}

func (b *backoffCfg) NextBackOff() time.Duration {
	d := b.current
	if b.current < b.max {
		b.current *= 2
	}
	return d
}

func backoff2() *backoffCfg {
	return &backoffCfg{max: 30 * time.Second, current: 1 * time.Second}
}

func NewKafkaEventBus(brokers []string, logger *zap.Logger) *KafkaEventBus {
	if len(brokers) == 0 {
		brokers = []string{"kafka:9092"}
	}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		if logger != nil {
			logger.Warn("kafka producer init failed, will retry", zap.Error(err))
		}
		bo := backoff2()
		for {
			time.Sleep(bo.NextBackOff())
			producer, err = sarama.NewSyncProducer(brokers, cfg)
			if err == nil {
				break
			}
			if logger != nil {
				logger.Warn("kafka producer retry failed", zap.Error(err))
			}
		}
	}
	if logger != nil {
		logger.Info("kafka producer connected", zap.Strings("brokers", brokers))
	}
	return &KafkaEventBus{producer: producer, logger: logger, brokers_: brokers}
}

func (k *KafkaEventBus) Publish(ctx context.Context, topic string, msg proto.Message) error {
	start := time.Now()
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("kafka publish marshal: %w", err)
	}
	_, _, err = k.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(payload),
	})
	if k.logger != nil {
		k.logger.Debug("kafka publish", zap.String("topic", topic), zap.Int64("latency_ms", time.Since(start).Milliseconds()))
	}
	if err != nil {
		return fmt.Errorf("kafka publish send: %w", err)
	}
	return nil
}

func (k *KafkaEventBus) Subscribe(ctx context.Context, topic, group string, fn func(ctx context.Context, payload []byte)) error {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	client, err := sarama.NewConsumerGroup(k.brokers_, group, cfg)
	if err != nil {
		return fmt.Errorf("kafka subscribe consumer group: %w", err)
	}
	subCtx, cancel := context.WithCancel(ctx)
	k.mu.Lock()
	k.subs = append(k.subs, &kafkaConsumer{cancel: cancel})
	k.mu.Unlock()
	go func() {
		defer client.Close()
		handler := &consumerHandler{fn: fn, logger: k.logger}
		for {
			select {
			case <-subCtx.Done():
				return
			default:
			}
			if err := client.Consume(subCtx, []string{topic}, handler); err != nil {
				if k.logger != nil {
					k.logger.Warn("kafka consume error", zap.Error(err))
				}
				time.Sleep(time.Second)
			}
		}
	}()
	if k.logger != nil {
		k.logger.Info("kafka subscriber started", zap.String("topic", topic), zap.String("group", group))
	}
	return nil
}

func (k *KafkaEventBus) Ack(ctx context.Context, topic, group, id string) error {
	return nil
}

func (k *KafkaEventBus) Shutdown() error {
	k.mu.Lock()
	for _, s := range k.subs {
		s.cancel()
	}
	k.mu.Unlock()
	if k.producer != nil {
		return k.producer.Close()
	}
	return nil
}

type consumerHandler struct {
	fn     func(ctx context.Context, payload []byte)
	logger *zap.Logger
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		ctx := sess.Context()
		h.fn(ctx, msg.Value)
		sess.MarkMessage(msg, "")
	}
	return nil
}

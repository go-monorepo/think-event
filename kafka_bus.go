package event

// 注意：此文件从 services/users-service/internal/event 迁移而来
// 作为公共事件包供所有服务使用

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// KafkaEventBus Kafka事件总线实现
type KafkaEventBus struct {
	producer sarama.SyncProducer
	consumer sarama.ConsumerGroup
	handlers map[string][]EventHandler
	mu       sync.RWMutex
	logger   *zap.Logger
	topic    string
	groupID  string
}

// NewKafkaEventBus 创建Kafka事件总线
func NewKafkaEventBus(brokers []string, topic, groupID string, logger *zap.Logger) (*KafkaEventBus, error) {
	// 创建生产者配置
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Retry.Max = 5

	// 创建生产者
	producer, err := sarama.NewSyncProducer(brokers, producerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	// 创建消费者配置
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	consumerConfig.Version = sarama.V2_8_0_0

	// 创建消费者组
	consumer, err := sarama.NewConsumerGroup(brokers, groupID, consumerConfig)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	return &KafkaEventBus{
		producer: producer,
		consumer: consumer,
		handlers: make(map[string][]EventHandler),
		logger:   logger,
		topic:    topic,
		groupID:  groupID,
	}, nil
}

// Publish 发布事件
func (b *KafkaEventBus) Publish(ctx context.Context, event Event) error {
	// 序列化事件
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// 创建消息
	message := &sarama.ProducerMessage{
		Topic: b.topic,
		Key:   sarama.StringEncoder(event.Type()),
		Value: sarama.ByteEncoder(data),
		Headers: []sarama.RecordHeader{
			{Key: []byte("event_type"), Value: []byte(event.Type())},
			{Key: []byte("tenant_id"), Value: []byte(event.GetTenantID())},
		},
		Timestamp: event.Timestamp(),
	}

	// 发送消息
	partition, offset, err := b.producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	b.logger.Debug("event published",
		zap.String("event_type", event.Type()),
		zap.String("tenant_id", event.GetTenantID()),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return nil
}

// Subscribe 订阅事件
func (b *KafkaEventBus) Subscribe(eventType string, handler EventHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.handlers[eventType] = append(b.handlers[eventType], handler)
	b.logger.Info("event handler subscribed", zap.String("event_type", eventType))
	return nil
}

// Start 启动事件总线（启动消费者）
func (b *KafkaEventBus) Start(ctx context.Context) error {
	handler := &kafkaConsumerGroupHandler{
		bus:      b,
		logger:   b.logger,
		handlers: b.handlers,
	}

	// 在goroutine中启动消费者
	go func() {
		for {
			select {
			case <-ctx.Done():
				b.logger.Info("stopping kafka consumer")
				return
			default:
				if err := b.consumer.Consume(ctx, []string{b.topic}, handler); err != nil {
					b.logger.Error("error from consumer", zap.Error(err))
					time.Sleep(time.Second)
				}
			}
		}
	}()

	b.logger.Info("kafka event bus started", zap.String("topic", b.topic), zap.String("group_id", b.groupID))
	return nil
}

// Stop 停止事件总线
func (b *KafkaEventBus) Stop() error {
	var errs []error

	if err := b.producer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close producer: %w", err))
	}

	if err := b.consumer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close consumer: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors stopping kafka bus: %v", errs)
	}

	b.logger.Info("kafka event bus stopped")
	return nil
}

// kafkaConsumerGroupHandler Kafka消费者组处理器
type kafkaConsumerGroupHandler struct {
	bus      *KafkaEventBus
	logger   *zap.Logger
	handlers map[string][]EventHandler
}

// Setup 会话开始时的回调
func (h *kafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup 会话结束时的回调
func (h *kafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 消费消息
func (h *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// 解析事件类型
			eventType := ""
			for _, header := range message.Headers {
				if string(header.Key) == "event_type" {
					eventType = string(header.Value)
					break
				}
			}

			if eventType == "" {
				h.logger.Warn("message without event_type header", zap.Int64("offset", message.Offset))
				session.MarkMessage(message, "")
				continue
			}

			// 查找处理器
			h.bus.mu.RLock()
			handlers := h.handlers[eventType]
			h.bus.mu.RUnlock()

			if len(handlers) == 0 {
				h.logger.Warn("no handler for event type", zap.String("event_type", eventType))
				session.MarkMessage(message, "")
				continue
			}

			// 解析事件
			var eventData map[string]interface{}
			if err := json.Unmarshal(message.Value, &eventData); err != nil {
				h.logger.Error("failed to unmarshal event", zap.Error(err), zap.String("event_type", eventType))
				session.MarkMessage(message, "")
				continue
			}

			// 创建事件对象
			event := &genericEvent{
				eventType: eventType,
				payload:   eventData,
				timestamp: message.Timestamp,
			}

			// 执行处理器
			ctx := context.Background()
			if tenantID, ok := eventData["tenant_id"].(string); ok {
				ctx = context.WithValue(ctx, "tenant_id", tenantID)
			}

			for _, handler := range handlers {
				if err := handler(ctx, event); err != nil {
					h.logger.Error("event handler failed",
						zap.Error(err),
						zap.String("event_type", eventType),
						zap.Int64("offset", message.Offset),
					)
					// 继续处理其他处理器
				}
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// genericEvent 通用事件实现
type genericEvent struct {
	eventType string
	payload   map[string]interface{}
	timestamp time.Time
}

func (e *genericEvent) Type() string {
	return e.eventType
}

func (e *genericEvent) Payload() interface{} {
	return e.payload
}

func (e *genericEvent) GetTenantID() string {
	if tenantID, ok := e.payload["tenant_id"].(string); ok {
		return tenantID
	}
	return ""
}

func (e *genericEvent) Timestamp() time.Time {
	return e.timestamp
}

package idempotent

import (
	"context"
	"fmt"

	event "github.com/EladiaShipmenyrt89/think-event"
	"go.uber.org/zap"
)

// IdempotentEventHandler 幂等性事件处理器装饰器
// T 表示事件数据的类型
type IdempotentEventHandler[T any] struct {
	checker          IdempotencyChecker[T]
	handler          event.EventHandler
	eventIDExtractor func(event.Event) string // 从事件中提取唯一ID
	logger           *zap.Logger
	// onSkipped 当事件被跳过时的回调（可选）
	onSkipped func(ctx context.Context, eventID string, evt event.Event)
}

// IdempotentEventHandlerOption 配置选项
type IdempotentEventHandlerOption[T any] func(*IdempotentEventHandler[T])

// WithLogger 设置日志记录器
func WithLogger[T any](logger *zap.Logger) IdempotentEventHandlerOption[T] {
	return func(h *IdempotentEventHandler[T]) {
		h.logger = logger
	}
}

// WithOnSkipped 设置跳过回调
func WithOnSkipped[T any](onSkipped func(ctx context.Context, eventID string, evt event.Event)) IdempotentEventHandlerOption[T] {
	return func(h *IdempotentEventHandler[T]) {
		h.onSkipped = onSkipped
	}
}

// NewIdempotentEventHandler 创建幂等性事件处理器装饰器
// checker: 幂等性检查器
// handler: 原始事件处理器
// eventIDExtractor: 从事件中提取唯一ID的函数
func NewIdempotentEventHandler[T any](
	checker IdempotencyChecker[T],
	handler event.EventHandler,
	eventIDExtractor func(event.Event) string,
	options ...IdempotentEventHandlerOption[T],
) event.EventHandler {
	h := &IdempotentEventHandler[T]{
		checker:          checker,
		handler:          handler,
		eventIDExtractor: eventIDExtractor,
	}

	// 应用选项
	for _, opt := range options {
		opt(h)
	}

	return h.Handle
}

// Handle 处理事件（实现 event.EventHandler 接口）
func (h *IdempotentEventHandler[T]) Handle(ctx context.Context, evt event.Event) error {
	// 提取事件ID
	eventID := h.eventIDExtractor(evt)
	if eventID == "" {
		// 如果无法提取事件ID，记录警告但继续处理（可能是事件格式问题）
		if h.logger != nil {
			h.logger.Warn("无法提取事件ID，跳过幂等性检查",
				zap.String("event_type", evt.Type()),
			)
		}
		// 直接调用原始处理器
		return h.handler(ctx, evt)
	}

	// 检查是否已处理
	processed, err := h.checker.IsProcessed(ctx, eventID)
	if err != nil {
		// 检查失败，记录错误但继续处理（避免因检查失败导致事件丢失）
		if h.logger != nil {
			h.logger.Error("幂等性检查失败，继续处理事件",
				zap.String("event_id", eventID),
				zap.String("event_type", evt.Type()),
				zap.Error(err),
			)
		}
		// 继续处理事件
	} else if processed {
		// 事件已处理，跳过
		if h.logger != nil {
			h.logger.Info("事件已处理，跳过",
				zap.String("event_id", eventID),
				zap.String("event_type", evt.Type()),
			)
		}
		if h.onSkipped != nil {
			h.onSkipped(ctx, eventID, evt)
		}
		return nil
	}

	// 调用原始处理器
	err = h.handler(ctx, evt)
	if err != nil {
		// 处理失败，不记录为已处理
		return err
	}

	// 处理成功，记录为已处理
	// 尝试从事件中提取数据（如果可能）
	var data T
	if payload, ok := evt.Payload().(T); ok {
		data = payload
	}

	if recordErr := h.checker.RecordProcessed(ctx, eventID, data); recordErr != nil {
		// 记录失败，但不影响事件处理结果
		if h.logger != nil {
			h.logger.Warn("记录事件处理状态失败",
				zap.String("event_id", eventID),
				zap.String("event_type", evt.Type()),
				zap.Error(recordErr),
			)
		}
	}

	return nil
}

// SimpleIdempotentHandler 简化的幂等性处理器（不需要泛型）
// 适用于不需要记录事件数据的场景
type SimpleIdempotentHandler struct {
	checker          func(ctx context.Context, eventID string) (bool, error)
	handler          event.EventHandler
	eventIDExtractor func(event.Event) string
	logger           *zap.Logger
}

// NewSimpleIdempotentHandler 创建简化的幂等性处理器
func NewSimpleIdempotentHandler(
	checker func(ctx context.Context, eventID string) (bool, error),
	handler event.EventHandler,
	eventIDExtractor func(event.Event) string,
	logger *zap.Logger,
) event.EventHandler {
	return (&SimpleIdempotentHandler{
		checker:          checker,
		handler:          handler,
		eventIDExtractor: eventIDExtractor,
		logger:           logger,
	}).Handle
}

// Handle 处理事件
func (h *SimpleIdempotentHandler) Handle(ctx context.Context, evt event.Event) error {
	// 提取事件ID
	eventID := h.eventIDExtractor(evt)
	if eventID == "" {
		if h.logger != nil {
			h.logger.Warn("无法提取事件ID，跳过幂等性检查",
				zap.String("event_type", evt.Type()),
			)
		}
		return h.handler(ctx, evt)
	}

	// 检查是否已处理
	processed, err := h.checker(ctx, eventID)
	if err != nil {
		if h.logger != nil {
			h.logger.Error("幂等性检查失败，继续处理事件",
				zap.String("event_id", eventID),
				zap.String("event_type", evt.Type()),
				zap.Error(err),
			)
		}
	} else if processed {
		if h.logger != nil {
			h.logger.Info("事件已处理，跳过",
				zap.String("event_id", eventID),
				zap.String("event_type", evt.Type()),
			)
		}
		return nil
	}

	// 调用原始处理器
	return h.handler(ctx, evt)
}

// ExtractEventID 辅助函数：从常见事件类型中提取ID
func ExtractEventID(evt event.Event) string {
	// 尝试从事件负载中提取 tenant_id
	if payload, ok := evt.Payload().(map[string]interface{}); ok {
		if tenantID, ok := payload["tenant_id"].(string); ok && tenantID != "" {
			return fmt.Sprintf("%s:%s", evt.Type(), tenantID)
		}
	}

	// 尝试从事件负载中提取 id
	if payload, ok := evt.Payload().(map[string]interface{}); ok {
		if id, ok := payload["id"].(string); ok && id != "" {
			return fmt.Sprintf("%s:%s", evt.Type(), id)
		}
	}

	return ""
}

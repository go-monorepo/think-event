package event

import (
	"context"
	"time"
)

// PublishFailureHandler 发布重试耗尽且无 FallbackHandler 时的全局回调。
// 业务侧应在启动时设置，用于告警或把事件落盘补偿；
// 不设置时事件会被静默丢弃（与历史行为一致）。
var PublishFailureHandler func(event Event, err error)

// maxRetryInterval 指数退避的间隔上限，防止间隔无限翻倍甚至溢出。
const maxRetryInterval = 30 * time.Second

// HighAvailabilityConfig 高可用配置
type HighAvailabilityConfig struct {
	// 重试配置
	MaxRetries    int           // 最大重试次数
	RetryInterval time.Duration // 重试间隔
	RetryBackoff  bool          // 是否使用指数退避

	// 降级配置
	FallbackEnabled bool         // 是否启用降级
	FallbackHandler EventHandler // 降级处理器

	// 超时配置
	Timeout time.Duration // 事件处理超时时间
}

// DefaultHighAvailabilityConfig 默认高可用配置
func DefaultHighAvailabilityConfig() *HighAvailabilityConfig {
	return &HighAvailabilityConfig{
		MaxRetries:      3,
		RetryInterval:   time.Second,
		RetryBackoff:    true,
		FallbackEnabled: true,
		Timeout:         5 * time.Second,
	}
}

// HighAvailabilityEventFacade 高可用事件门面
type HighAvailabilityEventFacade struct {
	*EventFacade
	config *HighAvailabilityConfig
}

// NewHighAvailabilityEventFacade 创建高可用事件门面
func NewHighAvailabilityEventFacade(bus EventBus, config *HighAvailabilityConfig) *HighAvailabilityEventFacade {
	if config == nil {
		config = DefaultHighAvailabilityConfig()
	}

	return &HighAvailabilityEventFacade{
		EventFacade: NewEventFacade(bus),
		config:      config,
	}
}

// TriggerWithHA 高可用触发事件（带重试和降级）
func (f *HighAvailabilityEventFacade) TriggerWithHA(ctx context.Context, eventType string, event Event) error {
	resolvedType, err := f.resolveEventType(eventType, event)
	if err != nil {
		return err
	}

	// 1. 先执行本地监听器（同步，带超时）
	if err := f.triggerLocalWithTimeout(ctx, resolvedType, event); err != nil {
		// 本地监听器失败，记录日志但不阻塞
		_ = err
	}

	// 2. 异步发布到 Kafka / 事件总线（带重试）
	if f.bus != nil {
		go f.publishWithRetry(ctx, event)
	}

	return nil
}

// triggerLocalWithTimeout 带超时的本地监听器执行
func (f *HighAvailabilityEventFacade) triggerLocalWithTimeout(ctx context.Context, eventType string, event Event) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, f.config.Timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- f.EventFacade.triggerLocal(timeoutCtx, eventType, event)
	}()

	select {
	case err := <-done:
		return err
	case <-timeoutCtx.Done():
		// 超时，执行降级处理
		if f.config.FallbackEnabled && f.config.FallbackHandler != nil {
			return f.config.FallbackHandler(ctx, event)
		}
		return timeoutCtx.Err()
	}
}

// publishWithRetry 带重试的事件发布
func (f *HighAvailabilityEventFacade) publishWithRetry(ctx context.Context, event Event) {
	var err error
	retryInterval := f.config.RetryInterval

	for i := 0; i <= f.config.MaxRetries; i++ {
		if i > 0 {
			// 重试前等待
			time.Sleep(retryInterval)
			if f.config.RetryBackoff {
				// 指数退避（带上限）
				retryInterval *= 2
				if retryInterval > maxRetryInterval {
					retryInterval = maxRetryInterval
				}
			}
		}

		err = f.bus.Publish(ctx, event)
		if err == nil {
			// 发布成功
			return
		}
	}

	// 所有重试都失败，执行降级处理
	if f.config.FallbackEnabled && f.config.FallbackHandler != nil {
		_ = f.config.FallbackHandler(ctx, event)
		return
	}
	// 没有降级处理器时通知全局回调，避免事件被静默丢弃
	if PublishFailureHandler != nil {
		PublishFailureHandler(event, err)
	}
}

// TriggerAsyncWithHA 高可用异步触发事件
func (f *HighAvailabilityEventFacade) TriggerAsyncWithHA(ctx context.Context, eventType string, event Event) error {
	if _, err := f.resolveEventType(eventType, event); err != nil {
		return err
	}

	if f.bus == nil {
		if f.config.FallbackEnabled && f.config.FallbackHandler != nil {
			return f.config.FallbackHandler(ctx, event)
		}
		return nil
	}

	go f.publishWithRetry(ctx, event)
	return nil
}

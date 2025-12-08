package idempotent

import (
	"context"
	"fmt"
)

// IdempotencyChecker 幂等性检查器接口
// T 表示事件数据的类型
type IdempotencyChecker[T any] interface {
	// IsProcessed 检查事件是否已处理过
	// eventID: 事件的唯一标识符
	// 返回: true 表示已处理，false 表示未处理，error 表示检查过程中出错
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	// RecordProcessed 记录事件已处理
	// eventID: 事件的唯一标识符
	// data: 事件数据（可选，用于记录详细信息）
	RecordProcessed(ctx context.Context, eventID string, data T) error
}

// DataExistenceChecker 数据存在性检查器
// 通过检查业务数据是否已存在来判断事件是否已处理
type DataExistenceChecker[T any] struct {
	// CheckFunc 检查函数，返回 true 表示数据已存在（事件已处理）
	CheckFunc func(ctx context.Context) (bool, error)
	// RecordFunc 记录函数（可选），用于记录处理状态
	RecordFunc func(ctx context.Context, eventID string, data T) error
}

// NewDataExistenceChecker 创建数据存在性检查器
func NewDataExistenceChecker[T any](
	checkFunc func(ctx context.Context) (bool, error),
	recordFunc func(ctx context.Context, eventID string, data T) error,
) IdempotencyChecker[T] {
	return &DataExistenceChecker[T]{
		CheckFunc:  checkFunc,
		RecordFunc: recordFunc,
	}
}

// IsProcessed 检查事件是否已处理过
func (c *DataExistenceChecker[T]) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	if c.CheckFunc == nil {
		return false, fmt.Errorf("check function is nil")
	}
	return c.CheckFunc(ctx)
}

// RecordProcessed 记录事件已处理
func (c *DataExistenceChecker[T]) RecordProcessed(ctx context.Context, eventID string, data T) error {
	if c.RecordFunc == nil {
		// 如果没有提供记录函数，直接返回 nil（数据存在性检查不需要额外记录）
		return nil
	}
	return c.RecordFunc(ctx, eventID, data)
}

// ProcessRecordChecker 处理记录表检查器
// 使用类似 CurrencyCreationFailureRepository 的表记录处理状态
type ProcessRecordChecker[T any] struct {
	// IsProcessedFunc 检查函数，查询处理记录表
	IsProcessedFunc func(ctx context.Context, eventID string) (bool, error)
	// RecordFunc 记录函数，写入处理记录表
	RecordFunc func(ctx context.Context, eventID string, data T) error
}

// NewProcessRecordChecker 创建处理记录表检查器
func NewProcessRecordChecker[T any](
	isProcessedFunc func(ctx context.Context, eventID string) (bool, error),
	recordFunc func(ctx context.Context, eventID string, data T) error,
) IdempotencyChecker[T] {
	return &ProcessRecordChecker[T]{
		IsProcessedFunc: isProcessedFunc,
		RecordFunc:      recordFunc,
	}
}

// IsProcessed 检查事件是否已处理过
func (c *ProcessRecordChecker[T]) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	if c.IsProcessedFunc == nil {
		return false, fmt.Errorf("is processed function is nil")
	}
	return c.IsProcessedFunc(ctx, eventID)
}

// RecordProcessed 记录事件已处理
func (c *ProcessRecordChecker[T]) RecordProcessed(ctx context.Context, eventID string, data T) error {
	if c.RecordFunc == nil {
		return fmt.Errorf("record function is nil")
	}
	return c.RecordFunc(ctx, eventID, data)
}

// CompositeChecker 组合检查器
// 组合多种检查策略，提供多层防护
// 只要任一检查器返回已处理，就认为事件已处理
type CompositeChecker[T any] struct {
	checkers []IdempotencyChecker[T]
}

// NewCompositeChecker 创建组合检查器
func NewCompositeChecker[T any](checkers ...IdempotencyChecker[T]) IdempotencyChecker[T] {
	return &CompositeChecker[T]{
		checkers: checkers,
	}
}

// IsProcessed 检查事件是否已处理过
// 只要任一检查器返回已处理，就认为事件已处理
func (c *CompositeChecker[T]) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	for _, checker := range c.checkers {
		processed, err := checker.IsProcessed(ctx, eventID)
		if err != nil {
			// 如果某个检查器出错，继续检查其他检查器
			continue
		}
		if processed {
			return true, nil
		}
	}
	return false, nil
}

// RecordProcessed 记录事件已处理
// 尝试在所有检查器上记录，忽略错误（至少有一个成功即可）
func (c *CompositeChecker[T]) RecordProcessed(ctx context.Context, eventID string, data T) error {
	var lastErr error
	for _, checker := range c.checkers {
		if err := checker.RecordProcessed(ctx, eventID, data); err != nil {
			lastErr = err
			// 继续尝试其他检查器
			continue
		}
		// 至少有一个成功，返回 nil
		return nil
	}
	// 如果所有检查器都失败，返回最后一个错误
	if lastErr != nil {
		return lastErr
	}
	return nil
}




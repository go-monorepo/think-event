# 事件幂等性处理包

`pkg/event/idempotent` 提供了通用的事件幂等性处理机制，确保同一事件不会被重复处理。

## 功能特性

- **多种检查策略**：支持处理记录表、数据存在性检查、组合策略等
- **泛型设计**：使用 Go 泛型，类型安全
- **装饰器模式**：易于集成到现有事件处理器
- **灵活配置**：支持自定义事件 ID 提取、日志记录、跳过回调等

## 使用示例

### 1. 数据存在性检查器

适用于通过检查业务数据是否已存在来判断事件是否已处理的场景：

```go
import (
    "github.com/EladiaShipmenyrt89/think-event/idempotent"
)

// 创建数据存在性检查器
checker := idempotent.NewDataExistenceChecker(
    // 检查函数：检查数据是否已存在
    func(ctx context.Context) (bool, error) {
        categories, err := categoryRepo.List(ctx, nil, nil)
        return err == nil && len(categories) > 0, err
    },
    // 记录函数（可选）
    nil,
)

// 创建幂等性处理器
handler := idempotent.NewIdempotentEventHandler(
    checker,
    originalHandler.Handle,
    func(evt event.Event) string {
        // 从事件中提取唯一ID
        if payload, ok := evt.Payload().(*TenantCreatedEvent); ok {
            return fmt.Sprintf("tenant.created:%s", payload.TenantID)
        }
        return ""
    },
    idempotent.WithLogger(logger),
)

// 注册事件监听器
eventBus.Subscribe("tenant.created", handler)
```

### 2. 处理记录表检查器

适用于使用专门的处理记录表来记录事件处理状态的场景：

```go
// 创建处理记录表检查器
checker := idempotent.NewProcessRecordChecker(
    // 检查函数：查询处理记录表
    func(ctx context.Context, eventID string) (bool, error) {
        return failureRepo.IsProcessed(ctx, eventID)
    },
    // 记录函数：写入处理记录表
    func(ctx context.Context, eventID string, data TenantCreatedEvent) error {
        return failureRepo.RecordSuccess(ctx, eventID, data.TenantID, data.TenantCode, "")
    },
)

// 创建幂等性处理器
handler := idempotent.NewIdempotentEventHandler(
    checker,
    originalHandler.Handle,
    func(evt event.Event) string {
        if payload, ok := evt.Payload().(*TenantCreatedEvent); ok {
            return fmt.Sprintf("tenant.created:%s", payload.TenantID)
        }
        return ""
    },
    idempotent.WithLogger(logger),
)
```

### 3. 组合检查器

组合多种检查策略，提供多层防护：

```go
// 创建多个检查器
dataChecker := idempotent.NewDataExistenceChecker(...)
recordChecker := idempotent.NewProcessRecordChecker(...)

// 组合检查器
compositeChecker := idempotent.NewCompositeChecker(dataChecker, recordChecker)

// 创建幂等性处理器
handler := idempotent.NewIdempotentEventHandler(
    compositeChecker,
    originalHandler.Handle,
    idempotent.ExtractEventID, // 使用辅助函数提取事件ID
    idempotent.WithLogger(logger),
)
```

### 4. 简化处理器

对于不需要记录事件数据的场景，可以使用简化处理器：

```go
handler := idempotent.NewSimpleIdempotentHandler(
    // 检查函数
    func(ctx context.Context, eventID string) (bool, error) {
        return checkDataExists(ctx, eventID)
    },
    // 原始处理器
    originalHandler.Handle,
    // 事件ID提取函数
    idempotent.ExtractEventID,
    // 日志记录器
    logger,
)
```

## 检查策略说明

### 数据存在性策略 (DataExistenceChecker)

- **适用场景**：通过检查业务数据是否已存在来判断事件是否已处理
- **优点**：不需要额外的处理记录表，直接利用业务数据
- **缺点**：如果业务数据被删除，可能无法正确判断

### 处理记录表策略 (ProcessRecordChecker)

- **适用场景**：需要详细记录事件处理状态和失败信息的场景
- **优点**：可以记录详细的处理信息，支持重试机制
- **缺点**：需要额外的数据库表和维护

### 组合策略 (CompositeChecker)

- **适用场景**：需要多层防护的场景
- **优点**：提供更强的可靠性，即使某一层失效，其他层仍能保护
- **缺点**：性能开销稍大（需要多次检查）

## 最佳实践

1. **事件 ID 设计**：确保事件 ID 的唯一性和稳定性

   - 推荐格式：`{event_type}:{unique_identifier}`
   - 例如：`tenant.created:tenant_123`

2. **错误处理**：幂等性检查失败时，建议继续处理事件而不是直接返回错误

   - 避免因检查失败导致事件丢失
   - 可以通过数据库唯一约束作为最后防线

3. **日志记录**：建议启用日志记录，便于排查问题

   ```go
   handler := idempotent.NewIdempotentEventHandler(
       checker,
       originalHandler.Handle,
       eventIDExtractor,
       idempotent.WithLogger(logger),
   )
   ```

4. **跳过回调**：可以设置跳过回调，在事件被跳过时执行额外操作
   ```go
   handler := idempotent.NewIdempotentEventHandler(
       checker,
       originalHandler.Handle,
       eventIDExtractor,
       idempotent.WithLogger(logger),
       idempotent.WithOnSkipped(func(ctx context.Context, eventID string, evt event.Event) {
           // 执行额外操作，如发送通知等
       }),
   )
   ```

## 注意事项

1. **事件 ID 提取**：确保事件 ID 提取函数能够正确从事件中提取唯一标识符
2. **并发安全**：幂等性检查器本身不提供并发控制，需要依赖数据库唯一约束或其他机制
3. **性能考虑**：组合检查器会依次检查所有检查器，可能影响性能，建议根据实际需求选择

package event

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// RawPayloadEvent 暴露原始消息体，供强类型订阅按需解码。
type RawPayloadEvent interface {
	Event
	PayloadBytes() []byte
}

// JSONEventHandler 是带强类型 payload 的异步事件处理器。
type JSONEventHandler[T any] func(ctx context.Context, payload *T, event Event) error

// PayloadAs 将事件负载解码为指定类型，避免从 map 二次序列化再反序列化。
func PayloadAs[T any](evt Event) (*T, error) {
	if evt == nil {
		return nil, fmt.Errorf("event is nil")
	}

	payloadValue := evt.Payload()
	if payload, ok := payloadValue.(*T); ok {
		return payload, nil
	}
	if payload, ok := payloadValue.(T); ok {
		payloadCopy := payload
		return &payloadCopy, nil
	}

	if payload, err := decodePayloadBytes[T](payloadBytes(evt)); err == nil {
		return payload, nil
	}

	if payload, err := decodePayloadValue[T](payloadValue); err == nil {
		return payload, nil
	}

	return nil, fmt.Errorf("failed to decode payload for %s into target type", evt.Type())
}

// SubscribeJSON 注册带强类型 JSON payload 的异步订阅。
func SubscribeJSON[T any](bus EventBus, eventType string, handler JSONEventHandler[T]) error {
	if bus == nil {
		return fmt.Errorf("event bus not configured")
	}

	return bus.Subscribe(eventType, func(ctx context.Context, evt Event) error {
		payload, typedEvent, err := decodeTypedEvent[T](evt)
		if err != nil {
			return err
		}
		return handler(ctx, payload, typedEvent)
	})
}

// ListenAsyncJSON 使用全局 facade 注册带强类型 payload 的异步订阅。
func ListenAsyncJSON[T any](eventType string, handler JSONEventHandler[T]) error {
	facade := GetFacade()
	if facade.bus == nil {
		return fmt.Errorf("event bus not configured")
	}
	return SubscribeJSON(facade.bus, eventType, handler)
}

func decodeTypedEvent[T any](evt Event) (*T, Event, error) {
	payload, err := PayloadAs[T](evt)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := evt.Payload().(*T); ok {
		return payload, evt, nil
	}
	if _, ok := evt.Payload().(T); ok {
		return payload, evt, nil
	}

	return payload, &decodedPayloadEvent{
		base:    evt,
		payload: payload,
	}, nil
}

type decodedPayloadEvent struct {
	base    Event
	payload interface{}
}

func (e *decodedPayloadEvent) Type() string {
	return e.base.Type()
}

func (e *decodedPayloadEvent) Payload() interface{} {
	return e.payload
}

func (e *decodedPayloadEvent) Timestamp() time.Time {
	return e.base.Timestamp()
}

func (e *decodedPayloadEvent) PayloadBytes() []byte {
	rawEvent, ok := e.base.(RawPayloadEvent)
	if !ok {
		return nil
	}
	return rawEvent.PayloadBytes()
}

func payloadBytes(evt Event) []byte {
	rawEvent, ok := evt.(RawPayloadEvent)
	if !ok {
		return nil
	}
	return rawEvent.PayloadBytes()
}

func decodePayloadBytes[T any](data []byte) (*T, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty payload bytes")
	}

	payload := new(T)
	if err := json.Unmarshal(data, payload); err != nil {
		return nil, err
	}

	return payload, nil
}

func decodePayloadValue[T any](payloadValue interface{}) (*T, error) {
	if payloadValue == nil {
		return nil, fmt.Errorf("payload is nil")
	}

	switch value := payloadValue.(type) {
	case json.RawMessage:
		return decodePayloadBytes[T](value)
	case []byte:
		return decodePayloadBytes[T](value)
	case string:
		return decodePayloadBytes[T]([]byte(value))
	}

	data, err := json.Marshal(payloadValue)
	if err != nil {
		return nil, err
	}

	return decodePayloadBytes[T](data)
}

package event

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGetHAConfigReturnsBuiltInDefaultWhenUnset(t *testing.T) {
	restore := snapshotGlobalState()
	defer restore()

	SetHAConfig(nil)

	got := GetHAConfig()
	want := DefaultHighAvailabilityConfig()
	if got.MaxRetries != want.MaxRetries {
		t.Fatalf("unexpected MaxRetries: got %d want %d", got.MaxRetries, want.MaxRetries)
	}
	if got.RetryInterval != want.RetryInterval {
		t.Fatalf("unexpected RetryInterval: got %s want %s", got.RetryInterval, want.RetryInterval)
	}
	if got.RetryBackoff != want.RetryBackoff {
		t.Fatalf("unexpected RetryBackoff: got %t want %t", got.RetryBackoff, want.RetryBackoff)
	}
	if got.FallbackEnabled != want.FallbackEnabled {
		t.Fatalf("unexpected FallbackEnabled: got %t want %t", got.FallbackEnabled, want.FallbackEnabled)
	}
	if got.Timeout != want.Timeout {
		t.Fatalf("unexpected Timeout: got %s want %s", got.Timeout, want.Timeout)
	}
}

func TestGlobalTriggerWithHAUsesGlobalDefaultConfig(t *testing.T) {
	restore := snapshotGlobalState()
	defer restore()

	bus := newHATestBus(
		errors.New("first publish failed"),
		errors.New("second publish failed"),
		nil,
	)
	SetFacade(NewEventFacade(bus))
	SetHAConfig(&HighAvailabilityConfig{
		MaxRetries:      2,
		RetryInterval:   time.Millisecond,
		RetryBackoff:    false,
		FallbackEnabled: false,
		Timeout:         time.Second,
	})

	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	if err := TriggerWithHA(context.Background(), "user.created", event); err != nil {
		t.Fatalf("TriggerWithHA returned error: %v", err)
	}

	bus.waitForCalls(t, 3)
	time.Sleep(20 * time.Millisecond)

	if got := bus.calls(); got != 3 {
		t.Fatalf("unexpected publish count: %d", got)
	}
}

func TestGlobalTriggerWithHAUsesPerCallConfigOverride(t *testing.T) {
	restore := snapshotGlobalState()
	defer restore()

	bus := newHATestBus(
		errors.New("first publish failed"),
		errors.New("second publish failed"),
		nil,
	)
	SetFacade(NewEventFacade(bus))
	SetHAConfig(&HighAvailabilityConfig{
		MaxRetries:      0,
		RetryInterval:   time.Millisecond,
		RetryBackoff:    false,
		FallbackEnabled: false,
		Timeout:         time.Second,
	})

	event := &facadeTestEvent{
		eventType: "user.created",
		payload:   "payload",
		timestamp: time.Now(),
	}

	if err := TriggerWithHA(context.Background(), "user.created", event, &HighAvailabilityConfig{
		MaxRetries:      2,
		RetryInterval:   time.Millisecond,
		RetryBackoff:    false,
		FallbackEnabled: false,
		Timeout:         time.Second,
	}); err != nil {
		t.Fatalf("TriggerWithHA returned error: %v", err)
	}

	bus.waitForCalls(t, 3)
	time.Sleep(20 * time.Millisecond)

	if got := bus.calls(); got != 3 {
		t.Fatalf("unexpected publish count: %d", got)
	}
	if got := GetHAConfig().MaxRetries; got != 0 {
		t.Fatalf("global config should remain unchanged, got MaxRetries=%d", got)
	}
}

func snapshotGlobalState() func() {
	facadeMu.RLock()
	oldFacade := globalFacade
	var oldHAConfig *HighAvailabilityConfig
	if globalHAConfig != nil {
		cloned := *globalHAConfig
		oldHAConfig = &cloned
	}
	facadeMu.RUnlock()

	return func() {
		facadeMu.Lock()
		defer facadeMu.Unlock()
		globalFacade = oldFacade
		globalHAConfig = oldHAConfig
	}
}

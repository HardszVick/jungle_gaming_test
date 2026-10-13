package bootstrap

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/config"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestFxGraphForComponentSelections(t *testing.T) {
	for _, components := range []string{
		"http",
		"pending-reference",
		"sqs-consumer,outbox,pending-reference",
		"http,sqs-consumer,outbox,pending-reference",
	} {
		t.Run(components, func(t *testing.T) {
			value := testConfig(t, components)
			if err := fx.ValidateApp(AppOptions(value)...); err != nil {
				t.Fatalf("validate Fx graph: %v", err)
			}
		})
	}
}

func TestSelectedComponentsLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		components config.Components
		want       []string
	}{
		{name: "http only", components: config.Components{config.ComponentHTTP}, want: []string{"http"}},
		{name: "worker only", components: config.Components{config.ComponentPendingReference}, want: []string{"pending-reference"}},
		{name: "complete", components: config.Components{
			config.ComponentHTTP, config.ComponentSQSConsumer, config.ComponentOutbox, config.ComponentPendingReference,
		}, want: []string{"http", "outbox", "pending-reference", "sqs-consumer"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := &componentRecorder{}
			options := []fx.Option{
				fx.Supply(slog.New(slog.NewTextHandler(io.Discard, nil))),
				fx.Invoke(registerRuntimeComponents),
			}
			for _, name := range selectedComponentNames(test.components) {
				component := &fakeRuntimeComponent{name: name, recorder: recorder}
				options = append(options, fx.Supply(fx.Annotate(component,
					fx.As(new(runtimeComponent)), fx.ResultTags(`group:"runtime-components"`))))
			}
			app := fxtest.New(t, options...)
			app.RequireStart().RequireStop()
			started, stopped := recorder.snapshot()
			if !slices.Equal(started, test.want) {
				t.Fatalf("started %v, want %v", started, test.want)
			}
			slices.Sort(stopped)
			if !slices.Equal(stopped, test.want) {
				t.Fatalf("stopped %v, want %v", stopped, test.want)
			}
		})
	}
}

func selectedComponentNames(components config.Components) []string {
	names := make([]string, 0, len(components))
	for _, component := range components {
		names = append(names, string(component))
	}
	slices.Sort(names)
	return names
}

type componentRecorder struct {
	mu      sync.Mutex
	started []string
	stopped []string
}

func (recorder *componentRecorder) snapshot() ([]string, []string) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.started...), append([]string(nil), recorder.stopped...)
}

type fakeRuntimeComponent struct {
	name     string
	recorder *componentRecorder
}

func (component *fakeRuntimeComponent) Name() string { return component.name }

func (component *fakeRuntimeComponent) Start(context.Context) error {
	component.recorder.mu.Lock()
	defer component.recorder.mu.Unlock()
	component.recorder.started = append(component.recorder.started, component.name)
	return nil
}

func (component *fakeRuntimeComponent) Shutdown(context.Context) error {
	component.recorder.mu.Lock()
	defer component.recorder.mu.Unlock()
	component.recorder.stopped = append(component.recorder.stopped, component.name)
	return nil
}

func testConfig(t *testing.T, components string) config.Config {
	t.Helper()
	values := map[string]string{
		"DATABASE_URL":     "postgres://user:password@localhost:5432/wager?sslmode=disable",
		"OIDC_ISSUER_URL":  "http://localhost:8081/realms/wager",
		"OIDC_AUDIENCE":    "backend-api",
		"COMPONENTS":       components,
		"SHUTDOWN_TIMEOUT": (5 * time.Second).String(),
	}
	value, err := config.LoadFrom(func(name string) (string, bool) {
		result, ok := values[name]
		return result, ok
	})
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}
	return value
}

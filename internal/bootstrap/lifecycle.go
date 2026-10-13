package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/config"
	"go.uber.org/fx"
)

type startShutdown interface {
	Start(context.Context) error
	Shutdown(context.Context) error
}

type runtimeComponent interface {
	startShutdown
	Name() string
}

type namedRuntimeComponent struct {
	name   string
	target startShutdown
}

func (component namedRuntimeComponent) Name() string { return component.name }

func (component namedRuntimeComponent) Start(ctx context.Context) error {
	return component.target.Start(ctx)
}

func (component namedRuntimeComponent) Shutdown(ctx context.Context) error {
	return component.target.Shutdown(ctx)
}

type runtimeComponentsParams struct {
	fx.In

	Lifecycle  fx.Lifecycle
	Logger     *slog.Logger
	Components []runtimeComponent `group:"runtime-components"`
}

func registerRuntimeComponents(params runtimeComponentsParams) {
	components := append([]runtimeComponent(nil), params.Components...)
	sort.Slice(components, func(left, right int) bool {
		return components[left].Name() < components[right].Name()
	})

	var started []runtimeComponent
	params.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			for _, component := range components {
				if err := component.Start(ctx); err != nil {
					_ = shutdownRuntimeComponents(ctx, started)
					return fmt.Errorf("bootstrap: start %s: %w", component.Name(), err)
				}
				started = append(started, component)
				params.Logger.Info("component started", slog.String("component", component.Name()))
			}
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return shutdownRuntimeComponents(ctx, started)
		},
	})
}

func shutdownRuntimeComponents(ctx context.Context, components []runtimeComponent) error {
	var wait sync.WaitGroup
	errorsByComponent := make(chan error, len(components))
	for _, component := range components {
		component := component
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := component.Shutdown(ctx); err != nil {
				errorsByComponent <- fmt.Errorf("shutdown %s: %w", component.Name(), err)
			}
		}()
	}
	wait.Wait()
	close(errorsByComponent)
	var joined error
	for err := range errorsByComponent {
		joined = errors.Join(joined, err)
	}
	return joined
}

type httpServers struct {
	public     *http.Server
	internal   *http.Server
	logger     *slog.Logger
	onFailure  func()
	mu         sync.Mutex
	listeners  []net.Listener
	serveGroup sync.WaitGroup
}

func newHTTPServers(value config.HTTPConfig, public, internal http.Handler, logger *slog.Logger, onFailure func()) (*httpServers, error) {
	if public == nil || internal == nil {
		return nil, errors.New("bootstrap: HTTP handlers are required")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	newServer := func(address string, handler http.Handler) *http.Server {
		return &http.Server{
			Addr:              address,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
	}
	return &httpServers{
		public:    newServer(value.Address, public),
		internal:  newServer(value.MetricsAddress, internal),
		logger:    logger,
		onFailure: onFailure,
	}, nil
}

func (servers *httpServers) Name() string { return string(config.ComponentHTTP) }

func (servers *httpServers) Start(context.Context) error {
	servers.mu.Lock()
	defer servers.mu.Unlock()
	if len(servers.listeners) != 0 {
		return errors.New("bootstrap: HTTP servers already started")
	}
	publicListener, err := net.Listen("tcp", servers.public.Addr)
	if err != nil {
		return fmt.Errorf("listen on public address %s: %w", servers.public.Addr, err)
	}
	internalListener, err := net.Listen("tcp", servers.internal.Addr)
	if err != nil {
		_ = publicListener.Close()
		return fmt.Errorf("listen on internal address %s: %w", servers.internal.Addr, err)
	}
	servers.listeners = []net.Listener{publicListener, internalListener}
	servers.serve(servers.public, publicListener, "public")
	servers.serve(servers.internal, internalListener, "internal")
	return nil
}

func (servers *httpServers) serve(server *http.Server, listener net.Listener, name string) {
	servers.serveGroup.Add(1)
	go func() {
		defer servers.serveGroup.Done()
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			servers.logger.Error("HTTP server stopped unexpectedly", slog.String("server", name), slog.String("error", err.Error()))
			if servers.onFailure != nil {
				servers.onFailure()
			}
		}
	}()
}

func (servers *httpServers) Shutdown(ctx context.Context) error {
	servers.mu.Lock()
	if len(servers.listeners) == 0 {
		servers.mu.Unlock()
		return nil
	}
	servers.listeners = nil
	servers.mu.Unlock()

	publicErr := servers.public.Shutdown(ctx)
	internalErr := servers.internal.Shutdown(ctx)
	done := make(chan struct{})
	go func() {
		servers.serveGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
		return errors.Join(publicErr, internalErr)
	case <-ctx.Done():
		_ = servers.public.Close()
		_ = servers.internal.Close()
		return errors.Join(publicErr, internalErr, ctx.Err())
	}
}

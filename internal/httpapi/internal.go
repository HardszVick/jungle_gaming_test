package httpapi

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewInternalHandler exposes operational endpoints intended for a separate,
// non-public listener.
func NewInternalHandler(gatherer prometheus.Gatherer) (http.Handler, error) {
	if gatherer == nil {
		return nil, errors.New("httpapi: metrics gatherer is required")
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	return mux, nil
}

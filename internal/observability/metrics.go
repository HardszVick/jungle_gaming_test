package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const metricsNamespace = "wager"

const sqlTransactionWorker = "sql-transaction"

// Metrics owns an isolated Prometheus registry and the service's bounded set of
// operational metrics. It never registers collectors in the default registry.
type Metrics struct {
	registry                  *prometheus.Registry
	transactionResults        *prometheus.CounterVec
	duplicates                *prometheus.CounterVec
	retries                   *prometheus.CounterVec
	dlqMessages               *prometheus.CounterVec
	concurrencyConflicts      *prometheus.CounterVec
	outboxLag                 prometheus.Histogram
	outboxPublications        *prometheus.CounterVec
	processingLatency         *prometheus.HistogramVec
	reconciliationDivergences prometheus.Counter
}

// NewMetrics creates all collectors in a new registry. Keeping registry
// ownership local avoids duplicate registration and global test state.
func NewMetrics() *Metrics {
	metrics := &Metrics{
		registry: prometheus.NewRegistry(),
		transactionResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "transaction_results_total",
			Help:      "Number of transaction processing results.",
		}, []string{"kind", "status", "code"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "duplicate_deliveries_total",
			Help:      "Number of duplicate transaction or message deliveries.",
		}, []string{"kind"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "retries_total",
			Help:      "Number of retry attempts by worker and result.",
		}, []string{"worker", "result"}),
		dlqMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dlq_messages_total",
			Help:      "Number of messages sent to a dead-letter queue.",
		}, []string{"worker", "code"}),
		concurrencyConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "concurrency_conflicts_total",
			Help:      "Number of database concurrency conflicts.",
		}, []string{"kind"}),
		outboxLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "outbox_lag_seconds",
			Help:      "Age of an outbox event when publication is attempted.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12),
		}),
		outboxPublications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "outbox_publications_total",
			Help:      "Number of outbox publication attempts by result.",
		}, []string{"result"}),
		processingLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "processing_latency_seconds",
			Help:      "Transaction processing latency by kind and status.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"kind", "status"}),
		reconciliationDivergences: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "reconciliation_divergences_total",
			Help:      "Number of reconciliation runs that found a balance divergence.",
		}),
	}
	metrics.registry.MustRegister(
		metrics.transactionResults,
		metrics.duplicates,
		metrics.retries,
		metrics.dlqMessages,
		metrics.concurrencyConflicts,
		metrics.outboxLag,
		metrics.outboxPublications,
		metrics.processingLatency,
		metrics.reconciliationDivergences,
	)
	return metrics
}

// Registry returns this Metrics instance's private registry for collection or
// exposure through a Prometheus HTTP handler.
func (metrics *Metrics) Registry() *prometheus.Registry {
	return metrics.registry
}

func (metrics *Metrics) RecordTransactionResult(kind, status, code string) {
	metrics.transactionResults.WithLabelValues(kind, status, code).Inc()
}

func (metrics *Metrics) RecordDuplicate(kind string) {
	metrics.duplicates.WithLabelValues(kind).Inc()
}

func (metrics *Metrics) RecordRetry(worker, result string) {
	metrics.retries.WithLabelValues(worker, result).Inc()
}

func (metrics *Metrics) RecordTransactionRetry(reason string) {
	switch reason {
	case "deadlock", "serialization_failure":
	default:
		reason = "unknown"
	}
	metrics.RecordRetry(sqlTransactionWorker, reason)
}

func (metrics *Metrics) RecordDLQ(worker, code string) {
	metrics.dlqMessages.WithLabelValues(worker, code).Inc()
}

func (metrics *Metrics) RecordConcurrencyConflict(kind string) {
	metrics.concurrencyConflicts.WithLabelValues(kind).Inc()
}

func (metrics *Metrics) ObserveOutboxLag(lag time.Duration) {
	metrics.outboxLag.Observe(nonNegativeSeconds(lag))
}

func (metrics *Metrics) RecordOutboxPublication(result string) {
	metrics.outboxPublications.WithLabelValues(result).Inc()
}

func (metrics *Metrics) ObserveProcessingLatency(kind, status string, latency time.Duration) {
	metrics.processingLatency.WithLabelValues(kind, status).Observe(nonNegativeSeconds(latency))
}

func (metrics *Metrics) RecordReconciliationDivergence() {
	metrics.reconciliationDivergences.Inc()
}

func nonNegativeSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}

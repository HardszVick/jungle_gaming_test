package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsAreCollectedInPrivateRegistryWithLowCardinalityLabels(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordTransactionResult("BET", "REJECTED", "INSUFFICIENT_FUNDS")
	metrics.RecordDuplicate("transaction")
	metrics.RecordRetry("sqs-consumer", "transient")
	metrics.RecordDLQ("sqs-consumer", "INVALID_MESSAGE")
	metrics.RecordConcurrencyConflict("deadlock")
	metrics.ObserveOutboxLag(3 * time.Second)
	metrics.RecordOutboxPublication("success")
	metrics.ObserveProcessingLatency("BET", "REJECTED", 250*time.Millisecond)
	metrics.RecordReconciliationDivergence()

	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	wantNames := map[string]bool{
		"wager_transaction_results_total":        false,
		"wager_duplicate_deliveries_total":       false,
		"wager_retries_total":                    false,
		"wager_dlq_messages_total":               false,
		"wager_concurrency_conflicts_total":      false,
		"wager_outbox_lag_seconds":               false,
		"wager_outbox_publications_total":        false,
		"wager_processing_latency_seconds":       false,
		"wager_reconciliation_divergences_total": false,
	}
	allowedLabels := map[string]bool{
		"kind": true, "status": true, "code": true, "worker": true, "result": true,
	}
	for _, family := range families {
		name := family.GetName()
		if _, expected := wantNames[name]; !expected {
			t.Errorf("unexpected metric %q in private registry", name)
			continue
		}
		wantNames[name] = true
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if !allowedLabels[label.GetName()] {
					t.Errorf("metric %q has high-cardinality label %q", name, label.GetName())
				}
			}
		}
	}
	for name, found := range wantNames {
		if !found {
			t.Errorf("metric %q was not collected", name)
		}
	}

	assertCounterValue(t, metrics.Registry(), "wager_transaction_results_total", 1)
	assertCounterValue(t, metrics.Registry(), "wager_duplicate_deliveries_total", 1)
	assertCounterValue(t, metrics.Registry(), "wager_retries_total", 1)
	assertCounterValue(t, metrics.Registry(), "wager_dlq_messages_total", 1)
	assertCounterValue(t, metrics.Registry(), "wager_concurrency_conflicts_total", 1)
	assertCounterValue(t, metrics.Registry(), "wager_outbox_publications_total", 1)
	assertCounterValue(t, metrics.Registry(), "wager_reconciliation_divergences_total", 1)
	assertHistogramCount(t, metrics.Registry(), "wager_outbox_lag_seconds", 1)
	assertHistogramCount(t, metrics.Registry(), "wager_processing_latency_seconds", 1)
}

func TestMetricsRegistriesAreIndependent(t *testing.T) {
	first := NewMetrics()
	second := NewMetrics()
	first.RecordDuplicate("transaction")

	assertCounterValue(t, first.Registry(), "wager_duplicate_deliveries_total", 1)
	families, err := second.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "wager_duplicate_deliveries_total" {
			t.Fatal("duplicate from first registry appeared in second registry")
		}
	}
}

func TestTransactionRetriesUseExistingMetricWithBoundedLabels(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordTransactionRetry("deadlock")
	metrics.RecordTransactionRetry("serialization_failure")
	metrics.RecordTransactionRetry("unbounded-database-error")

	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"deadlock":              false,
		"serialization_failure": false,
		"unknown":               false,
	}
	for _, family := range families {
		if family.GetName() != "wager_retries_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["worker"] != sqlTransactionWorker {
				t.Errorf("worker label = %q, want %q", labels["worker"], sqlTransactionWorker)
			}
			if _, ok := want[labels["result"]]; !ok {
				t.Errorf("unexpected retry result label %q", labels["result"])
			} else {
				want[labels["result"]] = true
			}
		}
	}
	for result, found := range want {
		if !found {
			t.Errorf("retry result label %q was not collected", result)
		}
	}
}

func assertCounterValue(t *testing.T, registry *prometheus.Registry, name string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if len(family.GetMetric()) != 1 {
			t.Fatalf("metric %q has %d series, want 1", name, len(family.GetMetric()))
		}
		if got := family.GetMetric()[0].GetCounter().GetValue(); got != want {
			t.Fatalf("metric %q = %v, want %v", name, got, want)
		}
		return
	}
	t.Fatalf("metric %q not found", name)
}

func assertHistogramCount(t *testing.T, registry *prometheus.Registry, name string, want uint64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if len(family.GetMetric()) != 1 {
			t.Fatalf("metric %q has %d series, want 1", name, len(family.GetMetric()))
		}
		if got := family.GetMetric()[0].GetHistogram().GetSampleCount(); got != want {
			t.Fatalf("metric %q sample count = %d, want %d", name, got, want)
		}
		return
	}
	t.Fatalf("metric %q not found", name)
}

package main

import "testing"

func TestLoadRuntimeConfigDoesNotRequireOIDCForWorkerOnly(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:password@localhost:5432/wager?sslmode=disable")
	t.Setenv("COMPONENTS", "pending-reference")
	t.Setenv("OIDC_ISSUER_URL", "")
	t.Setenv("OIDC_AUDIENCE", "")
	if _, err := loadRuntimeConfig(); err != nil {
		t.Fatalf("load worker-only config: %v", err)
	}
}

func TestContainsComponentUsesWholeNames(t *testing.T) {
	if !containsComponent("outbox, http", "http") {
		t.Fatal("expected HTTP component")
	}
	if containsComponent("not-http,outbox", "http") {
		t.Fatal("matched a partial component name")
	}
}

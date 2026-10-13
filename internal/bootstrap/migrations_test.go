package bootstrap

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMigrationsOrdersAndPairsFiles(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{
		"000002_second.down.sql", "000001_first.up.sql",
		"000002_second.up.sql", "000001_first.down.sql",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("SELECT 1;"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	catalog, err := LoadMigrationCatalog(directory)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	migrations := catalog.Migrations()
	if len(migrations) != 2 || migrations[0].Version != 1 || migrations[1].Version != 2 {
		t.Fatalf("unexpected migrations: %+v", migrations)
	}
	if catalog.LatestVersion() != 2 {
		t.Fatalf("latest migration = %d, want 2", catalog.LatestVersion())
	}
}

func TestLoadMigrationsRequiresBothDirections(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "000001_first.up.sql"), []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMigrationCatalog(directory); err == nil {
		t.Fatal("expected missing down migration to fail")
	}
}

func TestLoadMigrationCatalogRejectsMismatchedPairNames(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"000001_first.up.sql", "000001_other.down.sql"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("SELECT 1;"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := LoadMigrationCatalog(directory); err == nil {
		t.Fatal("expected mismatched migration names to fail")
	}
}

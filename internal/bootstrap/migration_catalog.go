package bootstrap

import "github.com/junglegaming/backend-challenge-go/internal/migrations"

type Migration = migrations.Migration
type MigrationCatalog = migrations.Catalog

func DefaultMigrationCatalog() (MigrationCatalog, error) {
	return migrations.DefaultCatalog()
}

func LoadMigrationCatalog(directory string) (MigrationCatalog, error) {
	return migrations.LoadCatalog(directory)
}

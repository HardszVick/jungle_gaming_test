package migrations

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationLockID int64 = 0x7761676572

func Run(ctx context.Context, databaseURL, direction string) error {
	if databaseURL == "" {
		return errors.New("migrate: DATABASE_URL is required")
	}
	if direction != "up" && direction != "down" {
		return fmt.Errorf("migrate: direction must be up or down")
	}
	catalog, err := DefaultCatalog()
	if err != nil {
		return err
	}
	return run(ctx, databaseURL, direction, catalog)
}

func RunFromDirectory(ctx context.Context, databaseURL, direction, directory string) error {
	if databaseURL == "" {
		return errors.New("migrate: DATABASE_URL is required")
	}
	if direction != "up" && direction != "down" {
		return fmt.Errorf("migrate: direction must be up or down")
	}
	catalog, err := LoadCatalog(directory)
	if err != nil {
		return err
	}
	return run(ctx, databaseURL, direction, catalog)
}

func run(ctx context.Context, databaseURL, direction string, catalog Catalog) error {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate: open PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("migrate: connect to PostgreSQL: %w", err)
	}
	if direction == "up" {
		return migrateUp(ctx, pool, catalog.Migrations())
	}
	return migrateDown(ctx, pool, catalog.Migrations())
}

func migrateUp(ctx context.Context, pool *pgxpool.Pool, migrations []Migration) error {
	for _, item := range migrations {
		script, err := os.ReadFile(item.UpPath)
		if err != nil {
			return fmt.Errorf("migrate: read %s: %w", item.UpPath, err)
		}
		err = withMigrationTransaction(ctx, pool, func(tx pgx.Tx) error {
			applied, err := migrationApplied(ctx, tx, item.Version)
			if err != nil || applied {
				return err
			}
			if _, err := tx.Exec(ctx, string(script), pgx.QueryExecModeSimpleProtocol); err != nil {
				return fmt.Errorf("execute: %w", err)
			}
			_, err = tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, item.Version, filepath.Base(item.UpPath))
			return err
		})
		if err != nil {
			return fmt.Errorf("migrate: apply version %d up: %w", item.Version, err)
		}
	}
	return nil
}

func migrateDown(ctx context.Context, pool *pgxpool.Pool, migrations []Migration) error {
	byVersion := make(map[int64]Migration, len(migrations))
	for _, item := range migrations {
		byVersion[item.Version] = item
	}
	return withMigrationTransaction(ctx, pool, func(tx pgx.Tx) error {
		var version int64
		err := tx.QueryRow(ctx, `SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = tx.Exec(ctx, `DROP TABLE schema_migrations`)
			return err
		}
		if err != nil {
			return err
		}
		item, ok := byVersion[version]
		if !ok {
			return fmt.Errorf("no down file for applied version %d", version)
		}
		script, err := os.ReadFile(item.DownPath)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(script), pgx.QueryExecModeSimpleProtocol); err != nil {
			return fmt.Errorf("execute: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version); err != nil {
			return err
		}
		var remaining int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&remaining); err != nil {
			return err
		}
		if remaining == 0 {
			_, err = tx.Exec(ctx, `DROP TABLE schema_migrations`)
		}
		return err
	})
}

func withMigrationTransaction(ctx context.Context, pool *pgxpool.Pool, operation func(pgx.Tx) error) (err error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := tx.Rollback(context.WithoutCancel(ctx))
		if err == nil && rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = rollbackErr
		}
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`); err != nil {
		return err
	}
	if err = operation(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func migrationApplied(ctx context.Context, tx pgx.Tx, version int64) (bool, error) {
	var applied bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&applied)
	return applied, err
}

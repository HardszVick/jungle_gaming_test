package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Database struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Database, error) {
	if databaseURL == "" {
		return nil, errors.New("postgres: database URL is required")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, TranslateError(err)
	}
	return &Database{pool: pool}, nil
}

func New(pool *pgxpool.Pool) (*Database, error) {
	if pool == nil {
		return nil, errors.New("postgres: pool is nil")
	}
	return &Database{pool: pool}, nil
}

func (db *Database) Pool() *pgxpool.Pool {
	if db == nil {
		return nil
	}
	return db.pool
}

func (db *Database) Health(ctx context.Context) error {
	if db == nil || db.pool == nil {
		return errors.New("postgres: database is closed")
	}
	return TranslateError(db.pool.Ping(ctx))
}

func (db *Database) Close() {
	if db != nil && db.pool != nil {
		db.pool.Close()
		db.pool = nil
	}
}

func (db *Database) Store() *Store {
	if db == nil {
		return &Store{}
	}
	return &Store{db: db.pool}
}

func (db *Database) WithinTx(ctx context.Context, options pgx.TxOptions, fn func(*Store) error) (err error) {
	if db == nil || db.pool == nil {
		return errors.New("postgres: database is closed")
	}
	tx, err := db.pool.BeginTx(ctx, options)
	if err != nil {
		return TranslateError(err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) && err == nil {
			err = TranslateError(rollbackErr)
		}
	}()

	if err = fn(&Store{db: tx}); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return TranslateError(tx.Commit(ctx))
}

func (db *Database) ReadCommitted(ctx context.Context, fn func(*Store) error) error {
	return db.WithinTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

func (db *Database) RepeatableRead(ctx context.Context, fn func(*Store) error) error {
	return db.WithinTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

// ExecuteSQLMigration runs a trusted migration script atomically. Migration
// discovery and version tracking remain the responsibility of the caller.
func (db *Database) ExecuteSQLMigration(ctx context.Context, sql string) error {
	if sql == "" {
		return errors.New("postgres: migration SQL is empty")
	}
	return db.WithinTx(ctx, pgx.TxOptions{}, func(store *Store) error {
		_, err := store.db.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol)
		return TranslateError(err)
	})
}

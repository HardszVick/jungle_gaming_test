package postgres

import (
	"context"
	"errors"
	"time"
)

type Store struct {
	db DBTX
}

func NewStore(db DBTX) (*Store, error) {
	if db == nil {
		return nil, errors.New("postgres: DBTX is nil")
	}
	return &Store{db: db}, nil
}

func (s *Store) TransactionTimestamp(ctx context.Context) (time.Time, error) {
	var timestamp time.Time
	if err := s.db.QueryRow(ctx, `SELECT CURRENT_TIMESTAMP`).Scan(&timestamp); err != nil {
		return time.Time{}, TranslateError(err)
	}
	return timestamp.UTC(), nil
}

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNotFound             = errors.New("postgres: not found")
	ErrConflict             = errors.New("postgres: conflict")
	ErrIdempotencyConflict  = errors.New("postgres: idempotency key conflict")
	ErrExternalIDConflict   = errors.New("postgres: external transaction identity conflict")
	ErrInboxHashConflict    = errors.New("postgres: inbox payload hash conflict")
	ErrConcurrentUpdate     = errors.New("postgres: concurrent update")
	ErrClaimLost            = errors.New("postgres: claim lost")
	ErrSerializationFailure = errors.New("postgres: serialization failure")
	ErrDeadlock             = errors.New("postgres: deadlock detected")
	ErrConstraintViolation  = errors.New("postgres: constraint violation")
)

type DatabaseError struct {
	Kind       error
	SQLState   string
	Constraint string
	Cause      error
}

func (e *DatabaseError) Error() string {
	if e.Constraint != "" {
		return fmt.Sprintf("%v (%s)", e.Kind, e.Constraint)
	}
	return e.Kind.Error()
}

func (e *DatabaseError) Unwrap() []error { return []error{e.Kind, e.Cause} }

func TranslateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}

	var kind error
	switch pgErr.Code {
	case "23505":
		kind = ErrConflict
	case "40001":
		kind = ErrSerializationFailure
	case "40P01":
		kind = ErrDeadlock
	case "57014":
		kind = context.Canceled
	default:
		if strings.HasPrefix(pgErr.Code, "23") {
			kind = ErrConstraintViolation
		} else {
			return err
		}
	}
	return &DatabaseError{Kind: kind, SQLState: pgErr.Code, Constraint: pgErr.ConstraintName, Cause: err}
}

func IsRetryable(err error) bool {
	return errors.Is(err, ErrSerializationFailure) || errors.Is(err, ErrDeadlock)
}

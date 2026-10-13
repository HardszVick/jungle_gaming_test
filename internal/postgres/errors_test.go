package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestTranslateErrorClassifiesPostgreSQLErrors(t *testing.T) {
	tests := []struct {
		code string
		want error
	}{
		{code: "23505", want: ErrConflict},
		{code: "23503", want: ErrConstraintViolation},
		{code: "40001", want: ErrSerializationFailure},
		{code: "40P01", want: ErrDeadlock},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			err := TranslateError(&pgconn.PgError{Code: test.code, ConstraintName: "constraint_name"})
			if !errors.Is(err, test.want) {
				t.Fatalf("TranslateError(%s) = %v, want %v", test.code, err, test.want)
			}
			var databaseErr *DatabaseError
			if !errors.As(err, &databaseErr) || databaseErr.SQLState != test.code || databaseErr.Constraint != "constraint_name" {
				t.Fatalf("translated detail = %#v", databaseErr)
			}
		})
	}
}

func TestTranslateErrorDoesNotClassifyOtherSQLStateClassesAsConstraints(t *testing.T) {
	for _, code := range []string{"42501", "42P01", "22P02"} {
		t.Run(code, func(t *testing.T) {
			source := &pgconn.PgError{Code: code}
			err := TranslateError(source)
			if !errors.Is(err, source) {
				t.Fatalf("TranslateError(%s) did not preserve the source error", code)
			}
			if errors.Is(err, ErrConstraintViolation) || errors.Is(err, ErrConflict) || IsRetryable(err) {
				t.Fatalf("TranslateError(%s) = %v, want unclassified PostgreSQL error", code, err)
			}
		})
	}
}

func TestIsRetryable(t *testing.T) {
	if !IsRetryable(TranslateError(&pgconn.PgError{Code: "40001"})) {
		t.Fatal("serialization failure must be retryable")
	}
	if IsRetryable(TranslateError(&pgconn.PgError{Code: "23505"})) {
		t.Fatal("uniqueness violation must not be retryable")
	}
}

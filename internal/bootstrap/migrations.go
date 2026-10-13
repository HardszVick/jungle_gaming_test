package bootstrap

import (
	"context"

	"github.com/junglegaming/backend-challenge-go/internal/migrations"
)

func RunMigrations(ctx context.Context, databaseURL, direction string) error {
	return migrations.Run(ctx, databaseURL, direction)
}

func RunMigrationsFromDirectory(ctx context.Context, databaseURL, direction, directory string) error {
	return migrations.RunFromDirectory(ctx, databaseURL, direction, directory)
}

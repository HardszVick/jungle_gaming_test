package migrations

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var migrationFilename = regexp.MustCompile(`^(\d+)_([^.]+)\.(up|down)\.sql$`)

// CurrentVersion is the schema version required by this application build.
const CurrentVersion int64 = 3

type Migration struct {
	Version  int64
	Name     string
	UpPath   string
	DownPath string
}

type Catalog struct {
	migrations []Migration
}

func DefaultCatalog() (Catalog, error) {
	directory, err := migrationDirectory()
	if err != nil {
		return Catalog{}, err
	}
	catalog, err := LoadCatalog(directory)
	if err != nil {
		return Catalog{}, err
	}
	if catalog.LatestVersion() != CurrentVersion {
		return Catalog{}, fmt.Errorf(
			"migrate: latest migration version is %d, application requires %d",
			catalog.LatestVersion(), CurrentVersion,
		)
	}
	return catalog, nil
}

func LoadCatalog(directory string) (Catalog, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return Catalog{}, fmt.Errorf("migrate: read migrations: %w", err)
	}
	byVersion := make(map[int64]*Migration)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches := migrationFilename.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil || version <= 0 {
			return Catalog{}, fmt.Errorf("migrate: invalid version in %s", entry.Name())
		}
		name := matches[1] + "_" + matches[2]
		item := byVersion[version]
		if item == nil {
			item = &Migration{Version: version, Name: name}
			byVersion[version] = item
		} else if item.Name != name {
			return Catalog{}, fmt.Errorf("migrate: version %d has mismatched names %s and %s", version, item.Name, name)
		}
		path := filepath.Join(directory, entry.Name())
		if matches[3] == "up" {
			if item.UpPath != "" {
				return Catalog{}, fmt.Errorf("migrate: duplicate up migration version %d", version)
			}
			item.UpPath = path
		} else {
			if item.DownPath != "" {
				return Catalog{}, fmt.Errorf("migrate: duplicate down migration version %d", version)
			}
			item.DownPath = path
		}
	}
	migrations := make([]Migration, 0, len(byVersion))
	for _, item := range byVersion {
		if item.UpPath == "" || item.DownPath == "" {
			return Catalog{}, fmt.Errorf("migrate: version %d must have matching up and down files", item.Version)
		}
		migrations = append(migrations, *item)
	}
	if len(migrations) == 0 {
		return Catalog{}, errors.New("migrate: no versioned migration files found")
	}
	sort.Slice(migrations, func(left, right int) bool {
		return migrations[left].Version < migrations[right].Version
	})
	return Catalog{migrations: migrations}, nil
}

func (catalog Catalog) Migrations() []Migration {
	return append([]Migration(nil), catalog.migrations...)
}

func (catalog Catalog) LatestVersion() int64 {
	if len(catalog.migrations) == 0 {
		return 0
	}
	return catalog.migrations[len(catalog.migrations)-1].Version
}

func migrationDirectory() (string, error) {
	if configured := os.Getenv("MIGRATIONS_DIR"); configured != "" {
		info, err := os.Stat(configured)
		if err != nil || !info.IsDir() {
			return "", fmt.Errorf("migrate: configured migrations directory %q is unavailable", configured)
		}
		return configured, nil
	}
	for _, candidate := range []string{"/app/migrations", "./migrations"} {
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate, nil
		}
	}
	return "", errors.New("migrate: migrations directory not found at /app/migrations or ./migrations")
}

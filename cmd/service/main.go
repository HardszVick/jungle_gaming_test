package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/junglegaming/backend-challenge-go/internal/bootstrap"
	"github.com/junglegaming/backend-challenge-go/internal/config"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(arguments []string) int {
	if len(arguments) == 0 {
		value, err := loadRuntimeConfig()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		bootstrap.NewApp(value).Run()
		return 0
	}
	if len(arguments) == 2 && arguments[0] == "migrate" && (arguments[1] == "up" || arguments[1] == "down") {
		if err := bootstrap.RunMigrations(context.Background(), os.Getenv("DATABASE_URL"), arguments[1]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
	fmt.Fprintln(os.Stderr, "usage: service [migrate up|down]")
	return 2
}

func loadRuntimeConfig() (config.Config, error) {
	components := os.Getenv("COMPONENTS")
	if components == "" || containsComponent(components, string(config.ComponentHTTP)) {
		return config.Load()
	}
	return config.LoadFrom(func(name string) (string, bool) {
		switch name {
		case "OIDC_ISSUER_URL":
			if value, ok := os.LookupEnv(name); ok && strings.TrimSpace(value) != "" {
				return value, true
			}
			return "http://unused.invalid", true
		case "OIDC_AUDIENCE":
			if value, ok := os.LookupEnv(name); ok && strings.TrimSpace(value) != "" {
				return value, true
			}
			return "unused", true
		default:
			return os.LookupEnv(name)
		}
	})
}

func containsComponent(value, wanted string) bool {
	for _, component := range strings.Split(value, ",") {
		if strings.TrimSpace(component) == wanted {
			return true
		}
	}
	return false
}

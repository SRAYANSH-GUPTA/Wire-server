package db

import (
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func RunMigrations(databaseURL, migrationsPath string) error {
	m, err := migrate.New("file://"+migrationsPath, databaseURL)
	if err != nil {
		return fmt.Errorf("db.RunMigrations create: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		_ = m.Close()
		return fmt.Errorf("db.RunMigrations up: %w", err)
	}
	if err := m.Close(); err != nil {
		return fmt.Errorf("db.RunMigrations close: %w", err)
	}
	return nil
}

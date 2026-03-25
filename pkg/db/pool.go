package db

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
)

// write-op
func NewPrimaryPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("db.NewPrimaryPool parse config: %w", err)
	}
	cfg.MaxConns = 25
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db.NewPrimaryPool create: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db.NewPrimaryPool ping: %w", err)
	}
	return pool, nil
}

// read-op
func NewReplicaPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("db.NewReplicaPool parse config: %w", err)
	}
	cfg.MaxConns = 50
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db.NewReplicaPool create: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db.NewReplicaPool ping: %w", err)
	}
	return pool, nil
}

// write-op
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, migrationsDir string) error {
	source := fmt.Sprintf("file://%s", migrationsDir)
	m, err := migrate.New(source, pool.Config().ConnString())
	if err != nil {
		return fmt.Errorf("db.RunMigrations create: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		if closeErr := m.Close(); closeErr != nil {
			return fmt.Errorf("db.RunMigrations up: %w, close: %v", err, closeErr)
		}
		return fmt.Errorf("db.RunMigrations up: %w", err)
	}
	if err := m.Close(); err != nil {
		return fmt.Errorf("db.RunMigrations close: %w", err)
	}
	return nil
}

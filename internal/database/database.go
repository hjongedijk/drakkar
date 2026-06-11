package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hjongedijk/drakkar/internal/config"
	"github.com/hjongedijk/drakkar/internal/stream"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

type DB struct {
	SQL            *sql.DB
	SegmentFetcher stream.SegmentFetcher
	ReadAhead      *stream.ReadAheadManager
}

func Open(cfg config.DatabaseConfig) (*DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", cfg.Host, cfg.Port, cfg.Name, cfg.Username, cfg.Password)
	pgxCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	// idle_in_transaction_session_timeout prevents connection-pool self-deadlock:
	// if a goroutine holds an open transaction (due to a missed Rollback) while
	// blocked waiting for a second connection, postgres kills the idle transaction
	// after 60s so the waiting goroutine can proceed.
	pgxCfg.RuntimeParams["idle_in_transaction_session_timeout"] = "60000"
	sqlDB := stdlib.OpenDB(*pgxCfg)
	sqlDB.SetMaxOpenConns(12)
	sqlDB.SetMaxIdleConns(4)
	return &DB{SQL: sqlDB}, nil
}

func (db *DB) Ping(ctx context.Context) error {
	return db.SQL.PingContext(ctx)
}

func (db *DB) Close() error {
	if db == nil || db.SQL == nil {
		return nil
	}
	return db.SQL.Close()
}

func (db *DB) ApplyMigrations(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	if _, err := db.SQL.ExecContext(ctx, `create table if not exists schema_migrations (version text primary key, applied_at timestamptz not null default now())`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	for _, name := range files {
		var exists bool
		if err := db.SQL.QueryRowContext(ctx, `select exists(select 1 from schema_migrations where version = $1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		tx, err := db.SQL.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `insert into schema_migrations(version) values ($1)`, name); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

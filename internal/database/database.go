package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hjongedijk/drakkar/internal/config"
	"github.com/hjongedijk/drakkar/internal/stream"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// cachedVF holds the immutable data for a virtual file so that repeated
// OpenVirtualMediaFile calls (e.g. each rclone range request) don't re-query
// the DB for the same segment data.
type cachedVF struct {
	name       string
	readerKind string
	inlineData []byte
	size       int64             // virtual file size in bytes
	spans      []stream.SegmentSpan // canonical spans — callers receive a copy
}

type DB struct {
	SQL            *sql.DB
	SegmentFetcher stream.SegmentFetcher
	ReadAhead      *stream.ReadAheadManager

	vfCacheMu sync.RWMutex
	vfCache   map[int64]*cachedVF
}

func Open(cfg config.DatabaseConfig) (*DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", cfg.Host, cfg.Port, cfg.Name, cfg.Username, cfg.Password)
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	// idle_in_transaction_session_timeout: kill connections that are idle inside
	// a transaction for >60s to prevent pool self-deadlock if Rollback is missed.
	poolCfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "60000"
	// pgxpool health-checks each connection before returning it to callers,
	// avoiding "driver: bad connection" errors from silently dropped idle conns.
	// 25 max gives headroom for 12 BullMQ workers + download/monitor/HTTP load.
	// Ping before returning any idle connection from the pool. pgxpool's
	// background health check only runs every HealthCheckPeriod (default 1min),
	// so a silently-dropped TCP connection can slip through and cause
	// "driver: bad connection" on the first I/O. BeforeAcquire forces a
	// round-trip on every acquire — sub-millisecond on a local Docker network
	// but guarantees the connection is alive before the caller sees it.
	poolCfg.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
		return c.Ping(ctx) == nil
	}
	poolCfg.MaxConns = 25
	poolCfg.MinConns = 2
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	// Hand all pooling to pgxpool: set sql.DB idle cache to 0 so every
	// operation calls pool.Acquire(), which health-checks before returning.
	// sql.DB's own idle cache only calls ResetSession (checks an in-memory
	// flag) — it misses TCP-level drops and produces "driver: bad connection".
	// pgxpool.Acquire() does a real network check, so this eliminates the error.
	sqlDB.SetMaxOpenConns(int(poolCfg.MaxConns))
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetConnMaxLifetime(0)
	sqlDB.SetConnMaxIdleTime(0)
	return &DB{SQL: sqlDB, vfCache: make(map[int64]*cachedVF)}, nil
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

// migrationLockID is the PostgreSQL advisory lock key used to serialise
// concurrent migration runs (e.g. two containers starting simultaneously).
const migrationLockID = 0x6472616b6b617200 // "drakkar\0"

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
		// Quick pre-check without locking — skip already-applied migrations cheaply.
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
		// Acquire a session-level advisory lock inside the transaction so that
		// only one concurrent Drakkar instance applies any given migration. The
		// lock is automatically released when the transaction commits or rolls back.
		if _, err := tx.ExecContext(ctx, `select pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("acquire migration lock: %w", err)
		}
		// Re-check inside the lock in case another instance applied this migration
		// while we were waiting to acquire the lock.
		if err := tx.QueryRowContext(ctx, `select exists(select 1 from schema_migrations where version = $1)`, name).Scan(&exists); err != nil {
			_ = tx.Rollback()
			return err
		}
		if exists {
			_ = tx.Rollback()
			continue
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

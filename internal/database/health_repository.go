package database

import (
	"context"
	"os"
	"time"
)

type HealthEntry struct {
	ID            int64      `json:"id"`
	LibraryItemID int64      `json:"libraryItemId"`
	LibraryPath   string     `json:"libraryPath"`
	TargetPath    string     `json:"targetPath"`
	CreatedAt     time.Time  `json:"createdAt"`
	LastCheckedAt *time.Time `json:"lastCheckedAt"`
	HealthOK      *bool      `json:"healthOk"`
}

type HealthSummary struct {
	Total             int `json:"total"`
	Checked           int `json:"checked"`
	Healthy           int `json:"healthy"`
	NeverChecked      int `json:"neverChecked"`
	ConsistencyIssues int `json:"consistencyIssues"`
}

type ConsistencyIssue struct {
	LibraryItemID int64  `json:"libraryItemId"`
	Title         string `json:"title"`
	MediaType     string `json:"mediaType"`
	QueueState    string `json:"queueState"`
}

type MaintenanceCursorEntry struct {
	TaskName  string    `json:"taskName"`
	Cursor    string    `json:"cursor"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func (db *DB) ListHealthEntries(ctx context.Context) ([]HealthEntry, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select id, library_item_id, library_path, target_path, created_at, last_checked_at, health_ok
		from symlink_publications
		order by last_checked_at asc nulls first, created_at asc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HealthEntry
	for rows.Next() {
		var e HealthEntry
		if err := rows.Scan(&e.ID, &e.LibraryItemID, &e.LibraryPath, &e.TargetPath,
			&e.CreatedAt, &e.LastCheckedAt, &e.HealthOK); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (db *DB) HealthSummary(ctx context.Context) (HealthSummary, error) {
	var s HealthSummary
	err := db.SQL.QueryRowContext(ctx, `
		select
			count(*)                                            as total,
			count(*) filter (where last_checked_at is not null) as checked,
			count(*) filter (where health_ok = true)            as healthy,
			count(*) filter (where last_checked_at is null)     as never_checked
		from symlink_publications`).Scan(&s.Total, &s.Checked, &s.Healthy, &s.NeverChecked)
	if err != nil {
		return s, err
	}
	// Count library items marked available but with no published symlink.
	// Exclude manual_nzb imports — they have no episode metadata and can never produce a library path.
	_ = db.SQL.QueryRowContext(ctx, `
		select count(*)
		from library_items li
		where li.available = true
		  and li.media_type != 'manual_nzb'
		  and not exists (
		      select 1 from symlink_publications sp
		      where sp.library_item_id = li.id
		  )`).Scan(&s.ConsistencyIssues)
	return s, nil
}

func (db *DB) ListConsistencyIssues(ctx context.Context) ([]ConsistencyIssue, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select li.id, li.title, li.media_type,
		       coalesce((select qi.state from queue_items qi
		                 where qi.library_item_id = li.id
		                 order by qi.id desc limit 1), '') as queue_state
		from library_items li
		where li.available = true
		  and li.media_type != 'manual_nzb'
		  and not exists (
		      select 1 from symlink_publications sp
		      where sp.library_item_id = li.id
		  )
		order by li.id asc
		limit 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConsistencyIssue
	for rows.Next() {
		var c ConsistencyIssue
		if err := rows.Scan(&c.LibraryItemID, &c.Title, &c.MediaType, &c.QueueState); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (db *DB) RecordHealthCheck(ctx context.Context, publicationID int64, ok bool) error {
	_, err := db.SQL.ExecContext(ctx, `
		update symlink_publications
		set last_checked_at = now(), health_ok = $2
		where id = $1`, publicationID, ok)
	return err
}

// CheckSymlinkHealth verifies the host-side symlink exists and points to the expected target.
func CheckSymlinkHealth(libraryPath, targetPath string) bool {
	dest, err := os.Readlink(libraryPath)
	if err != nil {
		return false
	}
	return dest == targetPath
}

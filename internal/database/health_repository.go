package database

import (
	"context"
	"os"
	"strings"
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

type DeepHealthCandidate struct {
	PublicationID int64
	LibraryItemID int64
	LibraryPath   string
	TargetPath    string
	CreatedAt     time.Time
	LastCheckedAt *time.Time
	HealthOK      *bool
	NZBDocumentID int64
	Title         string
	HasPAR2       bool
}

type HealthSummary struct {
	Total                int `json:"total"`
	Checked              int `json:"checked"`
	Healthy              int `json:"healthy"`
	NeverChecked         int `json:"neverChecked"`
	ConsistencyIssues    int `json:"consistencyIssues"`
	UncalibratedNZBFiles int `json:"uncalibratedNZBFiles"`
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

// ListBrokenSymlinkEntries returns all publications where health_ok = false.
func (db *DB) ListBrokenSymlinkEntries(ctx context.Context) ([]HealthEntry, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select id, library_item_id, library_path, target_path, created_at, last_checked_at, health_ok
		from symlink_publications
		where health_ok = false
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

type HealthEntriesPage struct {
	Items []HealthEntry `json:"items"`
	Total int           `json:"total"`
}

// ListHealthEntriesPage returns a paginated, filterable page of health entries.
// filter: "all" | "broken" | "unchecked"
func (db *DB) ListHealthEntriesPage(ctx context.Context, filter string, limit, offset int) (HealthEntriesPage, error) {
	var where string
	switch filter {
	case "broken":
		where = "where health_ok = false"
	case "unchecked":
		where = "where last_checked_at is null"
	default:
		where = ""
	}
	// Broken and unchecked entries float to top; within each group order by path.
	query := `
		select id, library_item_id, library_path, target_path, created_at, last_checked_at, health_ok
		from symlink_publications
		` + where + `
		order by
			case when health_ok is false then 0 when health_ok is null then 1 else 2 end,
			library_path asc
		limit $1 offset $2`
	countQuery := `select count(*) from symlink_publications ` + where
	var total int
	if err := db.SQL.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return HealthEntriesPage{}, err
	}
	rows, err := db.SQL.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return HealthEntriesPage{}, err
	}
	defer rows.Close()
	var items []HealthEntry
	for rows.Next() {
		var e HealthEntry
		if err := rows.Scan(&e.ID, &e.LibraryItemID, &e.LibraryPath, &e.TargetPath,
			&e.CreatedAt, &e.LastCheckedAt, &e.HealthOK); err != nil {
			return HealthEntriesPage{}, err
		}
		items = append(items, e)
	}
	if items == nil {
		items = []HealthEntry{}
	}
	return HealthEntriesPage{Items: items, Total: total}, rows.Err()
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
	_ = db.SQL.QueryRowContext(ctx, `
		select count(*) from nzb_files where calibrated_at is null`).Scan(&s.UncalibratedNZBFiles)
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

func (db *DB) RecordHealthStatus(ctx context.Context, publicationID int64, ok bool) error {
	_, err := db.SQL.ExecContext(ctx, `
		update symlink_publications
		set health_ok = $2
		where id = $1`, publicationID, ok)
	return err
}

func (db *DB) ListDeepHealthCandidates(ctx context.Context, limit int) ([]DeepHealthCandidate, error) {
	query := `
		SELECT DISTINCT ON (sp.library_item_id)
		    sp.id AS publication_id,
		    sp.library_item_id,
		    sp.library_path,
		    sp.target_path,
		    sp.created_at,
		    sp.last_checked_at,
		    sp.health_ok,
		    nd.id,
		    li.title,
		    EXISTS (
		        SELECT 1
		        FROM nzb_files nf
		        WHERE nf.nzb_document_id = nd.id
		          AND lower(nf.subject) LIKE '%.par2%'
		    ) AS has_par2
		FROM symlink_publications sp
		JOIN library_items li ON li.id = sp.library_item_id
		JOIN queue_items qi ON qi.library_item_id = sp.library_item_id
		JOIN selected_releases sr ON sr.id = qi.selected_release_id
		JOIN nzb_documents nd ON nd.selected_release_id = sr.id
		WHERE li.available = true
		  AND qi.state IN ('available', 'degraded')
		ORDER BY sp.library_item_id ASC, qi.id DESC`
	if limit > 0 {
		query = `
			SELECT *
			FROM (` + query + `) candidates
			ORDER BY last_checked_at ASC NULLS FIRST, created_at ASC, publication_id ASC
			LIMIT $1`
		rows, err := db.SQL.QueryContext(ctx, query, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanDeepHealthCandidates(rows)
	}
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT *
		FROM (`+query+`) candidates
		ORDER BY last_checked_at ASC NULLS FIRST, created_at ASC, publication_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeepHealthCandidates(rows)
}

type deepHealthScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanDeepHealthCandidates(rows deepHealthScanner) ([]DeepHealthCandidate, error) {
	var out []DeepHealthCandidate
	for rows.Next() {
		var item DeepHealthCandidate
		if err := rows.Scan(
			&item.PublicationID,
			&item.LibraryItemID,
			&item.LibraryPath,
			&item.TargetPath,
			&item.CreatedAt,
			&item.LastCheckedAt,
			&item.HealthOK,
			&item.NZBDocumentID,
			&item.Title,
			&item.HasPAR2,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) MarkLibraryItemDegraded(ctx context.Context, libraryItemID int64, reason string) error {
	_, err := db.SQL.ExecContext(ctx, `
		UPDATE queue_items
		SET state = $2, failure_reason = $3, updated_at = now()
		WHERE id = (
		    SELECT id
		    FROM queue_items
		    WHERE library_item_id = $1
		    ORDER BY id DESC
		    LIMIT 1
		)`, libraryItemID, QueueDegraded, reason)
	return err
}

// CheckSymlinkHealth verifies the host-side symlink exists and points to a valid VFS content path.
// It accepts any destination under the VFS /content/ tree — not just the exact stored target —
// because season-pack episodes share a library_path and the symlink may have been updated by a
// later publish to a different release while older DB records still reference the original target.
func CheckSymlinkHealth(libraryPath, targetPath string) bool {
	dest, err := os.Readlink(libraryPath)
	if err != nil {
		return false
	}
	if dest == targetPath {
		return true
	}
	// Accept any symlink that resolves into the VFS content tree.
	return strings.Contains(dest, "/content/releases/")
}

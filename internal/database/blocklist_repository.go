package database

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type BlocklistFilter struct {
	Q        string
	Reason   string
	Page     int
	PageSize int
	Sort     string // createdAt | expiresAt | reason | key
	Dir      string // asc | desc
}

func (db *DB) ListBlocklistItemsPaged(ctx context.Context, f BlocklistFilter) (BlocklistPage, error) {
	page := f.Page
	if page < 1 {
		page = 1
	}
	pageSize := f.PageSize
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	sort := "id"
	switch f.Sort {
	case "reason":
		sort = "reason"
	case "key":
		sort = "key"
	case "expiresAt":
		sort = "expires_at"
	case "createdAt":
		sort = "created_at"
	}
	dir := "DESC"
	if strings.ToLower(f.Dir) == "asc" {
		dir = "ASC"
	}

	var args []any
	var where []string
	where = append(where, "(expires_at is null or expires_at > now())")
	if f.Q != "" {
		args = append(args, "%"+strings.ToLower(f.Q)+"%")
		where = append(where, fmt.Sprintf("(lower(key) like $%d or lower(reason) like $%d)", len(args), len(args)))
	}
	if f.Reason != "" {
		r := strings.TrimSpace(f.Reason)
		// Normalized preflight/strict-health categories: reason stored in DB contains
		// a unique segment ID that was stripped for display. Use LIKE to match both
		// old entries (with segment ID) and new entries (without).
		if (strings.HasPrefix(r, "preflight: ") || strings.HasPrefix(r, "strict health: ")) &&
			!strings.Contains(r, " segment ") {
			prefix := r[:strings.Index(r, ": ")+2] // "preflight: " or "strict health: "
			suffix := r[len(prefix):]
			args = append(args, prefix+"%", "%"+suffix+"%")
			where = append(where, fmt.Sprintf("(reason LIKE $%d AND reason LIKE $%d)", len(args)-1, len(args)))
		} else {
			args = append(args, r)
			where = append(where, fmt.Sprintf("reason = $%d", len(args)))
		}
	}

	whereClause := "WHERE " + strings.Join(where, " AND ")

	var total int
	countQuery := fmt.Sprintf(`select count(*) from blocklist_items %s`, whereClause)
	if err := db.SQL.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return BlocklistPage{}, err
	}

	offset := (page - 1) * pageSize
	countArgs := len(args)
	args = append(args, pageSize, offset)

	query := fmt.Sprintf(`
		select id, key, reason, created_at, expires_at
		from blocklist_items %s
		order by %s %s
		limit $%d offset $%d`,
		whereClause, sort, dir, countArgs+1, countArgs+2)

	rows, err := db.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return BlocklistPage{}, err
	}
	defer rows.Close()

	var items []BlocklistItemSummary
	for rows.Next() {
		var (
			item      BlocklistItemSummary
			expiresAt sql.NullTime
		)
		if err := rows.Scan(&item.ID, &item.Key, &item.Reason, &item.CreatedAt, &expiresAt); err != nil {
			return BlocklistPage{}, err
		}
		if expiresAt.Valid {
			value := expiresAt.Time
			item.ExpiresAt = &value
		}
		item.KeyType = blocklistKeyType(item.Key)
		if err := db.enrichBlocklistItem(ctx, &item); err != nil {
			return BlocklistPage{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return BlocklistPage{}, err
	}
	if items == nil {
		items = []BlocklistItemSummary{}
	}
	totalPages := (total + pageSize - 1) / pageSize
	if totalPages < 1 {
		totalPages = 1
	}
	return BlocklistPage{Items: items, Page: page, PageSize: pageSize, Total: total, TotalPages: totalPages}, nil
}

func blocklistKeyType(key string) string {
	switch {
	case strings.HasPrefix(key, "external_url:"):
		return "external_url"
	case strings.HasPrefix(key, "release_signature:"):
		return "release_signature"
	default:
		return "unknown"
	}
}

func (db *DB) enrichBlocklistItem(ctx context.Context, item *BlocklistItemSummary) error {
	if item == nil || strings.TrimSpace(item.Key) == "" {
		return nil
	}
	switch blocklistKeyType(item.Key) {
	case "external_url":
		return db.enrichBlocklistExternalURL(ctx, item, strings.TrimSpace(strings.TrimPrefix(item.Key, "external_url:")))
	case "release_signature":
		titleKey, indexerKey, sizeBucket, dateBucket := parseReleaseSignatureKey(item.Key)
		if titleKey == "" {
			return nil
		}
		return db.enrichBlocklistSignature(ctx, item, titleKey, indexerKey, sizeBucket, dateBucket)
	default:
		return nil
	}
}

func (db *DB) enrichBlocklistExternalURL(ctx context.Context, item *BlocklistItemSummary, externalURL string) error {
	if externalURL == "" {
		return nil
	}
	var (
		selectedReleaseID sql.NullInt64
		libraryItemID     sql.NullInt64
		releaseTitle      string
		indexerName       string
		sizeBytes         int64
		postedAt          sql.NullTime
	)
	err := db.SQL.QueryRowContext(ctx, `
		select
			sr.id,
			coalesce(sr.library_item_id, rc.library_item_id),
			coalesce(rc.title, ''),
			coalesce(rc.indexer_name, ''),
			coalesce(rc.size_bytes, 0),
			rc.posted_at
		from release_candidates rc
		left join selected_releases sr on sr.release_candidate_id = rc.id
		where rc.external_url = $1
		order by sr.id desc nulls last, rc.id desc
		limit 1`, externalURL,
	).Scan(&selectedReleaseID, &libraryItemID, &releaseTitle, &indexerName, &sizeBytes, &postedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	assignBlocklistMetadata(item, selectedReleaseID, libraryItemID, releaseTitle, indexerName, sizeBytes, postedAt)
	return nil
}

func (db *DB) enrichBlocklistSignature(ctx context.Context, item *BlocklistItemSummary, titleKey, indexerKey string, sizeBucket int64, dateBucket *time.Time) error {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			sr.id,
			coalesce(sr.library_item_id, rc.library_item_id),
			coalesce(rc.title, ''),
			coalesce(rc.indexer_name, ''),
			coalesce(rc.size_bytes, 0),
			rc.posted_at
		from release_candidates rc
		left join selected_releases sr on sr.release_candidate_id = rc.id
		where ($1 = '' or lower(trim(rc.indexer_name)) = $1)
		  and ($2 < 0 or coalesce(rc.size_bytes, 0) / (1024 * 1024) = $2)
		  and ($3::date is null or rc.posted_at::date = $3::date)
		order by sr.id desc nulls last, rc.id desc`, indexerKey, sizeBucket, dateBucket)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			selectedReleaseID sql.NullInt64
			libraryItemID     sql.NullInt64
			releaseTitle      string
			indexerName       string
			sizeBytes         int64
			postedAt          sql.NullTime
		)
		if err := rows.Scan(&selectedReleaseID, &libraryItemID, &releaseTitle, &indexerName, &sizeBytes, &postedAt); err != nil {
			return err
		}
		if normalizeReleaseTitleForBlocklist(releaseTitle) != titleKey {
			continue
		}
		assignBlocklistMetadata(item, selectedReleaseID, libraryItemID, releaseTitle, indexerName, sizeBytes, postedAt)
		return nil
	}
	return rows.Err()
}

func assignBlocklistMetadata(item *BlocklistItemSummary, selectedReleaseID, libraryItemID sql.NullInt64, releaseTitle, indexerName string, sizeBytes int64, postedAt sql.NullTime) {
	if selectedReleaseID.Valid {
		value := selectedReleaseID.Int64
		item.SelectedReleaseID = &value
	}
	if libraryItemID.Valid {
		value := libraryItemID.Int64
		item.LibraryItemID = &value
	}
	item.ReleaseTitle = releaseTitle
	item.IndexerName = indexerName
	item.SizeBytes = sizeBytes
	if postedAt.Valid {
		value := postedAt.Time
		item.PostedAt = &value
	}
}

func parseReleaseSignatureKey(key string) (titleKey, indexerKey string, sizeBucket int64, dateBucket *time.Time) {
	raw := strings.TrimSpace(strings.TrimPrefix(key, "release_signature:"))
	parts := strings.Split(raw, "|")
	if len(parts) != 4 {
		return "", "", -1, nil
	}
	titleKey = strings.TrimSpace(parts[0])
	indexerKey = strings.TrimSpace(parts[1])
	sizeBucket = -1
	if strings.TrimSpace(parts[2]) != "" && parts[2] != "0" {
		if v, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			sizeBucket = v
		}
	}
	if strings.TrimSpace(parts[3]) != "" && parts[3] != "none" {
		if v, err := time.Parse("2006-01-02", parts[3]); err == nil {
			dateBucket = &v
		}
	}
	return titleKey, indexerKey, sizeBucket, dateBucket
}

func normalizeReleaseTitleForBlocklist(value string) string {
	replacer := strings.NewReplacer(".", " ", "_", " ", "-", " ", "[", " ", "]", " ", "(", " ", ")", " ", "{", " ", "}", " ")
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(strings.TrimSpace(value)))), " ")
}

func (db *DB) BlocklistStats(ctx context.Context) (BlocklistStats, error) {
	var s BlocklistStats
	if err := db.SQL.QueryRowContext(ctx, `
		select
			count(*) as total,
			count(*) filter (where expires_at is not null and expires_at <= now()) as expired,
			count(*) filter (where expires_at is null or expires_at > now()) as active
		from blocklist_items`).Scan(&s.Total, &s.Expired, &s.Active); err != nil {
		return s, err
	}
	rows, err := db.SQL.QueryContext(ctx, `
		select norm_reason, sum(cnt)::int as cnt
		from (
			select
				case
					when reason ~ '^preflight: (first|last) segment \S+ unavailable: '
					then regexp_replace(
						regexp_replace(reason,
							'^preflight: (first|last) segment \S+ unavailable: ',
							'preflight: '),
						': [^: @]+@\S+$', '')
					when reason ~ '^strict health: (first|last) segment \S+ unavailable: '
					then regexp_replace(
						regexp_replace(reason,
							'^strict health: (first|last) segment \S+ unavailable: ',
							'strict health: '),
						': [^: @]+@\S+$', '')
					else reason
				end as norm_reason,
				count(*) as cnt
			from blocklist_items
			where expires_at is null or expires_at > now()
			group by reason
		) sub
		group by norm_reason
		order by cnt desc`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	s.ByReason = make(map[string]int)
	for rows.Next() {
		var reason string
		var cnt int
		if err := rows.Scan(&reason, &cnt); err != nil {
			continue
		}
		s.ByReason[reason] = cnt
	}
	return s, rows.Err()
}

// ListBlocklistItems is kept for backwards compatibility with existing callers.
func (db *DB) ListBlocklistItems(ctx context.Context) ([]BlocklistItemSummary, error) {
	page, err := db.ListBlocklistItemsPaged(ctx, BlocklistFilter{PageSize: 1000})
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

func (db *DB) DeleteBlocklistItem(ctx context.Context, id int64) error {
	result, err := db.SQL.ExecContext(ctx, `delete from blocklist_items where id = $1`, id)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (db *DB) CreateBlocklistItem(ctx context.Context, item BlocklistMutation) (BlocklistItemSummary, error) {
	reason := strings.TrimSpace(item.Reason)
	if reason == "" {
		reason = "manual"
	}
	var created BlocklistItemSummary
	var expiresAt sql.NullTime
	err := db.SQL.QueryRowContext(ctx, `
		insert into blocklist_items (key, reason, expires_at, created_at)
		values ($1, $2, $3, now())
		returning id, key, reason, created_at, expires_at`,
		strings.TrimSpace(item.Key), reason, item.ExpiresAt,
	).Scan(&created.ID, &created.Key, &created.Reason, &created.CreatedAt, &expiresAt)
	if err != nil {
		return BlocklistItemSummary{}, err
	}
	if expiresAt.Valid {
		value := expiresAt.Time
		created.ExpiresAt = &value
	}
	created.KeyType = blocklistKeyType(created.Key)
	if err := db.enrichBlocklistItem(ctx, &created); err != nil {
		return BlocklistItemSummary{}, err
	}
	return created, nil
}

func (db *DB) UpdateBlocklistItem(ctx context.Context, id int64, item BlocklistMutation) (BlocklistItemSummary, error) {
	reason := strings.TrimSpace(item.Reason)
	if reason == "" {
		reason = "manual"
	}
	var updated BlocklistItemSummary
	var expiresAt sql.NullTime
	err := db.SQL.QueryRowContext(ctx, `
		update blocklist_items
		set key = $2,
		    reason = $3,
		    expires_at = $4
		where id = $1
		returning id, key, reason, created_at, expires_at`,
		id, strings.TrimSpace(item.Key), reason, item.ExpiresAt,
	).Scan(&updated.ID, &updated.Key, &updated.Reason, &updated.CreatedAt, &expiresAt)
	if err != nil {
		return BlocklistItemSummary{}, err
	}
	if expiresAt.Valid {
		value := expiresAt.Time
		updated.ExpiresAt = &value
	}
	updated.KeyType = blocklistKeyType(updated.Key)
	if err := db.enrichBlocklistItem(ctx, &updated); err != nil {
		return BlocklistItemSummary{}, err
	}
	return updated, nil
}

func (db *DB) DeleteAllBlocklistItems(ctx context.Context) (int, error) {
	result, err := db.SQL.ExecContext(ctx, `
		delete from blocklist_items
		where expires_at is null or expires_at > now()`)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rows), nil
}

func (db *DB) DeleteBlocklistItemsByReason(ctx context.Context, reason string) (int, error) {
	result, err := db.SQL.ExecContext(ctx, `
		delete from blocklist_items
		where reason = $1 and (expires_at is null or expires_at > now())`, reason)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(rows), nil
}

package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/hjongedijk/drakkar/internal/policy"
	"github.com/hjongedijk/drakkar/internal/stream"
)

// bulkInsertSegments inserts all segments for one NZB file in a single query
// (one round-trip instead of N). Returns the inserted IDs in segment order.
func bulkInsertSegments(ctx context.Context, tx *sql.Tx, nzbFileID int64, segments []ImportedNZBSegment) ([]int64, error) {
	if len(segments) == 0 {
		return nil, nil
	}
	// Build: INSERT INTO nzb_segments (...) VALUES ($1,$2,$3,$4,$5,$6,'unknown'), ... RETURNING id
	sb := strings.Builder{}
	sb.WriteString(`insert into nzb_segments (nzb_file_id, segment_number, message_id, encoded_size_bytes, decoded_start_offset, decoded_end_offset, availability_status) values `)
	args := make([]interface{}, 0, len(segments)*6)
	for i, seg := range segments {
		if i > 0 {
			sb.WriteByte(',')
		}
		base := i * 6
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,'unknown')", base+1, base+2, base+3, base+4, base+5, base+6)
		args = append(args, nzbFileID, seg.Number, seg.MessageID, seg.EncodedSizeBytes, seg.DecodedStartOffset, seg.DecodedEndOffset)
	}
	sb.WriteString(` returning id`)
	rows, err := tx.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0, len(segments))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

type importedFileSegments struct {
	fileName string
	spans    []stream.SegmentSpan
	ids      []int64
}

func (db *DB) ListQueue(ctx context.Context) ([]QueueSnapshot, error) {
	// Return active items (all states except requested) + last 200 available/failed.
	// Skipping 'requested' items keeps the response fast — there can be thousands
	// and they're not actionable until the search worker picks them up.
	rows, err := db.SQL.QueryContext(ctx, `
		with active as (
			select q.id from queue_items q
			where q.state not in ('requested', 'available', 'failed')
		),
		recent_history as (
			select q.id from queue_items q
			where q.state in ('available', 'failed')
			order by q.updated_at desc, q.id desc
			limit 200
		),
		ids as (select id from active union select id from recent_history)
		select
			q.id,
			q.library_item_id,
			l.title,
			q.state,
			q.failure_reason,
			q.idempotency_key,
			q.selected_release_id,
			n.id,
			coalesce(n.file_name, ''),
			coalesce((select count(*) from nzb_files nf where nf.nzb_document_id = n.id), 0),
			coalesce((select count(*) from nzb_segments ns join nzb_files nf on nf.id = ns.nzb_file_id where nf.nzb_document_id = n.id), 0),
			q.created_at,
			q.updated_at
		from queue_items q
		join ids on ids.id = q.id
		join library_items l on l.id = q.library_item_id
		left join selected_releases sr on sr.id = q.selected_release_id
		left join nzb_documents n on n.selected_release_id = sr.id
		order by
			case q.state
				when 'fetching_nzb' then 0
				when 'indexing' then 1
				when 'preflight' then 2
				when 'publishing' then 3
				when 'selected' then 4
				when 'ranking' then 5
				when 'searching' then 6
				when 'available' then 7
				when 'failed' then 8
				else 9
			end asc,
			q.updated_at desc,
			q.id desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []QueueSnapshot
	for rows.Next() {
		var item QueueSnapshot
		var selectedRelease sql.NullInt64
		var nzbDocument sql.NullInt64
		if err := rows.Scan(
			&item.QueueItemID,
			&item.LibraryItemID,
			&item.LibraryTitle,
			&item.State,
			&item.FailureReason,
			&item.IdempotencyKey,
			&selectedRelease,
			&nzbDocument,
			&item.NZBFileName,
			&item.NZBFileCount,
			&item.NZBSegmentCount,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if selectedRelease.Valid {
			value := selectedRelease.Int64
			item.SelectedRelease = &value
		}
		if nzbDocument.Valid {
			value := nzbDocument.Int64
			item.NZBDocumentID = &value
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) CreateImportedNZB(ctx context.Context, imported ImportedNZB) (QueueSnapshot, error) {
	imported = db.applyImportPolicies(ctx, imported)
	imported.Archives = inspectImportedArchives(ctx, imported.Archives, imported.Files, db.SegmentFetcher)
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return QueueSnapshot{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var snapshot QueueSnapshot
	var existing bool
	err = tx.QueryRowContext(ctx, `select exists(select 1 from queue_items where idempotency_key = $1)`, imported.IdempotencyKey).Scan(&existing)
	if err != nil {
		return QueueSnapshot{}, err
	}
	if existing {
		if err := tx.Rollback(); err != nil {
			return QueueSnapshot{}, err
		}
		items, err := db.ListQueue(ctx)
		if err != nil {
			return QueueSnapshot{}, err
		}
		for _, item := range items {
			if item.IdempotencyKey == imported.IdempotencyKey {
				return item, nil
			}
		}
		return QueueSnapshot{}, errors.New("existing queue item not found after idempotency hit")
	}

	var libraryItemID int64
	if err = tx.QueryRowContext(ctx, `
		insert into library_items (media_type, title)
		values ('manual_nzb', $1)
		returning id`, imported.FileName).Scan(&libraryItemID); err != nil {
		return QueueSnapshot{}, err
	}

	var releaseCandidateID int64
	if err = tx.QueryRowContext(ctx, `
		insert into release_candidates (library_item_id, title, score, selected)
		values ($1, $2, 0, true)
		returning id`, libraryItemID, imported.FileName).Scan(&releaseCandidateID); err != nil {
		return QueueSnapshot{}, err
	}

	var selectedReleaseID int64
	if err = tx.QueryRowContext(ctx, `
		insert into selected_releases (library_item_id, release_candidate_id)
		values ($1, $2)
		returning id`, libraryItemID, releaseCandidateID).Scan(&selectedReleaseID); err != nil {
		return QueueSnapshot{}, err
	}

	var nzbDocumentID int64
	if err = tx.QueryRowContext(ctx, `
		insert into nzb_documents (selected_release_id, file_name, xml)
		values ($1, $2, $3)
		returning id`, selectedReleaseID, imported.FileName, imported.XML).Scan(&nzbDocumentID); err != nil {
		return QueueSnapshot{}, err
	}

	fileSegments := make(map[string]importedFileSegments, len(imported.Files))
	for _, file := range imported.Files {
		var postedAt any
		if file.PostedUnix > 0 {
			postedAt = time.Unix(file.PostedUnix, 0).UTC()
		}
		var nzbFileID int64
		if err = tx.QueryRowContext(ctx, `
			insert into nzb_files (nzb_document_id, subject, poster, posted_at, file_size_bytes)
			values ($1, $2, $3, $4, $5)
			returning id`,
			nzbDocumentID, file.Subject, file.Poster, postedAt, file.FileSizeBytes,
		).Scan(&nzbFileID); err != nil {
			return QueueSnapshot{}, err
		}

		segmentIDs, err := bulkInsertSegments(ctx, tx, nzbFileID, file.Segments)
		if err != nil {
			return QueueSnapshot{}, err
		}
		fileSegments[file.FileName] = importedFileSegments{
			fileName: file.FileName,
			ids:      segmentIDs,
			spans:    importedSegmentSpans(file.Segments, segmentIDs),
		}

		if isPlayableMedia(file.FileName) {
			virtualPath := "releases/" + fmt.Sprintf("%d", selectedReleaseID) + "/" + file.FileName
			var virtualFileID int64
			if err = tx.QueryRowContext(ctx, `
				insert into virtual_files (
					selected_release_id, path, file_name, size_bytes, reader_kind
				) values ($1, $2, $3, $4, 'direct_nzb')
				returning id`,
				selectedReleaseID, virtualPath, file.FileName, file.FileSizeBytes,
			).Scan(&virtualFileID); err != nil {
				return QueueSnapshot{}, err
			}
			for i, segment := range file.Segments {
				if _, err = tx.ExecContext(ctx, `
					insert into virtual_file_ranges (virtual_file_id, nzb_segment_id, range_start, range_end)
					values ($1, $2, $3, $4)`,
					virtualFileID,
					segmentIDs[i],
					segment.DecodedStartOffset,
					segment.DecodedEndOffset,
				); err != nil {
					return QueueSnapshot{}, err
				}
			}
		}
	}
	if err = insertImportedArchives(ctx, tx, selectedReleaseID, imported.Archives, fileSegments); err != nil {
		return QueueSnapshot{}, err
	}

	if err = tx.QueryRowContext(ctx, `
		insert into queue_items (library_item_id, state, idempotency_key, selected_release_id)
		values ($1, $2, $3, $4)
		returning id, created_at, updated_at`,
		libraryItemID, QueueIndexing, imported.IdempotencyKey, selectedReleaseID,
	).Scan(&snapshot.QueueItemID, &snapshot.CreatedAt, &snapshot.UpdatedAt); err != nil {
		return QueueSnapshot{}, err
	}

	snapshot.LibraryItemID = libraryItemID
	snapshot.LibraryTitle = imported.FileName
	snapshot.State = QueueIndexing
	snapshot.IdempotencyKey = imported.IdempotencyKey
	snapshot.SelectedRelease = &selectedReleaseID
	snapshot.NZBDocumentID = &nzbDocumentID
	snapshot.NZBFileName = imported.FileName
	snapshot.NZBFileCount = imported.FileCount
	snapshot.NZBSegmentCount = imported.SegmentCount

	if err = tx.Commit(); err != nil {
		return QueueSnapshot{}, err
	}
	return snapshot, nil
}

func (db *DB) ImportSelectedReleaseNZB(ctx context.Context, selectedReleaseID int64, imported ImportedNZB) (QueueSnapshot, error) {
	imported = db.applyImportPolicies(ctx, imported)
	imported.Archives = inspectImportedArchives(ctx, imported.Archives, imported.Files, db.SegmentFetcher)
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return QueueSnapshot{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var snapshot QueueSnapshot
	err = tx.QueryRowContext(ctx, `
		select q.id, q.library_item_id, l.title, q.idempotency_key, q.created_at
		from queue_items q
		join library_items l on l.id = q.library_item_id
		where q.selected_release_id = $1`, selectedReleaseID,
	).Scan(&snapshot.QueueItemID, &snapshot.LibraryItemID, &snapshot.LibraryTitle, &snapshot.IdempotencyKey, &snapshot.CreatedAt)
	if err != nil {
		return QueueSnapshot{}, err
	}

	if _, err = tx.ExecContext(ctx, `
		delete from nzb_documents
		where selected_release_id = $1`, selectedReleaseID); err != nil {
		return QueueSnapshot{}, err
	}
	if _, err = tx.ExecContext(ctx, `
		delete from virtual_files
		where selected_release_id = $1`, selectedReleaseID); err != nil {
		return QueueSnapshot{}, err
	}
	if _, err = tx.ExecContext(ctx, `
		delete from archives
		where selected_release_id = $1`, selectedReleaseID); err != nil {
		return QueueSnapshot{}, err
	}

	var nzbDocumentID int64
	if err = tx.QueryRowContext(ctx, `
		insert into nzb_documents (selected_release_id, external_url, file_name, xml)
		values ($1, $2, $3, $4)
		returning id`, selectedReleaseID, imported.ExternalURL, imported.FileName, imported.XML).Scan(&nzbDocumentID); err != nil {
		return QueueSnapshot{}, err
	}

	fileSegments := make(map[string]importedFileSegments, len(imported.Files))
	for _, file := range imported.Files {
		var postedAt any
		if file.PostedUnix > 0 {
			postedAt = time.Unix(file.PostedUnix, 0).UTC()
		}
		var nzbFileID int64
		if err = tx.QueryRowContext(ctx, `
			insert into nzb_files (nzb_document_id, subject, poster, posted_at, file_size_bytes)
			values ($1, $2, $3, $4, $5)
			returning id`,
			nzbDocumentID, file.Subject, file.Poster, postedAt, file.FileSizeBytes,
		).Scan(&nzbFileID); err != nil {
			return QueueSnapshot{}, err
		}
		segmentIDs, err := bulkInsertSegments(ctx, tx, nzbFileID, file.Segments)
		if err != nil {
			return QueueSnapshot{}, err
		}
		fileSegments[file.FileName] = importedFileSegments{
			fileName: file.FileName,
			ids:      segmentIDs,
			spans:    importedSegmentSpans(file.Segments, segmentIDs),
		}
		if isPlayableMedia(file.FileName) {
			virtualPath := "releases/" + fmt.Sprintf("%d", selectedReleaseID) + "/" + file.FileName
			var virtualFileID int64
			if err = tx.QueryRowContext(ctx, `
				insert into virtual_files (
					selected_release_id, path, file_name, size_bytes, reader_kind
				) values ($1, $2, $3, $4, 'direct_nzb')
				returning id`,
				selectedReleaseID, virtualPath, file.FileName, file.FileSizeBytes,
			).Scan(&virtualFileID); err != nil {
				return QueueSnapshot{}, err
			}
			for i, segment := range file.Segments {
				if _, err = tx.ExecContext(ctx, `
					insert into virtual_file_ranges (virtual_file_id, nzb_segment_id, range_start, range_end)
					values ($1, $2, $3, $4)`,
					virtualFileID,
					segmentIDs[i],
					segment.DecodedStartOffset,
					segment.DecodedEndOffset,
				); err != nil {
					return QueueSnapshot{}, err
				}
			}
		}
	}
	if err = insertImportedArchives(ctx, tx, selectedReleaseID, imported.Archives, fileSegments); err != nil {
		return QueueSnapshot{}, err
	}

	if err = tx.QueryRowContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', updated_at = now()
		where id = $1
		returning updated_at`, snapshot.QueueItemID, QueueIndexing,
	).Scan(&snapshot.UpdatedAt); err != nil {
		return QueueSnapshot{}, err
	}

	snapshot.State = QueueIndexing
	snapshot.SelectedRelease = &selectedReleaseID
	snapshot.NZBDocumentID = &nzbDocumentID
	snapshot.NZBFileName = imported.FileName
	snapshot.NZBFileCount = imported.FileCount
	snapshot.NZBSegmentCount = imported.SegmentCount

	if err = tx.Commit(); err != nil {
		return QueueSnapshot{}, err
	}
	return snapshot, nil
}

func insertImportedArchives(ctx context.Context, tx *sql.Tx, selectedReleaseID int64, archives []ImportedArchive, fileSegments map[string]importedFileSegments) error {
	for _, archive := range archives {
		var archiveID int64
		if err := tx.QueryRowContext(ctx, `
			insert into archives (selected_release_id, kind, status, reject_reason)
			values ($1, $2, $3, $4)
			returning id`,
			selectedReleaseID,
			archive.Kind,
			archive.Status,
			archive.RejectReason,
		).Scan(&archiveID); err != nil {
			return err
		}
		volumeIDs := make(map[int]int64, len(archive.Volumes))
		volumePaths := make(map[int]string, len(archive.Volumes))
		for _, volume := range archive.Volumes {
			var archiveVolumeID int64
			if err := tx.QueryRowContext(ctx, `
				insert into archive_volumes (archive_id, path, volume_index)
				values ($1, $2, $3)
				returning id`,
				archiveID,
				volume.Path,
				volume.VolumeIndex,
			).Scan(&archiveVolumeID); err != nil {
				return err
			}
			volumeIDs[volume.VolumeIndex] = archiveVolumeID
			volumePaths[volume.VolumeIndex] = volume.Path
		}
		for _, entry := range archive.Entries {
			var archiveEntryID int64
			if err := tx.QueryRowContext(ctx, `
				insert into archive_entries (
					archive_id, path, size_bytes, packed_size_bytes,
					compression_method, encrypted, solid, source_volume_index, source_archive_offset
				)
				values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				returning id`,
				archiveID,
				entry.Path,
				entry.SizeBytes,
				entry.PackedSizeBytes,
				entry.CompressionMethod,
				entry.Encrypted,
				entry.Solid,
				entry.VolumeIndex,
				entry.ArchiveOffset,
			).Scan(&archiveEntryID); err != nil {
				return err
			}
			for _, item := range entry.Ranges {
				archiveVolumeID, ok := volumeIDs[item.VolumeIndex]
				if !ok {
					continue
				}
				if _, err := tx.ExecContext(ctx, `
					insert into archive_ranges (archive_entry_id, archive_volume_id, entry_offset, archive_offset, length_bytes)
					values ($1, $2, $3, $4, $5)`,
					archiveEntryID,
					archiveVolumeID,
					item.EntryOffset,
					item.ArchiveOffset,
					item.LengthBytes,
				); err != nil {
					return err
				}
			}
			if archive.Status != "supported" || !isPlayableMedia(entry.Path) || len(entry.Ranges) == 0 {
				continue
			}
			virtualFileID, err := insertArchiveVirtualFile(ctx, tx, selectedReleaseID, entry, volumePaths, fileSegments)
			if err != nil {
				return err
			}
			_ = virtualFileID
		}
	}
	return nil
}

func insertArchiveVirtualFile(ctx context.Context, tx *sql.Tx, selectedReleaseID int64, entry ImportedArchiveEntry, volumePaths map[int]string, fileSegments map[string]importedFileSegments) (int64, error) {
	virtualPath := "releases/" + fmt.Sprintf("%d", selectedReleaseID) + "/" + entry.Path
	var virtualFileID int64
	if err := tx.QueryRowContext(ctx, `
		insert into virtual_files (
			selected_release_id, path, file_name, size_bytes, reader_kind
		) values ($1, $2, $3, $4, 'stored_rar')
		returning id`,
		selectedReleaseID, virtualPath, entry.Path, entry.SizeBytes,
	).Scan(&virtualFileID); err != nil {
		return 0, err
	}
	for _, item := range entry.Ranges {
		volumePath, ok := volumePaths[item.VolumeIndex]
		if !ok {
			continue
		}
		source, ok := fileSegments[volumePath]
		if !ok {
			continue
		}
		ranges, err := resolveArchiveEntryRanges(source, item)
		if err != nil {
			return 0, err
		}
		for _, resolved := range ranges {
			if _, err := tx.ExecContext(ctx, `
				insert into virtual_file_ranges (virtual_file_id, nzb_segment_id, range_start, range_end)
				values ($1, $2, $3, $4)`,
				virtualFileID,
				resolved.SegmentID,
				resolved.RangeStart,
				resolved.RangeEnd,
			); err != nil {
				return 0, err
			}
		}
	}
	return virtualFileID, nil
}

type resolvedArchiveRange struct {
	SegmentID  int64
	RangeStart int64
	RangeEnd   int64
}

func resolveArchiveEntryRanges(source importedFileSegments, item ImportedArchiveRange) ([]resolvedArchiveRange, error) {
	parts, err := stream.ResolveRange(source.spans, item.ArchiveOffset, item.LengthBytes)
	if err != nil {
		return nil, err
	}
	out := make([]resolvedArchiveRange, 0, len(parts))
	for _, part := range parts {
		start := item.EntryOffset + (part.RangeStart - item.ArchiveOffset)
		end := start + (part.RangeEnd - part.RangeStart)
		out = append(out, resolvedArchiveRange{
			SegmentID:  part.SegmentID,
			RangeStart: start,
			RangeEnd:   end,
		})
	}
	return out, nil
}

func importedSegmentSpans(segments []ImportedNZBSegment, segmentIDs []int64) []stream.SegmentSpan {
	out := make([]stream.SegmentSpan, 0, len(segments))
	for i, segment := range segments {
		out = append(out, stream.SegmentSpan{
			SegmentID: segmentIDs[i],
			MessageID: segment.MessageID,
			Start:     segment.DecodedStartOffset,
			End:       segment.DecodedEndOffset,
		})
	}
	return out
}

func (db *DB) MarkSelectedReleaseFetching(ctx context.Context, selectedReleaseID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', updated_at = now()
		where selected_release_id = $1`, selectedReleaseID, QueueFetchingNZB)
	return err
}

func (db *DB) SetImportedNZBIndexed(ctx context.Context, queueItemID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, updated_at = now()
		where id = $1`, queueItemID, QueuePreflight)
	return err
}

func (db *DB) CancelNZBDocument(ctx context.Context, nzbDocumentID int64) error {
	result, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = 'cancelled', updated_at = now()
		where selected_release_id in (
			select selected_release_id from nzb_documents where id = $1
		)`, nzbDocumentID, QueueFailed)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("nzb document %d not found", nzbDocumentID)
	}
	return nil
}

func (db *DB) ListNZBMountEntries(ctx context.Context) ([]NZBMountEntry, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			n.id,
			n.file_name,
			n.xml,
			q.state
		from nzb_documents n
		join selected_releases sr on sr.id = n.selected_release_id
		join queue_items q on q.selected_release_id = sr.id
		where q.state not in ($1, $2)
		order by n.id asc`, QueueFailed, QueueAvailable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []NZBMountEntry
	for rows.Next() {
		var item NZBMountEntry
		if err := rows.Scan(&item.DocumentID, &item.FileName, &item.XML, &item.State); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func isPlayableMedia(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv", ".mp4", ".avi":
		return true
	default:
		return false
	}
}

func (db *DB) applyImportPolicies(ctx context.Context, imported ImportedNZB) ImportedNZB {
	settings := policy.DefaultSettings()
	if db != nil {
		var stored policy.Settings
		if ok, err := db.GetAppSetting(ctx, policy.SettingsKey, &stored); err == nil && ok {
			settings = policy.Merge(settings, stored)
		}
	}
	return filterImportedByPatterns(imported, settings.IgnoredPatterns)
}

func filterImportedByPatterns(imported ImportedNZB, patterns []string) ImportedNZB {
	if len(patterns) == 0 || len(imported.Files) == 0 {
		return imported
	}
	filtered := imported
	filtered.Files = filtered.Files[:0]
	filtered.Archives = nil
	filtered.FileCount = 0
	filtered.SegmentCount = 0
	for _, file := range imported.Files {
		if matchesIgnoredPattern(file.FileName, patterns) {
			continue
		}
		filtered.Files = append(filtered.Files, file)
		filtered.FileCount++
		filtered.SegmentCount += len(file.Segments)
	}
	return filtered
}

func matchesIgnoredPattern(name string, patterns []string) bool {
	base := strings.ToLower(filepath.Base(strings.TrimSpace(name)))
	if base == "" {
		return false
	}
	for _, pattern := range patterns {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		if pattern == "" {
			continue
		}
		if ok, err := filepath.Match(pattern, base); err == nil && ok {
			return true
		}
	}
	return false
}

// ClearFailedQueueItems resets all failed queue items back to 'requested' so
// they leave the history view and re-enter the search queue on the next pass.
// Returns the number of items reset.
func (db *DB) ClearFailedQueueItems(ctx context.Context) (int, error) {
	result, err := db.SQL.ExecContext(ctx, `
		UPDATE queue_items SET
			state       = $1,
			failure_reason = '',
			updated_at  = now()
		WHERE state = $2`,
		QueueRequested, QueueFailed)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// ResetStuckQueueItems resets items stuck in transitional states (fetching_nzb,
// indexing, publishing, preflight, searching, ranking) back to failed so they
// will be retried by the next monitoring pass. This runs on startup to recover
// from in-progress work that was interrupted by a process restart.
func (db *DB) ResetStuckQueueItems(ctx context.Context) (int, error) {
	result, err := db.SQL.ExecContext(ctx, `
		UPDATE queue_items SET
			state = $1,
			failure_reason = 'interrupted_by_restart',
			updated_at = now()
		WHERE state IN ($2, $3, $4, $5, $6, $7, $8)
		  AND state != $1`,
		QueueFailed,
		QueueFetchingNZB, QueueIndexing, QueuePublishing,
		QueuePreflight, QueueSearching, QueueRanking, QueueSelected,
	)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

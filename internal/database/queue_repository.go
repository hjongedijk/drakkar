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
)

type importedFileSegments struct {
	fileName  string
	nzbFileID int64
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
			coalesce((select sum(array_length(nf.message_ids, 1)) from nzb_files nf where nf.nzb_document_id = n.id), 0),
			q.created_at,
			q.updated_at
		from queue_items q
		join ids on ids.id = q.id
		join library_items l on l.id = q.library_item_id
		left join selected_releases sr on sr.id = q.selected_release_id
		left join lateral (
			select id, file_name
			from nzb_documents
			where selected_release_id = sr.id
			order by id desc limit 1
		) n on sr.id is not null
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

	mediaType := imported.MediaType
	if mediaType == "" {
		mediaType = "manual_nzb"
	}
	var libraryItemID int64
	if err = tx.QueryRowContext(ctx, `
		insert into library_items (media_type, title)
		values ($1, $2)
		returning id`, mediaType, imported.FileName).Scan(&libraryItemID); err != nil {
		return QueueSnapshot{}, err
	}

	var releaseCandidateID int64
	if err = tx.QueryRowContext(ctx, `
		insert into release_candidates (library_item_id, title, score, custom_format_score, selected)
		values ($1, $2, 0, 0, true)
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
		returning id`, selectedReleaseID, imported.FileName, compressNZBXML(imported.XML)).Scan(&nzbDocumentID); err != nil {
		return QueueSnapshot{}, err
	}

	fileSegments := make(map[string]importedFileSegments, len(imported.Files))
	for _, file := range imported.Files {
		var postedAt any
		if file.PostedUnix > 0 {
			postedAt = time.Unix(file.PostedUnix, 0).UTC()
		}
		msgIDs := make([]string, len(file.Segments))
		for i, s := range file.Segments {
			msgIDs[i] = s.MessageID
		}
		decSegSize, lastDecSize := segmentSizes(file.Segments)
		var nzbFileID int64
		if err = tx.QueryRowContext(ctx, `
			insert into nzb_files (nzb_document_id, subject, poster, posted_at, file_size_bytes, message_ids, decoded_segment_size, last_decoded_size)
			values ($1, $2, $3, $4, $5, $6, $7, $8)
			returning id`,
			nzbDocumentID, file.Subject, file.Poster, postedAt, file.FileSizeBytes,
			pgTextArray(msgIDs), decSegSize, lastDecSize,
		).Scan(&nzbFileID); err != nil {
			return QueueSnapshot{}, err
		}

		fileSegments[file.FileName] = importedFileSegments{
			fileName:  file.FileName,
			nzbFileID: nzbFileID,
		}

		if isPlayableMedia(file.FileName) {
			virtualPath := "releases/" + fmt.Sprintf("%d", selectedReleaseID) + "/" + file.FileName
			if err = tx.QueryRowContext(ctx, `
				insert into virtual_files (
					selected_release_id, path, file_name, size_bytes, reader_kind,
					nzb_file_id, segment_byte_offset
				) values ($1, $2, $3, $4, 'direct_nzb', $5, 0)
				returning id`,
				selectedReleaseID, virtualPath, file.FileName, file.FileSizeBytes, nzbFileID,
			).Scan(new(int64)); err != nil {
				return QueueSnapshot{}, err
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
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
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
		returning id`, selectedReleaseID, imported.ExternalURL, imported.FileName, compressNZBXML(imported.XML)).Scan(&nzbDocumentID); err != nil {
		return QueueSnapshot{}, err
	}

	fileSegments := make(map[string]importedFileSegments, len(imported.Files))
	for _, file := range imported.Files {
		var postedAt any
		if file.PostedUnix > 0 {
			postedAt = time.Unix(file.PostedUnix, 0).UTC()
		}
		msgIDs := make([]string, len(file.Segments))
		for i, s := range file.Segments {
			msgIDs[i] = s.MessageID
		}
		decSegSize, lastDecSize := segmentSizes(file.Segments)
		var nzbFileID int64
		if err = tx.QueryRowContext(ctx, `
			insert into nzb_files (nzb_document_id, subject, poster, posted_at, file_size_bytes, message_ids, decoded_segment_size, last_decoded_size)
			values ($1, $2, $3, $4, $5, $6, $7, $8)
			returning id`,
			nzbDocumentID, file.Subject, file.Poster, postedAt, file.FileSizeBytes,
			pgTextArray(msgIDs), decSegSize, lastDecSize,
		).Scan(&nzbFileID); err != nil {
			return QueueSnapshot{}, err
		}
		fileSegments[file.FileName] = importedFileSegments{
			fileName:  file.FileName,
			nzbFileID: nzbFileID,
		}
		if isPlayableMedia(file.FileName) {
			virtualPath := "releases/" + fmt.Sprintf("%d", selectedReleaseID) + "/" + file.FileName
			if err = tx.QueryRowContext(ctx, `
				insert into virtual_files (
					selected_release_id, path, file_name, size_bytes, reader_kind,
					nzb_file_id, segment_byte_offset
				) values ($1, $2, $3, $4, 'direct_nzb', $5, 0)
				returning id`,
				selectedReleaseID, virtualPath, file.FileName, file.FileSizeBytes, nzbFileID,
			).Scan(new(int64)); err != nil {
				return QueueSnapshot{}, err
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
	// For stored_rar entries that span multiple volumes, only the first range's
	// NZB file and archive offset are stored — multi-volume RAR is not supported
	// for streaming, so we use the first volume's source as the reference.
	var nzbFileID int64
	archiveByteOffset := entry.ArchiveOffset // byte offset in the NZB file decoded content
	for _, item := range entry.Ranges {
		volumePath, ok := volumePaths[item.VolumeIndex]
		if !ok {
			continue
		}
		source, ok := fileSegments[volumePath]
		if !ok {
			continue
		}
		nzbFileID = source.nzbFileID
		archiveByteOffset = item.ArchiveOffset
		break
	}

	virtualPath := "releases/" + fmt.Sprintf("%d", selectedReleaseID) + "/" + entry.Path
	var virtualFileID int64
	if err := tx.QueryRowContext(ctx, `
		insert into virtual_files (
			selected_release_id, path, file_name, size_bytes, reader_kind,
			nzb_file_id, segment_byte_offset
		) values ($1, $2, $3, $4, 'stored_rar', $5, $6)
		returning id`,
		selectedReleaseID, virtualPath, entry.Path, entry.SizeBytes,
		nzbFileID, archiveByteOffset,
	).Scan(&virtualFileID); err != nil {
		return 0, err
	}
	return virtualFileID, nil
}


// segmentSizes returns (decodedSegmentSize, lastDecodedSize) from the imported segments.
// decodedSegmentSize is the size of the first (uniform) segment; lastDecodedSize is the
// size of the final segment (which may differ).
func segmentSizes(segments []ImportedNZBSegment) (int64, int64) {
	if len(segments) == 0 {
		return 0, 0
	}
	first := segments[0].DecodedEndOffset - segments[0].DecodedStartOffset
	last := segments[len(segments)-1].DecodedEndOffset - segments[len(segments)-1].DecodedStartOffset
	return first, last
}

func (db *DB) MarkSelectedReleaseFetching(ctx context.Context, selectedReleaseID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', updated_at = now()
		where selected_release_id = $1`, selectedReleaseID, QueueFetchingNZB)
	return err
}

// StoreRawNZBDocument persists the raw NZB bytes immediately after download,
// before any preflight or segment indexing. This ensures NZBDocumentID is set
// even if the app crashes during subsequent processing — preventing a re-download
// from the indexer on the next attempt (the NZBDocumentID != nil check skips the fetch).
// ImportSelectedReleaseNZB will later overwrite this row with the full indexed version.
func (db *DB) StoreRawNZBDocument(ctx context.Context, selectedReleaseID int64, fileName string, xml []byte, externalURL string) error {
	_, err := db.SQL.ExecContext(ctx, `
		insert into nzb_documents (selected_release_id, external_url, file_name, xml)
		select $1, $2, $3, $4
		where not exists (
			select 1 from nzb_documents where selected_release_id = $1
		)`,
		selectedReleaseID, externalURL, fileName, compressNZBXML(xml))
	return err
}

func (db *DB) SetImportedNZBIndexed(ctx context.Context, queueItemID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, updated_at = now()
		where id = $1`, queueItemID, QueuePreflight)
	return err
}

func (db *DB) MarkQueueItemPublishing(ctx context.Context, queueItemID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, updated_at = now()
		where id = $1`, queueItemID, QueuePublishing)
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
		var decErr error
		if item.XML, decErr = decompressNZBXML(item.XML); decErr != nil {
			return nil, decErr
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func isPlayableMedia(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv", ".mp4", ".avi":
		return !isSampleFilename(name)
	default:
		return false
	}
}

// isSampleFilename returns true when the filename (without extension) is or
// looks like a sample clip: exactly "sample", "sample-something",
// "something-sample", etc. Mirrors the reSample logic in ranking.
func isSampleFilename(name string) bool {
	base := strings.ToLower(strings.TrimSuffix(filepath.Base(name), filepath.Ext(name)))
	return base == "sample" ||
		strings.HasPrefix(base, "sample-") ||
		strings.HasPrefix(base, "sample_") ||
		strings.HasSuffix(base, "-sample") ||
		strings.HasSuffix(base, "_sample")
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
	// Re-detect archives from the filtered file list — clearing then rebuilding
	// ensures that files removed by the ignore patterns (e.g. .nfo) don't
	// leave orphaned volume references in the archive groups.
	filtered.Archives = DetectImportedArchives(filtered.Files)
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

// RecoverInterruptedDownloads resets all failed items whose failure reason is
// 'interrupted_by_restart' or 'stale_worker' and that have a selected release
// back to 'requested' so they re-enter the normal download cycle.  This runs
// once on startup to instantly recover the full backlog without the 500-item
// per-pass limit that RetryFailedQueue imposes (needed to cap Hydra calls for
// non-stale items, but irrelevant here since no Hydra call is made).
func (db *DB) RecoverInterruptedDownloads(ctx context.Context) (int, error) {
	result, err := db.SQL.ExecContext(ctx, `
		UPDATE queue_items
		SET state = $1, failure_reason = '', updated_at = now()
		WHERE state = $2
		  AND failure_reason IN ('interrupted_by_restart', 'stale_worker')
		  AND selected_release_id IS NOT NULL`,
		QueueRequested, QueueFailed,
	)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// ResetStaleQueueItems resets items stuck in transitional states.
// Active download states (fetching_nzb, indexing, publishing) use downloadStaleAfter
// because large files can take tens of minutes. The selected state uses
// selectedStaleAfter (20 min) because BullMQ workers may be busy. Idle
// search transitions (preflight, searching, ranking) use staleAfter (10 min).
func (db *DB) ResetStaleQueueItems(ctx context.Context, staleAfter, downloadStaleAfter, selectedStaleAfter time.Duration) (int, error) {
	now := time.Now()
	idleCutoff := now.Add(-staleAfter)
	downloadCutoff := now.Add(-downloadStaleAfter)
	selectedCutoff := now.Add(-selectedStaleAfter)
	result, err := db.SQL.ExecContext(ctx, `
		UPDATE queue_items SET
			state = $1,
			failure_reason = 'stale_worker',
			updated_at = now()
		WHERE (
			(state IN ($2, $3, $4) AND updated_at < $7)
			OR (state IN ($5, $6, $8) AND updated_at < $10)
			OR (state = $9 AND updated_at < $11)
		)`,
		QueueFailed,
		QueueFetchingNZB, QueueIndexing, QueuePublishing, // slow: download cutoff ($7)
		QueuePreflight, QueueSearching,                   // fast: idle cutoff ($10)
		downloadCutoff,
		QueueRanking,    // fast: idle cutoff ($10)
		QueueSelected,   // medium: selected cutoff ($11)
		idleCutoff,
		selectedCutoff,
	)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

func (db *DB) ListSabQueueItems(ctx context.Context, category string, start, limit int) ([]SabQueueItem, int, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT li.id, li.title, li.media_type, q.state
		FROM library_items li
		JOIN queue_items q ON q.library_item_id = li.id
		WHERE q.state NOT IN ('available', 'degraded', 'failed')
		  AND q.sab_dismissed = false
		  AND ($1 = ''
		       OR ($1 = 'movies' AND li.media_type = 'movie')
		       OR ($1 = 'tv' AND li.media_type IN ('tv', 'episode'))
		       OR ($1 NOT IN ('movies', 'tv')))
		ORDER BY q.created_at ASC
		LIMIT $2 OFFSET $3`, category, limit, start)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var items []SabQueueItem
	for rows.Next() {
		var it SabQueueItem
		if err := rows.Scan(&it.LibraryItemID, &it.Title, &it.MediaType, &it.State); err != nil {
			return nil, 0, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	var total int
	err = db.SQL.QueryRowContext(ctx, `
		SELECT count(*)
		FROM library_items li
		JOIN queue_items q ON q.library_item_id = li.id
		WHERE q.state NOT IN ('available', 'degraded', 'failed')
		  AND q.sab_dismissed = false
		  AND ($1 = ''
		       OR ($1 = 'movies' AND li.media_type = 'movie')
		       OR ($1 = 'tv' AND li.media_type IN ('tv', 'episode'))
		       OR ($1 NOT IN ('movies', 'tv')))`, category).Scan(&total)
	if err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (db *DB) ListSabHistoryItems(ctx context.Context, category string, start, limit int) ([]SabHistoryItem, int, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT li.id, li.title, li.media_type, q.state, q.failure_reason,
		       COALESCE(q.selected_release_id, 0),
		       COALESCE((
		           SELECT SUM(nf.file_size_bytes)
		           FROM nzb_documents nd
		           JOIN nzb_files nf ON nf.nzb_document_id = nd.id
		           WHERE nd.selected_release_id = q.selected_release_id
		       ), 0)
		FROM library_items li
		JOIN queue_items q ON q.library_item_id = li.id
		WHERE q.state IN ('available', 'degraded', 'failed')
		  AND q.sab_dismissed = false
		  AND ($1 = ''
		       OR ($1 = 'movies' AND li.media_type = 'movie')
		       OR ($1 = 'tv' AND li.media_type IN ('tv', 'episode'))
		       OR ($1 NOT IN ('movies', 'tv')))
		ORDER BY q.updated_at DESC
		LIMIT $2 OFFSET $3`, category, limit, start)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var items []SabHistoryItem
	for rows.Next() {
		var it SabHistoryItem
		if err := rows.Scan(&it.LibraryItemID, &it.Title, &it.MediaType, &it.State, &it.FailureReason, &it.SelectedReleaseID, &it.TotalBytes); err != nil {
			return nil, 0, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	var total int
	err = db.SQL.QueryRowContext(ctx, `
		SELECT count(*)
		FROM library_items li
		JOIN queue_items q ON q.library_item_id = li.id
		WHERE q.state IN ('available', 'degraded', 'failed')
		  AND q.sab_dismissed = false
		  AND ($1 = ''
		       OR ($1 = 'movies' AND li.media_type = 'movie')
		       OR ($1 = 'tv' AND li.media_type IN ('tv', 'episode'))
		       OR ($1 NOT IN ('movies', 'tv')))`, category).Scan(&total)
	if err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// DismissSabItems marks queue items as dismissed from the SABnzbd history/queue
// view. Called when Radarr/Sonarr sends mode=history&name=delete or
// mode=queue&name=delete. Dismissed items are excluded from future polls
// without altering queue state or triggering any workflow transitions.
func (db *DB) DismissSabItems(ctx context.Context, libraryItemIDs []int64) error {
	if len(libraryItemIDs) == 0 {
		return nil
	}
	// Build $1, $2, ... placeholders
	placeholders := make([]string, len(libraryItemIDs))
	args := make([]any, len(libraryItemIDs))
	for i, id := range libraryItemIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf(`
		UPDATE queue_items SET sab_dismissed = true
		WHERE library_item_id IN (%s)`, strings.Join(placeholders, ","))
	_, err := db.SQL.ExecContext(ctx, query, args...)
	return err
}

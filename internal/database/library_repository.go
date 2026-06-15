package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func (db *DB) ListLibraryItems(ctx context.Context) ([]LibraryItemSummary, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			li.id,
			li.media_type,
			li.title,
			li.available,
			li.requested_at,
			coalesce(q.state, ''),
			coalesce(q.failure_reason, ''),
			q.selected_release_id
		from library_items li
		left join queue_items q on q.library_item_id = li.id
		order by li.requested_at desc, li.id desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LibraryItemSummary
	for rows.Next() {
		var (
			item            LibraryItemSummary
			selectedRelease sql.NullInt64
			queueState      string
		)
		if err := rows.Scan(
			&item.ID,
			&item.MediaType,
			&item.Title,
			&item.Available,
			&item.RequestedAt,
			&queueState,
			&item.FailureReason,
			&selectedRelease,
		); err != nil {
			return nil, err
		}
		item.QueueState = QueueState(queueState)
		if selectedRelease.Valid {
			value := selectedRelease.Int64
			item.SelectedReleaseID = &value
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) ListReleaseSummaries(ctx context.Context, libraryItemID int64) ([]ReleaseSummary, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			sr.id,
			rc.id,
			rc.library_item_id,
			rc.title,
			rc.external_url,
			rc.indexer_name,
			rc.size_bytes,
			coalesce(rc.posted_at, to_timestamp(0)),
			rc.score,
			coalesce(rc.custom_format_score, 0),
			rc.selected,
			rc.rejected,
			rc.reject_reason,
			rc.failure_count,
			rc.last_failure_reason,
			rc.explanations,
			rc.compatibility_warnings,
			coalesce((select count(*) from archives a where a.selected_release_id = sr.id), 0),
			coalesce((select count(*) from archive_volumes av join archives a on a.id = av.archive_id where a.selected_release_id = sr.id), 0),
			coalesce((select string_agg(distinct a.status, ',' order by a.status) from archives a where a.selected_release_id = sr.id), ''),
			coalesce((select string_agg(distinct a.reject_reason, ',' order by a.reject_reason) from archives a where a.selected_release_id = sr.id and a.reject_reason <> ''), ''),
			coalesce((select count(*) from virtual_files vf where vf.selected_release_id = sr.id), 0),
			rc.created_at,
			n.id,
			coalesce(n.file_name, '')
		from release_candidates rc
		left join selected_releases sr on sr.release_candidate_id = rc.id
		left join nzb_documents n on n.selected_release_id = sr.id
		where rc.library_item_id = $1
		order by rc.selected desc, rc.score desc, rc.created_at asc, rc.id asc`, libraryItemID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ReleaseSummary
	for rows.Next() {
		var item ReleaseSummary
		var selectedRelease sql.NullInt64
		var nzbDocument sql.NullInt64
		if err := rows.Scan(
			&selectedRelease,
			&item.ReleaseCandidateID,
			&item.LibraryItemID,
			&item.Title,
			&item.ExternalURL,
			&item.IndexerName,
			&item.SizeBytes,
			&item.PostedAt,
			&item.Score,
			&item.CustomFormatScore,
			&item.Selected,
			&item.Rejected,
			&item.RejectReason,
			&item.FailureCount,
			&item.LastFailureReason,
			pgTextArrayScan(&item.Explanations),
			pgTextArrayScan(&item.CompatibilityWarnings),
			&item.ArchiveCount,
			&item.ArchiveVolumeCount,
			&item.ArchiveStatuses,
			&item.ArchiveRejects,
			&item.VirtualFileCount,
			&item.CreatedAt,
			&nzbDocument,
			&item.NZBFileName,
		); err != nil {
			return nil, err
		}
		if selectedRelease.Valid {
			item.SelectedReleaseID = selectedRelease.Int64
			archives, err := db.listReleaseArchives(ctx, item.SelectedReleaseID)
			if err != nil {
				return nil, err
			}
			item.Archives = archives
		}
		failedAttempts, err := db.listFailedReleaseAttempts(ctx, item.ReleaseCandidateID)
		if err != nil {
			return nil, err
		}
		item.FailedAttempts = failedAttempts
		if nzbDocument.Valid {
			value := nzbDocument.Int64
			item.NZBDocumentID = &value
		}
		item.Explanations = releaseSummaryExplanations(item)
		out = append(out, item)
	}
	return out, rows.Err()
}

func releaseSummaryExplanations(item ReleaseSummary) []string {
	out := append([]string{}, item.Explanations...)
	if item.Selected {
		out = append(out, "Currently selected for this library item.")
	}
	if item.Rejected && strings.TrimSpace(item.RejectReason) != "" {
		if strings.HasPrefix(item.RejectReason, "blocklist:") {
			out = append(out, "Rejected by a release filtering rule: "+strings.TrimPrefix(item.RejectReason, "blocklist:"))
		} else {
			out = append(out, "Rejected: "+item.RejectReason)
		}
	}
	if item.FailureCount > 0 {
		message := fmt.Sprintf("Previously failed %d time(s).", item.FailureCount)
		if strings.TrimSpace(item.LastFailureReason) != "" {
			message += " Latest failure: " + item.LastFailureReason + "."
		}
		out = append(out, message)
	}
	if strings.TrimSpace(item.ArchiveRejects) != "" {
		out = append(out, "Archive inspection rejected content: "+item.ArchiveRejects)
	}
	if len(out) == 0 && !item.Rejected && item.FailureCount == 0 && strings.TrimSpace(item.ArchiveRejects) == "" && !item.Selected {
		out = append(out, "No stored rejections or failed attempts for this candidate.")
	}
	return out
}

func (db *DB) listFailedReleaseAttempts(ctx context.Context, releaseCandidateID int64) ([]FailedReleaseAttempt, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select reason, created_at
		from failed_releases
		where release_candidate_id = $1
		order by created_at desc, id desc`, releaseCandidateID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FailedReleaseAttempt
	for rows.Next() {
		var item FailedReleaseAttempt
		if err := rows.Scan(&item.Reason, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) listReleaseArchives(ctx context.Context, selectedReleaseID int64) ([]ReleaseArchiveSummary, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			a.id,
			a.kind,
			a.status,
			a.reject_reason,
			coalesce((select count(*) from archive_volumes av where av.archive_id = a.id), 0)
		from archives a
		where a.selected_release_id = $1
		order by a.id asc`, selectedReleaseID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type archiveRow struct {
		id int64
		ReleaseArchiveSummary
	}
	var items []archiveRow
	for rows.Next() {
		var item archiveRow
		if err := rows.Scan(&item.id, &item.Kind, &item.Status, &item.RejectReason, &item.VolumeCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range items {
		entryRows, err := db.SQL.QueryContext(ctx, `
			select
				path,
				size_bytes,
				packed_size_bytes,
				compression_method,
				encrypted,
				solid,
				source_volume_index,
				source_archive_offset
			from archive_entries
			where archive_id = $1
			order by path asc`, items[i].id,
		)
		if err != nil {
			return nil, err
		}
		for entryRows.Next() {
			var entry ReleaseArchiveEntry
			if err := entryRows.Scan(
				&entry.Path,
				&entry.SizeBytes,
				&entry.PackedSizeBytes,
				&entry.CompressionMethod,
				&entry.Encrypted,
				&entry.Solid,
				&entry.SourceVolumeIndex,
				&entry.SourceArchiveOffset,
			); err != nil {
				entryRows.Close()
				return nil, err
			}
			items[i].Entries = append(items[i].Entries, entry)
		}
		if err := entryRows.Close(); err != nil {
			return nil, err
		}
	}

	out := make([]ReleaseArchiveSummary, 0, len(items))
	for _, item := range items {
		out = append(out, item.ReleaseArchiveSummary)
	}
	return out, nil
}

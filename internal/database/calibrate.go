package database

import (
	"context"
	"fmt"
	"log/slog"
)

// SegmentSizer can return the actual decoded byte size of an NNTP article.
type SegmentSizer interface {
	DecodedSize(ctx context.Context, messageID string) (int64, error)
}

// PreflightCheckFirstSegments verifies that the first AND last segment of every
// NZB file in the given document is reachable on NNTP. Unlike CalibrateNZBOffsets
// (which silently skips missing segments), this returns an error immediately so
// the workqueue can reject the release and fall back to the next candidate.
func (db *DB) PreflightCheckFirstSegments(ctx context.Context, nzbDocumentID int64) error {
	sizer, ok := db.SegmentFetcher.(SegmentSizer)
	if !ok || sizer == nil {
		return nil // NNTP fetcher not available; skip preflight
	}
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT
		    (SELECT ns.message_id FROM nzb_segments ns WHERE ns.nzb_file_id = nf.id ORDER BY ns.segment_number ASC  LIMIT 1),
		    (SELECT ns.message_id FROM nzb_segments ns WHERE ns.nzb_file_id = nf.id ORDER BY ns.segment_number DESC LIMIT 1)
		FROM nzb_files nf
		WHERE nf.nzb_document_id = $1`, nzbDocumentID)
	if err != nil {
		return err
	}
	defer rows.Close()
	type segPair struct{ first, last string }
	var pairs []segPair
	for rows.Next() {
		var p segPair
		if err := rows.Scan(&p.first, &p.last); err != nil {
			return err
		}
		pairs = append(pairs, p)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, p := range pairs {
		if _, err := sizer.DecodedSize(ctx, p.first); err != nil {
			return fmt.Errorf("preflight: first segment %s unavailable: %w", p.first, err)
		}
		if p.last != p.first {
			if _, err := sizer.DecodedSize(ctx, p.last); err != nil {
				return fmt.Errorf("preflight: last segment %s unavailable: %w", p.last, err)
			}
		}
	}
	return nil
}

// CalibrateAllNZBOffsets runs CalibrateNZBOffsets for every NZB document in the
// database that has uncalibrated files. Called once at startup to fix any NZBs
// imported with the old estimated offset factor.
func (db *DB) CalibrateAllNZBOffsets(ctx context.Context) error {
	// Only select documents that have at least one uncalibrated file.
	// calibrated_at is set after a successful rescale, so NULL means uncalibrated.
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT DISTINCT nzb_document_id FROM nzb_files WHERE calibrated_at IS NULL`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := db.CalibrateNZBOffsets(ctx, id); err != nil {
			slog.Warn("calibrate all: failed for document", "nzb_document_id", id, "err", err)
		}
	}
	return nil
}

// CalibrateNZBOffsets corrects segment decoded offsets for all files in an NZB
// document by fetching the first segment of each file and measuring its actual
// decoded size. This replaces the estimated offsets (0.74 or 0.97 factor) with
// values derived from the real yEnc payload size.
func (db *DB) CalibrateNZBOffsets(ctx context.Context, nzbDocumentID int64) error {
	sizer, ok := db.SegmentFetcher.(SegmentSizer)
	if !ok || sizer == nil {
		return nil
	}

	rows, err := db.SQL.QueryContext(ctx, `
		SELECT nf.id,
		       (SELECT ns.message_id FROM nzb_segments ns WHERE ns.nzb_file_id = nf.id ORDER BY ns.segment_number ASC  LIMIT 1),
		       (SELECT ns.decoded_end_offset - ns.decoded_start_offset FROM nzb_segments ns WHERE ns.nzb_file_id = nf.id ORDER BY ns.segment_number ASC  LIMIT 1),
		       (SELECT ns.message_id FROM nzb_segments ns WHERE ns.nzb_file_id = nf.id ORDER BY ns.segment_number DESC LIMIT 1),
		       (SELECT ns.decoded_end_offset - ns.decoded_start_offset FROM nzb_segments ns WHERE ns.nzb_file_id = nf.id ORDER BY ns.segment_number DESC LIMIT 1)
		FROM nzb_files nf
		WHERE nf.nzb_document_id = $1
		  AND nf.calibrated_at IS NULL`, nzbDocumentID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type fileInfo struct {
		id          int64
		firstMsgID  string
		estFirstSize int64
		lastMsgID   string
		estLastSize  int64
	}
	var files []fileInfo
	for rows.Next() {
		var f fileInfo
		if err := rows.Scan(&f.id, &f.firstMsgID, &f.estFirstSize, &f.lastMsgID, &f.estLastSize); err != nil {
			return err
		}
		if f.firstMsgID != "" && f.estFirstSize > 0 {
			files = append(files, f)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, f := range files {
		actualFirst, err := sizer.DecodedSize(ctx, f.firstMsgID)
		if err != nil {
			slog.Warn("calibrate: could not fetch first segment — marking as skipped", "nzb_file_id", f.id, "err", err)
			// Mark calibrated so expired/missing articles are not retried on every startup.
			_, _ = db.SQL.ExecContext(ctx, `UPDATE nzb_files SET calibrated_at = now() WHERE id = $1`, f.id)
			continue
		}
		if actualFirst <= 0 {
			continue
		}
		// Fetch the last segment to get the exact total file size — the same
		// approach nzbdav uses (GetFileSizeAsync fetches the last segment's yEnc
		// header). This avoids the compounding estimation error for the last segment
		// that makes the total file size too large, which causes Plex to seek to
		// positions beyond the real end of file.
		actualLast := actualFirst // default: same size as other segments
		if f.lastMsgID != "" && f.lastMsgID != f.firstMsgID {
			if n, err := sizer.DecodedSize(ctx, f.lastMsgID); err == nil && n > 0 {
				actualLast = n
			} else if err != nil {
				slog.Warn("calibrate: could not fetch last segment", "nzb_file_id", f.id, "err", err)
			}
		}
		if err := db.rescaleFileSegments(ctx, f.id, actualFirst, actualLast); err != nil {
			return fmt.Errorf("rescale nzb_file %d: %w", f.id, err)
		}
		slog.Info("calibrate: corrected segment offsets",
			"nzb_file_id", f.id,
			"estimated_first", f.estFirstSize,
			"actual_first", actualFirst,
			"actual_last", actualLast)
	}
	return nil
}

// rescaleFileSegments rewrites decoded byte offsets for all segments of a file.
// actualFirstSize is the measured decoded size of segment 1 (applied uniformly
// to all non-last segments). actualLastSize is the measured decoded size of the
// final segment — using the real value avoids the file-size overestimation that
// causes Plex to seek past the real end of file (mirrors nzbdav's behaviour of
// fetching the last segment's yEnc header for an exact total size).
func (db *DB) rescaleFileSegments(ctx context.Context, nzbFileID, actualFirstSize, actualLastSize int64) error {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Rewrite nzb_segments: all non-last segments get uniform actualFirstSize
	// boundaries; the last segment uses its own measured actualLastSize.
	_, err = tx.ExecContext(ctx, `
		WITH numbered AS (
		    SELECT id, segment_number,
		           ROW_NUMBER() OVER (ORDER BY segment_number) - 1 AS idx,
		           COUNT(*) OVER () AS total
		    FROM nzb_segments
		    WHERE nzb_file_id = $1
		)
		UPDATE nzb_segments ns SET
		    decoded_start_offset = n.idx * $2,
		    decoded_end_offset   = CASE
		        WHEN n.idx = n.total - 1 THEN n.idx * $2 + $3
		        ELSE (n.idx + 1) * $2
		    END
		FROM numbered n
		WHERE ns.id = n.id`,
		nzbFileID, actualFirstSize, actualLastSize)
	if err != nil {
		return err
	}

	// Rewrite virtual_file_ranges with the same uniform boundaries.
	_, err = tx.ExecContext(ctx, `
		WITH numbered AS (
		    SELECT ns.id as seg_id,
		           ROW_NUMBER() OVER (PARTITION BY ns.nzb_file_id ORDER BY ns.segment_number) - 1 AS idx,
		           COUNT(*) OVER (PARTITION BY ns.nzb_file_id) AS total
		    FROM nzb_segments ns
		    WHERE ns.nzb_file_id = $1
		)
		UPDATE virtual_file_ranges vfr SET
		    range_start = n.idx * $2,
		    range_end   = CASE
		        WHEN n.idx = n.total - 1 THEN n.idx * $2 + $3
		        ELSE (n.idx + 1) * $2
		    END
		FROM numbered n
		WHERE vfr.nzb_segment_id = n.seg_id`,
		nzbFileID, actualFirstSize, actualLastSize)
	if err != nil {
		return err
	}

	// Sync virtual_files.size_bytes to the corrected max range_end.
	_, err = tx.ExecContext(ctx, `
		UPDATE virtual_files vf SET
		    size_bytes = (
		        SELECT COALESCE(MAX(vfr.range_end), 0)
		        FROM virtual_file_ranges vfr
		        WHERE vfr.virtual_file_id = vf.id
		    )
		WHERE id IN (
		    SELECT DISTINCT vfr.virtual_file_id
		    FROM virtual_file_ranges vfr
		    JOIN nzb_segments ns ON ns.id = vfr.nzb_segment_id
		    WHERE ns.nzb_file_id = $1
		)`, nzbFileID)
	if err != nil {
		return err
	}

	// Mark this file as calibrated so future startup passes can skip it.
	if _, err = tx.ExecContext(ctx, `UPDATE nzb_files SET calibrated_at = now() WHERE id = $1`, nzbFileID); err != nil {
		return err
	}

	return tx.Commit()
}

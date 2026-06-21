package database

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
)

// SegmentSizer can return the actual decoded byte size of an NNTP article.
type SegmentSizer interface {
	DecodedSize(ctx context.Context, messageID string) (int64, error)
}

type SegmentChecker interface {
	Exists(ctx context.Context, messageID string) error
}

type segmentPair struct{ first, last string }

func (db *DB) loadNZBFirstLastSegmentPairs(ctx context.Context, nzbDocumentID int64) ([]segmentPair, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT
		    nf.subject,
		    nf.message_ids[1],
		    nf.message_ids[array_length(nf.message_ids, 1)]
		FROM nzb_files nf
		WHERE nf.nzb_document_id = $1
		  AND array_length(nf.message_ids, 1) > 0`, nzbDocumentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pairs []segmentPair
	for rows.Next() {
		var subject string
		var p segmentPair
		if err := rows.Scan(&subject, &p.first, &p.last); err != nil {
			return nil, err
		}
		if !shouldValidateNZBSubject(subject) {
			continue
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

func shouldValidateNZBSubject(subject string) bool {
	name := parseNZBSubjectFilename(subject)
	if name == "" {
		name = subject
	}
	base := strings.ToLower(filepath.Base(strings.TrimSpace(name)))
	if base == "" {
		return false
	}
	switch ext := strings.ToLower(filepath.Ext(base)); ext {
	case ".par2", ".sfv", ".nfo", ".jpg", ".jpeg", ".png":
		return false
	}
	if isSampleFilename(base) {
		return false
	}
	return true
}

func parseNZBSubjectFilename(subject string) string {
	start := strings.Index(subject, "\"")
	end := strings.LastIndex(subject, "\"")
	if start >= 0 && end > start {
		return subject[start+1 : end]
	}
	fields := strings.Fields(subject)
	if len(fields) == 0 {
		return ""
	}
	return strings.Trim(fields[0], "\"")
}

// PreflightCheckFirstSegments verifies that the first AND last segment of every
// NZB file in the given document is reachable on NNTP. Unlike CalibrateNZBOffsets
// (which silently skips missing segments), this returns an error immediately so
// the workqueue can reject the release and fall back to the next candidate.
func (db *DB) PreflightCheckFirstSegments(ctx context.Context, nzbDocumentID int64) error {
	checker, ok := db.SegmentFetcher.(SegmentChecker)
	if !ok || checker == nil {
		return nil // NNTP fetcher not available; skip preflight
	}
	pairs, err := db.loadNZBFirstLastSegmentPairs(ctx, nzbDocumentID)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}
	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	const maxConcurrentChecks = 8
	sem := make(chan struct{}, maxConcurrentChecks)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for _, pair := range pairs {
		pair := pair
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-checkCtx.Done():
				return
			}
			defer func() { <-sem }()
			if err := checker.Exists(checkCtx, pair.first); err != nil {
				errOnce.Do(func() {
					firstErr = sanitizedSegmentErr("preflight", "first", err)
					cancel()
				})
				return
			}
			if pair.last != pair.first {
				if err := checker.Exists(checkCtx, pair.last); err != nil {
					errOnce.Do(func() {
						firstErr = sanitizedSegmentErr("preflight", "last", err)
						cancel()
					})
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// StrictCheckFirstSegments uses the older heavier validation strategy:
// download enough decoded article data to measure first/last segment sizes for
// every file, failing as soon as any required segment is unavailable.
func (db *DB) StrictCheckFirstSegments(ctx context.Context, nzbDocumentID int64) error {
	sizer, ok := db.SegmentFetcher.(SegmentSizer)
	if !ok || sizer == nil {
		return nil
	}
	pairs, err := db.loadNZBFirstLastSegmentPairs(ctx, nzbDocumentID)
	if err != nil {
		return err
	}
	for _, p := range pairs {
		if _, err := sizer.DecodedSize(ctx, p.first); err != nil {
			return sanitizedSegmentErr("strict health", "first", err)
		}
		if p.last != p.first {
			if _, err := sizer.DecodedSize(ctx, p.last); err != nil {
				return sanitizedSegmentErr("strict health", "last", err)
			}
		}
	}
	return nil
}

// sanitizedSegmentErr builds a blocklist-safe error for a failed segment check.
// It omits the raw message ID (which would create one unique reason per segment)
// and strips trailing message IDs from the wrapped error text.
func sanitizedSegmentErr(kind, pos string, err error) error {
	msg := err.Error()
	// Strip trailing bare message ID: "... (cached): msgid@host" → "... (cached)"
	if i := strings.LastIndex(msg, ": "); i > 0 {
		suffix := msg[i+2:]
		if strings.ContainsRune(suffix, '@') && !strings.ContainsRune(suffix, ' ') {
			msg = msg[:i]
		}
	}
	return fmt.Errorf("%s: %s segment unavailable: %s", kind, pos, msg)
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
		       nf.message_ids[1],
		       nf.decoded_segment_size,
		       nf.message_ids[array_length(nf.message_ids, 1)],
		       nf.last_decoded_size
		FROM nzb_files nf
		WHERE nf.nzb_document_id = $1
		  AND nf.calibrated_at IS NULL
		  AND array_length(nf.message_ids, 1) > 0`, nzbDocumentID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type fileInfo struct {
		id           int64
		firstMsgID   string
		estFirstSize int64
		lastMsgID    string
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

// rescaleFileSegments updates decoded segment sizes for a file inline in nzb_files
// and recomputes virtual_files.size_bytes for any direct_nzb virtual file backed
// by this nzb_file.  The old nzb_segments / virtual_file_ranges tables were
// removed by migration 000041; all segment data now lives in nzb_files.
//
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

	// Update the inline segment sizes stored in nzb_files.
	var segmentCount int64
	if err = tx.QueryRowContext(ctx, `
		UPDATE nzb_files
		SET decoded_segment_size = $2,
		    last_decoded_size     = $3
		WHERE id = $1
		RETURNING array_length(message_ids, 1)`,
		nzbFileID, actualFirstSize, actualLastSize,
	).Scan(&segmentCount); err != nil {
		return err
	}

	// Recompute virtual_files.size_bytes for direct_nzb entries backed by this
	// nzb_file.  The total decoded size is:
	//   (segmentCount - 1) * actualFirstSize + actualLastSize
	// This exactly mirrors what computeSpans produces.
	if segmentCount > 0 {
		totalSize := (segmentCount-1)*actualFirstSize + actualLastSize
		if _, err = tx.ExecContext(ctx, `
			UPDATE virtual_files
			SET size_bytes = $2
			WHERE nzb_file_id = $1
			  AND reader_kind = 'direct_nzb'`,
			nzbFileID, totalSize,
		); err != nil {
			return err
		}
	}

	// Mark this file as calibrated so future startup passes can skip it.
	if _, err = tx.ExecContext(ctx, `UPDATE nzb_files SET calibrated_at = now() WHERE id = $1`, nzbFileID); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	// Flush the in-memory VF cache so the next open picks up the new sizes.
	db.InvalidateVFCacheForNZBFile(nzbFileID)
	return nil
}

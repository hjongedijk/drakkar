package app

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/hjongedijk/drakkar/internal/database"
	"github.com/hjongedijk/drakkar/internal/library"
	"github.com/hjongedijk/drakkar/internal/maintenance"
	"github.com/hjongedijk/drakkar/internal/workflow"
	"github.com/rs/zerolog"
)

type maintenanceOpsService struct {
	base           *maintenance.Service
	db             *database.DB
	workflowSvc    *workflow.Service
	publicationSvc *library.Publisher
	logger         zerolog.Logger
}

func (s *maintenanceOpsService) RemoveBrokenMediaSymlinks(ctx context.Context) (maintenance.Result, error) {
	return s.base.RemoveBrokenMediaSymlinks(ctx)
}

func (s *maintenanceOpsService) RemoveOrphanedCompletedSymlinks(ctx context.Context) (maintenance.Result, error) {
	return s.base.RemoveOrphanedCompletedSymlinks(ctx)
}

func (s *maintenanceOpsService) RemoveOrphanedContent(ctx context.Context) (maintenance.Result, error) {
	return s.base.RemoveOrphanedContent(ctx)
}

func (s *maintenanceOpsService) DeepNZBHealthCheck(ctx context.Context) (maintenance.Result, error) {
	return runNZBHealthCheck(ctx, s.db, s.workflowSvc, s.publicationSvc, s.logger)
}

func nextDeepHealthCheckDelay(createdAt time.Time) time.Duration {
	age := time.Since(createdAt)
	if age < time.Hour {
		return time.Hour
	}
	if age > 30*24*time.Hour {
		return 30 * 24 * time.Hour
	}
	return age
}

func shouldRunDeepHealthCheck(now time.Time, item database.DeepHealthCandidate) bool {
	if item.LastCheckedAt == nil || item.HealthOK == nil {
		return true
	}
	if !*item.HealthOK {
		return true
	}
	return now.Sub(*item.LastCheckedAt) >= nextDeepHealthCheckDelay(item.CreatedAt)
}

func runNZBHealthCheckBatch(ctx context.Context, db *database.DB, workflowSvc *workflow.Service, publicationSvc *library.Publisher, logger zerolog.Logger, limit int, force bool) (maintenance.Result, error) {
	result := maintenance.Result{TaskName: "nzb-health-check"}
	candidates, err := db.ListDeepHealthCandidates(ctx, limit)
	if err != nil {
		logger.Error().Err(err).Msg("health check: query failed")
		return result, err
	}
	now := time.Now()
	logger.Info().Int("count", len(candidates)).Bool("force", force).Msg("health check: scanning deep-check candidates")
	resetSeen := make(map[int64]struct{})
	repairedSeen := make(map[int64]struct{})
	for _, c := range candidates {
		if ctx.Err() != nil {
			break
		}
		result.ScannedRows++
		symlinkOK := database.CheckSymlinkHealth(c.LibraryPath, c.TargetPath)
		_ = db.RecordHealthCheck(ctx, c.PublicationID, symlinkOK)
		if !symlinkOK {
			if publicationSvc != nil {
				logger.Warn().
					Int64("libraryItemId", c.LibraryItemID).
					Str("libraryPath", c.LibraryPath).
					Msg("health check: broken symlink publication — re-publishing item")
				if err := publicationSvc.RepublishLibraryItem(ctx, c.LibraryItemID); err != nil {
					logger.Error().Err(err).Int64("libraryItemId", c.LibraryItemID).Msg("health check: republish failed")
				} else {
					symlinkOK = database.CheckSymlinkHealth(c.LibraryPath, c.TargetPath)
					_ = db.RecordHealthCheck(ctx, c.PublicationID, symlinkOK)
					if symlinkOK {
						if _, exists := repairedSeen[c.LibraryItemID]; !exists {
							repairedSeen[c.LibraryItemID] = struct{}{}
							result.RepairedItems++
						}
					}
				}
			}
			if !symlinkOK && !force {
				continue
			}
		}
		if !strings.Contains(c.TargetPath, "/content/") {
			_ = db.RecordHealthCheck(ctx, c.PublicationID, true)
			continue
		}
		if !force && !shouldRunDeepHealthCheck(now, c) {
			continue
		}
		if c.NZBDocumentID <= 0 {
			continue
		}
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		err := db.StrictCheckFirstSegments(checkCtx, c.NZBDocumentID)
		cancel()
		if err == nil {
			_ = db.RecordHealthCheck(ctx, c.PublicationID, true)
			continue
		}
		logger.Warn().
			Int64("libraryItemId", c.LibraryItemID).
			Str("title", c.Title).
			Err(err).
			Msg("health check: strict NZB validation failed — blocklisting release and promoting next")
		_ = db.RecordHealthCheck(ctx, c.PublicationID, false)
		// Remove symlinks before blocklisting so the filesystem is clean
		// regardless of whether a next candidate exists.
		paths, pathErr := db.DeleteSymlinkPublicationsForLibraryItem(ctx, c.LibraryItemID)
		if pathErr == nil {
			for _, p := range paths {
				if removeErr := os.Remove(p); removeErr != nil && !os.IsNotExist(removeErr) {
					logger.Warn().Str("path", p).Err(removeErr).Msg("health check: could not remove symlink")
				}
			}
		}
		blocklistErr := workflowSvc.FailAndBlocklistRelease(ctx, c.SelectedReleaseID, "strict health: "+err.Error())
		if blocklistErr != nil {
			logger.Error().Err(blocklistErr).Int64("libraryItemId", c.LibraryItemID).Msg("health check: blocklist failed")
		} else if _, exists := resetSeen[c.LibraryItemID]; !exists {
			resetSeen[c.LibraryItemID] = struct{}{}
			result.ResetItems++
		}
	}
	return result, nil
}

// runNZBHealthCheck repairs bad symlinks by re-publishing them, performs decoded-
// segment validation on available items, resets broken releases for re-queue,
// and resets sample-only publications.
func runNZBHealthCheck(ctx context.Context, db *database.DB, workflowSvc *workflow.Service, publicationSvc *library.Publisher, logger zerolog.Logger) (maintenance.Result, error) {
	result, err := runNZBHealthCheckBatch(ctx, db, workflowSvc, publicationSvc, logger, 0, true)
	if err != nil {
		return result, err
	}
	resetSeen := make(map[int64]struct{})

	sampleRows, err := db.SQL.QueryContext(ctx, `
		SELECT DISTINCT qi.library_item_id, li.title
		FROM queue_items qi
		JOIN library_items li ON li.id = qi.library_item_id
		JOIN selected_releases sr ON sr.id = qi.selected_release_id
		JOIN virtual_files vf ON vf.selected_release_id = sr.id
		WHERE qi.state = 'available' AND li.available = true
		  AND lower(vf.file_name) ~ '^(sample|sample[-_].+|.+[-_]sample)\.(mkv|mp4|avi)$'
		  AND NOT EXISTS (
		      SELECT 1 FROM virtual_files vf2
		      WHERE vf2.selected_release_id = sr.id
		        AND lower(vf2.file_name) !~ '^(sample|sample[-_].+|.+[-_]sample)\.(mkv|mp4|avi)$'
		  )`)
	if err == nil {
		defer sampleRows.Close()
		for sampleRows.Next() {
			var libID int64
			var title string
			if err := sampleRows.Scan(&libID, &title); err != nil {
				continue
			}
			if _, exists := resetSeen[libID]; exists {
				continue
			}
			logger.Warn().Int64("libraryItemId", libID).Str("title", title).
				Msg("health check: only sample file published — resetting item for re-queue")
			if resetErr := workflowSvc.ResetLibraryItem(ctx, libID); resetErr != nil {
				logger.Error().Err(resetErr).Int64("libraryItemId", libID).Msg("health check: sample reset failed")
			} else {
				resetSeen[libID] = struct{}{}
				result.ResetItems++
			}
		}
	}

	if err := db.TouchMaintenanceCursor(ctx, taskNZBHealthCheck, time.Now().UTC().Format(time.RFC3339)); err != nil {
		return result, err
	}
	if result.ResetItems > 0 {
		logger.Info().Int("reset", result.ResetItems).Msg("health check: reset broken items for re-queue")
	}
	return result, nil
}

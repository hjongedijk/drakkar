package database

import (
	"context"
	"database/sql"
	"regexp"
	"strconv"
	"strings"
)

// episodePattern extracts SxxExx, sxex, or x×xx from a filename.
var episodePattern = regexp.MustCompile(`(?i)[^a-z]s(\d{1,2})e(\d{1,3})[^0-9]`)

// ParseEpisodeFromFilename returns (season, episode) or (0, 0) if not found.
func ParseEpisodeFromFilename(name string) (season, episode int) {
	// Pad both sides to ensure the non-alpha boundary matches.
	m := episodePattern.FindStringSubmatch(" " + strings.ToLower(name) + " ")
	if m == nil {
		return 0, 0
	}
	s, _ := strconv.Atoi(m[1])
	e, _ := strconv.Atoi(m[2])
	return s, e
}

// SeasonPackEpisodeMatch pairs a virtual file path with a library item.
type SeasonPackEpisodeMatch struct {
	VirtualFileID   int64
	VirtualFilePath string
	FileName        string
	LibraryItemID   int64
	SeasonNumber    int
	EpisodeNumber   int
}

// FindSeasonPackMatches looks up library items matching the episode numbers
// encoded in the virtual file filenames for a given selected release.
// It returns one match per (season, episode) pair, preferring library items
// for the same TV show as the triggering library item.
func (db *DB) FindSeasonPackMatches(ctx context.Context, selectedReleaseID, triggeringLibraryItemID int64) ([]SeasonPackEpisodeMatch, error) {
	tvShowID, _, err := db.resolveSeasonPackShow(ctx, triggeringLibraryItemID)
	if err != nil || tvShowID == 0 {
		return nil, nil
	}

	// Get all virtual files for this release that we haven't matched yet.
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT vf.id, vf.path, vf.file_name
		FROM virtual_files vf
		WHERE vf.selected_release_id = $1`, selectedReleaseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type vfRow struct {
		id, path, name string
		intID          int64
	}
	var vfs []vfRow
	for rows.Next() {
		var r vfRow
		if err := rows.Scan(&r.intID, &r.path, &r.name); err != nil {
			return nil, err
		}
		vfs = append(vfs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var matches []SeasonPackEpisodeMatch
	seen := map[[2]int]bool{}

	for _, vf := range vfs {
		season, episode := ParseEpisodeFromFilename(vf.name)
		if season <= 0 || episode <= 0 {
			continue
		}
		key := [2]int{season, episode}
		if seen[key] {
			continue
		}

		// Find the library item for this episode.
		var libraryItemID int64
		err := db.SQL.QueryRowContext(ctx, `
			SELECT li.id
			FROM library_items li
			JOIN episodes e ON e.id = li.episode_id
			WHERE e.tv_show_id = $1
			  AND e.season_number = $2
			  AND e.episode_number = $3
			LIMIT 1`, tvShowID, season, episode).Scan(&libraryItemID)
		if err != nil {
			continue // no matching un-fulfilled library item
		}
		seen[key] = true
		matches = append(matches, SeasonPackEpisodeMatch{
			VirtualFileID:   vf.intID,
			VirtualFilePath: vf.path,
			FileName:        vf.name,
			LibraryItemID:   libraryItemID,
			SeasonNumber:    season,
			EpisodeNumber:   episode,
		})
	}
	return matches, nil
}

// CreateSeasonPackEpisodeItems is called when a season pack is published for a
// whole-show library item (season=0, episode=0). It parses each virtual file
// in the release, creates episode + library_item records for any SxxExx it
// finds, and marks them available. This turns one whole-show library item into
// many per-episode items so the library reflects actual episode availability.
func (db *DB) CreateSeasonPackEpisodeItems(ctx context.Context, selectedReleaseID, triggeringLibraryItemID int64) error {
	tvShowID, showTitle, err := db.resolveSeasonPackShow(ctx, triggeringLibraryItemID)
	if err != nil || tvShowID == 0 {
		return nil
	}

	// Collect virtual files for this release.
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT id, file_name FROM virtual_files WHERE selected_release_id = $1`, selectedReleaseID)
	if err != nil {
		return err
	}
	defer rows.Close()
	type vf struct {
		id   int64
		name string
	}
	var files []vf
	for rows.Next() {
		var f vf
		if err := rows.Scan(&f.id, &f.name); err != nil {
			return err
		}
		files = append(files, f)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Also get the release_candidate_id to link the new selected_releases.
	var releaseCandidateID int64
	err = db.SQL.QueryRowContext(ctx, `
		SELECT release_candidate_id FROM selected_releases WHERE id = $1`, selectedReleaseID).Scan(&releaseCandidateID)
	if err != nil {
		return err
	}

	seen := map[[2]int]bool{}
	for _, f := range files {
		season, episode := ParseEpisodeFromFilename(f.name)
		if season <= 0 || episode <= 0 {
			continue
		}
		key := [2]int{season, episode}
		if seen[key] {
			continue
		}
		seen[key] = true

		// Upsert the episode record.
		var episodeID int64
		err = db.SQL.QueryRowContext(ctx, `
			INSERT INTO episodes (tv_show_id, season_number, episode_number, title)
			VALUES ($1, $2, $3, '')
			ON CONFLICT (tv_show_id, season_number, episode_number) DO UPDATE
			  SET tv_show_id = excluded.tv_show_id
			RETURNING id`, tvShowID, season, episode).Scan(&episodeID)
		if err != nil {
			continue
		}

		// Upsert the library_item for this episode (unique on episode_id).
		var libItemID int64
		err = db.SQL.QueryRowContext(ctx, `
			INSERT INTO library_items (media_type, episode_id, title, available)
			VALUES ('episode', $1, $2, true)
			ON CONFLICT (episode_id) WHERE episode_id IS NOT NULL DO UPDATE
			  SET available = true
			RETURNING id`, episodeID, showTitle).Scan(&libItemID)
		if err != nil || libItemID == 0 {
			// May already exist — find and update.
			_ = db.SQL.QueryRowContext(ctx, `
				SELECT id FROM library_items WHERE episode_id = $1`, episodeID).Scan(&libItemID)
			if libItemID > 0 {
				_, _ = db.SQL.ExecContext(ctx, `
					UPDATE library_items SET available = true WHERE id = $1`, libItemID)
			}
			if libItemID == 0 {
				continue
			}
		}

		// Link a selected_release so the episode is associated with the NZB release.
		var srID int64
		_ = db.SQL.QueryRowContext(ctx, `
			INSERT INTO selected_releases (release_candidate_id, library_item_id)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
			RETURNING id`, releaseCandidateID, libItemID).Scan(&srID)
		if srID == 0 {
			_ = db.SQL.QueryRowContext(ctx, `
				SELECT id FROM selected_releases WHERE library_item_id = $1`, libItemID).Scan(&srID)
		}

		// Create a queue_item so the monitoring system knows this episode is done.
		ikey := strings.ToLower(showTitle) + "-pack-" + strconv.Itoa(season) + "-" + strconv.Itoa(episode)
		_, _ = db.SQL.ExecContext(ctx, `
			INSERT INTO queue_items
			    (library_item_id, selected_release_id, state, idempotency_key, updated_at)
			VALUES ($1, $2, 'available', $3, now())
			ON CONFLICT (library_item_id) DO UPDATE
			  SET state = 'available', selected_release_id = $2, updated_at = now()`,
			libItemID, srID, ikey)
	}
	return nil
}

func (db *DB) resolveSeasonPackShow(ctx context.Context, triggeringLibraryItemID int64) (int64, string, error) {
	var (
		tvShowID  int64
		showTitle string
	)
	err := db.SQL.QueryRowContext(ctx, `
		SELECT
			coalesce(
				e.tv_show_id,
				(
					SELECT tv.id
					FROM tv_shows tv
					WHERE lower(tv.title) = lower(li.title)
					ORDER BY tv.id ASC
					LIMIT 1
				),
				0
			),
			coalesce(nullif(tv.title, ''), nullif(li.title, ''), '')
		FROM library_items li
		LEFT JOIN episodes e ON e.id = li.episode_id
		LEFT JOIN tv_shows tv ON tv.id = e.tv_show_id
		WHERE li.id = $1`, triggeringLibraryItemID).Scan(&tvShowID, &showTitle)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, "", nil
		}
		return 0, "", err
	}
	return tvShowID, showTitle, nil
}

// FulfillEpisodeLibraryItem creates a selected_release + queue_item for an
// episode library item that is being fulfilled by a season pack virtual file,
// then marks it as available.
func (db *DB) FulfillEpisodeLibraryItem(ctx context.Context, libraryItemID, sourceSelectedReleaseID, virtualFileID int64) error {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Re-use the same release candidate as the triggering item so all episodes
	// share the same NZB document and provenance.
	var releaseCandidateID int64
	err = tx.QueryRowContext(ctx, `
		SELECT release_candidate_id FROM selected_releases WHERE id = $1`,
		sourceSelectedReleaseID).Scan(&releaseCandidateID)
	if err != nil {
		return err
	}

	// Insert a new selected_release row for this episode.
	var newSelectedReleaseID int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO selected_releases (release_candidate_id, library_item_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING
		RETURNING id`, releaseCandidateID, libraryItemID).Scan(&newSelectedReleaseID)
	if err != nil || newSelectedReleaseID == 0 {
		// Already selected — just mark available.
		_ = tx.Commit()
		return db.markLibraryItemAvailable(ctx, libraryItemID)
	}

	// Update the virtual file to also reference this selected release.
	// We do NOT duplicate the virtual file — we just update the queue item.
	_, err = tx.ExecContext(ctx, `
		UPDATE queue_items SET
			state = 'available',
			selected_release_id = $2,
			updated_at = now()
		WHERE library_item_id = $1 AND state != 'available'`,
		libraryItemID, newSelectedReleaseID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE library_items SET available = true WHERE id = $1`, libraryItemID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (db *DB) markLibraryItemAvailable(ctx context.Context, libraryItemID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		UPDATE library_items SET available = true WHERE id = $1`, libraryItemID)
	if err != nil {
		return err
	}
	_, err = db.SQL.ExecContext(ctx, `
		UPDATE queue_items SET state = 'available', updated_at = now()
		WHERE library_item_id = $1 AND state != 'available'`, libraryItemID)
	return err
}

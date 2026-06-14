package database

import (
	"context"
	"path/filepath"
)

func (db *DB) ListVirtualFilesForRelease(ctx context.Context, selectedReleaseID int64) ([]ReleaseVirtualFile, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			vf.id,
			vf.selected_release_id,
			sr.library_item_id,
			li.media_type,
			vf.path,
			vf.file_name,
			coalesce(m.title, ''),
			coalesce(m.release_year, 0),
			coalesce(m.tmdb_id, 0),
			coalesce(tv.title, ''),
			coalesce(tv.release_year, 0),
			coalesce(tv.tvdb_id, 0),
			coalesce(e.season_number, 0),
			coalesce(e.episode_number, 0)
		from virtual_files vf
		join selected_releases sr on sr.id = vf.selected_release_id
		join library_items li on li.id = sr.library_item_id
		left join movies m on m.id = li.movie_id
		left join episodes e on e.id = li.episode_id
		left join tv_shows tv on tv.id = e.tv_show_id
		where vf.selected_release_id = $1
		order by vf.path asc`, selectedReleaseID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ReleaseVirtualFile
	for rows.Next() {
		var item ReleaseVirtualFile
		if err := rows.Scan(
			&item.VirtualFileID,
			&item.SelectedReleaseID,
			&item.LibraryItemID,
			&item.MediaType,
			&item.Path,
			&item.FileName,
			&item.MovieTitle,
			&item.MovieYear,
			&item.MovieTMDBID,
			&item.ShowTitle,
			&item.ShowYear,
			&item.ShowTVDBID,
			&item.SeasonNumber,
			&item.EpisodeNumber,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// DeleteSymlinkPublicationsForLibraryItem removes all symlink_publications rows
// for the given library item and returns their library_path values so the caller
// can delete the corresponding filesystem symlinks.
func (db *DB) DeleteSymlinkPublicationsForLibraryItem(ctx context.Context, libraryItemID int64) ([]string, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		DELETE FROM symlink_publications
		WHERE library_item_id = $1
		RETURNING library_path`, libraryItemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

func (db *DB) UpsertSymlinkPublication(ctx context.Context, libraryItemID, virtualFileID int64, libraryPath, targetPath string) error {
	_, err := db.SQL.ExecContext(ctx, `
		insert into symlink_publications (library_item_id, virtual_file_id, library_path, target_path)
		values ($1, $2, $3, $4)
		on conflict (library_path)
		do update set
			virtual_file_id = excluded.virtual_file_id,
			target_path = excluded.target_path`,
		libraryItemID, virtualFileID, libraryPath, targetPath,
	)
	return err
}

func (db *DB) MarkReleaseAvailable(ctx context.Context, selectedReleaseID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, updated_at = now()
		where selected_release_id = $1`, selectedReleaseID, QueueAvailable,
	)
	if err != nil {
		return err
	}
	_, err = db.SQL.ExecContext(ctx, `
		update library_items
		set available = true
		where id in (
			select library_item_id from selected_releases where id = $1
		)`, selectedReleaseID,
	)
	return err
}

func (db *DB) ListCompletedSymlinkEntries(ctx context.Context) ([]CompletedSymlinkEntry, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select id, library_path, target_path
		from symlink_publications
		order by id asc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CompletedSymlinkEntry
	for rows.Next() {
		var item CompletedSymlinkEntry
		var libraryPath string
		if err := rows.Scan(&item.PublicationID, &libraryPath, &item.TargetPath); err != nil {
			return nil, err
		}
		item.Name = filepath.Base(libraryPath)
		out = append(out, item)
	}
	return out, rows.Err()
}

// SymlinkPublication holds the full library_path and target_path from symlink_publications.
type SymlinkPublication struct {
	LibraryPath string
	TargetPath  string
}

func (db *DB) GetSymlinkPathsForLibraryItem(ctx context.Context, libraryItemID int64) ([]string, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select library_path
		from symlink_publications
		where library_item_id = $1`, libraryItemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (db *DB) ListSymlinkPublications(ctx context.Context) ([]SymlinkPublication, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select library_path, target_path
		from symlink_publications
		order by library_path asc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SymlinkPublication
	for rows.Next() {
		var item SymlinkPublication
		if err := rows.Scan(&item.LibraryPath, &item.TargetPath); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) ListSelectedReleasesForPublication(ctx context.Context) ([]int64, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select distinct vf.selected_release_id
		from virtual_files vf
		join queue_items q on q.selected_release_id = vf.selected_release_id
		where q.state in ($1, $2, $3, $4)
		order by vf.selected_release_id asc`,
		QueuePreflight, QueuePublishing, QueueAvailable, QueueIndexing,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var selectedReleaseID int64
		if err := rows.Scan(&selectedReleaseID); err != nil {
			return nil, err
		}
		out = append(out, selectedReleaseID)
	}
	return out, rows.Err()
}

func (db *DB) ListSelectedReleasesByLibraryItem(ctx context.Context, libraryItemID int64) ([]int64, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select sr.id
		from selected_releases sr
		join virtual_files vf on vf.selected_release_id = sr.id
		where sr.library_item_id = $1
		order by sr.id asc`, libraryItemID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var selectedReleaseID int64
		if err := rows.Scan(&selectedReleaseID); err != nil {
			return nil, err
		}
		out = append(out, selectedReleaseID)
	}
	return out, rows.Err()
}

func (db *DB) ListPendingRepublishTargets(ctx context.Context) ([]PendingRepublishTarget, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select distinct sr.library_item_id
		from selected_releases sr
		join queue_items q on q.selected_release_id = sr.id
		join library_items li on li.id = sr.library_item_id
		where li.available = false
		  and q.state in ($1, $2, $3)
		order by sr.library_item_id asc`, QueuePreflight, QueuePublishing, QueueIndexing,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingRepublishTarget
	for rows.Next() {
		var item PendingRepublishTarget
		if err := rows.Scan(&item.LibraryItemID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

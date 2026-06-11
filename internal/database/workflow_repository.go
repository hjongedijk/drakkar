package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// preDeleteVFRBySelectedRelease removes virtual_file_ranges rows that would be
// cascade-deleted when a selected_release row is deleted, doing it as a single
// bulk DELETE instead of one-per-segment. For large NZBs (100k+ segments) the
// cascade does 100k individual index scans; this query does two.
func preDeleteVFRBySelectedRelease(ctx context.Context, tx *sql.Tx, selectedReleaseID int64) error {
	if _, err := tx.ExecContext(ctx, `
		delete from virtual_file_ranges
		where nzb_segment_id in (
			select ns.id from nzb_segments ns
			join nzb_files nf on nf.id = ns.nzb_file_id
			join nzb_documents nd on nd.id = nf.nzb_document_id
			where nd.selected_release_id = $1
		)`, selectedReleaseID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		delete from virtual_file_ranges
		where virtual_file_id in (
			select id from virtual_files where selected_release_id = $1
		)`, selectedReleaseID); err != nil {
		return err
	}
	return nil
}

// preDeleteVFRByLibraryItem does the same bulk pre-delete for all selected
// releases belonging to a library item.
func preDeleteVFRByLibraryItem(ctx context.Context, tx *sql.Tx, libraryItemID int64) error {
	if _, err := tx.ExecContext(ctx, `
		delete from virtual_file_ranges
		where nzb_segment_id in (
			select ns.id from nzb_segments ns
			join nzb_files nf on nf.id = ns.nzb_file_id
			join nzb_documents nd on nd.id = nf.nzb_document_id
			join selected_releases sr on sr.id = nd.selected_release_id
			where sr.library_item_id = $1
		)`, libraryItemID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		delete from virtual_file_ranges
		where virtual_file_id in (
			select id from virtual_files where selected_release_id in (
				select id from selected_releases where library_item_id = $1
			)
		)`, libraryItemID); err != nil {
		return err
	}
	return nil
}

// pgTextArray formats a Go string slice as a PostgreSQL text array literal.
func pgTextArray(vals []string) string {
	if len(vals) == 0 {
		return "{}"
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		// Escape double-quotes and backslashes inside array elements.
		escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(v)
		parts[i] = `"` + escaped + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (db *DB) ListMediaRequests(ctx context.Context) ([]MediaRequestSummary, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			mr.id,
			coalesce(mr.external_id, ''),
			mr.request_type,
			coalesce(li.title, ''),
			coalesce(li.media_type, ''),
			li.id,
			coalesce(q.state, ''),
			mr.created_at
		from media_requests mr
		left join queue_items q on q.id = (
			select q2.id from queue_items q2
			where q2.idempotency_key in ('seerr-movie-' || coalesce(mr.external_id, ''), 'seerr-tv-' || coalesce(mr.external_id, ''))
			order by q2.id desc
			limit 1
		)
		left join library_items li on li.id = q.library_item_id
		order by mr.created_at desc, mr.id desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MediaRequestSummary
	for rows.Next() {
		var (
			item          MediaRequestSummary
			libraryItemID sql.NullInt64
			queueState    string
		)
		if err := rows.Scan(
			&item.ID,
			&item.ExternalID,
			&item.RequestType,
			&item.Title,
			&item.MediaType,
			&libraryItemID,
			&queueState,
			&item.CreatedAt,
		); err != nil {
			return nil, err
		}
		item.QueueState = QueueState(queueState)
		if libraryItemID.Valid {
			value := libraryItemID.Int64
			item.LibraryItemID = &value
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) DetectMovieSearchConflict(ctx context.Context, libraryItemID int64) (string, error) {
	var reason string
	err := db.SQL.QueryRowContext(ctx, `
		select coalesce((
			select 'metadata_conflict'
			from library_items li
			join movies m on m.id = li.movie_id
			join library_items li2 on li2.id <> li.id and li2.media_type = 'movie'
			join movies m2 on m2.id = li2.movie_id
			where li.id = $1
			  and li.media_type = 'movie'
			  and lower(trim(li2.title)) = lower(trim(li.title))
			  and coalesce(m2.release_year, 0) = coalesce(m.release_year, 0)
			  and coalesce(m2.tmdb_id, 0) <> coalesce(m.tmdb_id, 0)
			  and coalesce(m.imdb_id, '') = ''
			  and coalesce(m2.imdb_id, '') <> ''
			limit 1
		), '')`, libraryItemID,
	).Scan(&reason)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(reason), nil
}

func (db *DB) UpsertMovieRequest(ctx context.Context, externalID string, tmdbID int64, title string, year int) (int64, bool, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var requestID int64
	err = tx.QueryRowContext(ctx, `select id from media_requests where external_id = $1 and request_type = 'movie'`, externalID).Scan(&requestID)
	if err == nil {
		var libraryItemID int64
		err = tx.QueryRowContext(ctx, `
			select q.library_item_id
			from queue_items q
			where q.idempotency_key = $1
			limit 1`, "seerr-movie-"+externalID).Scan(&libraryItemID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, false, err
		}
		if err = tx.Commit(); err != nil {
			return 0, false, err
		}
		return libraryItemID, false, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}

	var movieID int64
	err = tx.QueryRowContext(ctx, `select id from movies where tmdb_id = $1`, tmdbID).Scan(&movieID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `
			insert into movies (tmdb_id, title, release_year)
			values ($1, $2, $3)
			returning id`, tmdbID, title, year).Scan(&movieID)
	}
	if err != nil {
		return 0, false, err
	}

	var libraryItemID int64
	err = tx.QueryRowContext(ctx, `select id from library_items where movie_id = $1 limit 1`, movieID).Scan(&libraryItemID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `
			insert into library_items (media_type, movie_id, title)
			values ('movie', $1, $2)
			returning id`, movieID, title).Scan(&libraryItemID)
	}
	if err != nil {
		return 0, false, err
	}

	if err = tx.QueryRowContext(ctx, `
		insert into media_requests (external_id, request_type)
		values ($1, 'movie')
		returning id`, externalID).Scan(&requestID); err != nil {
		return 0, false, err
	}
	if _, err = tx.ExecContext(ctx, `
		insert into queue_items (library_item_id, state, idempotency_key)
		values ($1, $2, $3)
		on conflict (idempotency_key) do nothing`,
		libraryItemID, QueueRequested, "seerr-movie-"+externalID,
	); err != nil {
		return 0, false, err
	}

	if err = tx.Commit(); err != nil {
		return 0, false, err
	}
	return libraryItemID, true, nil
}

// MovieEnrichment carries all enrichable movie fields from TMDB/Seerr.
type MovieEnrichment struct {
	TMDBID               int64
	Title                string
	OriginalTitle        string
	Year                 int
	ReleaseDate          string   // "YYYY-MM-DD"
	IMDbID               string
	Overview             string
	Tagline              string
	Status               string   // "Released", "In Production", etc.
	ContentRating        string   // "PG-13", "R", etc.
	OriginalLanguage     string
	RuntimeMinutes       int
	PosterURL            string
	BackdropURL          string
	TrailerURL           string
	Genres               []string
	AlternativeTitles    []string
	ProductionCompanies  []string
	Popularity           float64
	VoteAverage          float64
	VoteCount            int
	Budget               int64
	Revenue              int64
	CastJSON             []byte   // JSON: [{name,character,profile_url}]
	RawTMDB              []byte   // full /movie/:id TMDB JSON response
}

func (db *DB) EnrichMovieMetadata(ctx context.Context, libraryItemID, tmdbID int64, title string, year int, imdbID string) error {
	return db.EnrichMovieFull(ctx, libraryItemID, MovieEnrichment{
		TMDBID: tmdbID, Title: title, Year: year, IMDbID: imdbID,
	})
}

func (db *DB) EnrichMovieFull(ctx context.Context, libraryItemID int64, e MovieEnrichment) error {
	var releaseDate *string
	if e.ReleaseDate != "" {
		releaseDate = &e.ReleaseDate
	}
	var castJSON, rawTMDB interface{}
	if len(e.CastJSON) > 0 {
		castJSON = e.CastJSON
	}
	if len(e.RawTMDB) > 0 {
		rawTMDB = e.RawTMDB
	}
	_, err := db.SQL.ExecContext(ctx, `
		update movies m
		set tmdb_id              = case when $2 > 0   then $2  else m.tmdb_id              end,
		    title                = case when $3 <> ''  then $3  else m.title                end,
		    original_title       = case when $4 <> ''  then $4  else m.original_title       end,
		    release_year         = case when $5 > 0    then $5  else m.release_year         end,
		    imdb_id              = case when $6 <> ''  then $6  else m.imdb_id              end,
		    overview             = case when $7 <> ''  then $7  else m.overview             end,
		    original_language    = case when $8 <> ''  then $8  else m.original_language    end,
		    runtime_minutes      = case when $9 > 0    then $9  else m.runtime_minutes      end,
		    poster_url           = case when $10 <> '' then $10 else m.poster_url           end,
		    backdrop_url         = case when $11 <> '' then $11 else m.backdrop_url         end,
		    popularity           = case when $12 > 0   then $12 else m.popularity           end,
		    vote_average         = case when $13 > 0   then $13 else m.vote_average         end,
		    genres               = case when array_length($14::text[], 1) > 0 then $14::text[] else m.genres end,
		    alternative_titles   = case when array_length($15::text[], 1) > 0 then $15::text[] else m.alternative_titles end,
		    tagline              = case when $16 <> '' then $16 else m.tagline              end,
		    status               = case when $17 <> '' then $17 else m.status               end,
		    content_rating       = case when $18 <> '' then $18 else m.content_rating       end,
		    trailer_url          = case when $19 <> '' then $19 else m.trailer_url          end,
		    vote_count           = case when $20::bigint > 0 then $20::bigint else m.vote_count  end,
		    budget               = case when $21::bigint > 0 then $21::bigint else m.budget      end,
		    revenue              = case when $22::bigint > 0 then $22::bigint else m.revenue     end,
		    production_companies = case when array_length($23::text[], 1) > 0 then $23::text[] else m.production_companies end,
		    release_date         = case when $24::date is not null then $24::date else m.release_date end,
		    cast_json            = case when $25::jsonb is not null then $25::jsonb else m.cast_json end,
		    raw_tmdb             = case when $26::jsonb is not null then $26::jsonb else m.raw_tmdb end
		from library_items li
		where li.id = $1
		  and li.movie_id = m.id`,
		libraryItemID,
		e.TMDBID, e.Title, e.OriginalTitle, e.Year, e.IMDbID,
		e.Overview, e.OriginalLanguage, e.RuntimeMinutes,
		e.PosterURL, e.BackdropURL, e.Popularity, e.VoteAverage,
		pgTextArray(e.Genres), pgTextArray(e.AlternativeTitles),
		e.Tagline, e.Status, e.ContentRating, e.TrailerURL,
		e.VoteCount, e.Budget, e.Revenue, pgTextArray(e.ProductionCompanies),
		releaseDate, castJSON, rawTMDB,
	)
	if err != nil {
		return err
	}
	if strings.TrimSpace(e.Title) == "" {
		return nil
	}
	_, err = db.SQL.ExecContext(ctx, `update library_items set title = $2 where id = $1`, libraryItemID, e.Title)
	return err
}

func (db *DB) UpsertEpisodeRequest(ctx context.Context, externalID string, tvdbID, tmdbID int64, show string, year, season, episode int, episodeTitle string) (int64, bool, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var requestID int64
	err = tx.QueryRowContext(ctx, `select id from media_requests where external_id = $1 and request_type = 'tv'`, externalID).Scan(&requestID)
	if err == nil {
		var libraryItemID int64
		err = tx.QueryRowContext(ctx, `
			select q.library_item_id
			from queue_items q
			where q.idempotency_key = $1
			limit 1`, "seerr-tv-"+externalID).Scan(&libraryItemID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, false, err
		}
		if err = tx.Commit(); err != nil {
			return 0, false, err
		}
		return libraryItemID, false, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}

	var showID int64
	err = tx.QueryRowContext(ctx, `select id from tv_shows where tvdb_id = $1`, tvdbID).Scan(&showID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `
			insert into tv_shows (tvdb_id, tmdb_id, title, release_year)
			values ($1, $2, $3, $4)
			returning id`, tvdbID, tmdbID, show, year).Scan(&showID)
	}
	if err != nil {
		return 0, false, err
	}

	var episodeID int64
	err = tx.QueryRowContext(ctx, `
		select id from episodes
		where tv_show_id = $1 and season_number = $2 and episode_number = $3`,
		showID, season, episode).Scan(&episodeID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `
			insert into episodes (tv_show_id, season_number, episode_number, tvdb_id, tmdb_id, title)
			values ($1, $2, $3, $4, $5, $6)
			returning id`, showID, season, episode, tvdbID, tmdbID, episodeTitle).Scan(&episodeID)
	}
	if err != nil {
		return 0, false, err
	}

	var libraryItemID int64
	title := show
	if season > 0 && episode > 0 {
		title = fmt.Sprintf("%s S%02dE%02d", show, season, episode)
	}
	err = tx.QueryRowContext(ctx, `select id from library_items where episode_id = $1 limit 1`, episodeID).Scan(&libraryItemID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `
			insert into library_items (media_type, episode_id, title)
			values ('episode', $1, $2)
			returning id`, episodeID, title).Scan(&libraryItemID)
	}
	if err != nil {
		return 0, false, err
	}

	if err = tx.QueryRowContext(ctx, `
		insert into media_requests (external_id, request_type)
		values ($1, 'tv')
		returning id`, externalID).Scan(&requestID); err != nil {
		return 0, false, err
	}
	if _, err = tx.ExecContext(ctx, `
		insert into queue_items (library_item_id, state, idempotency_key)
		values ($1, $2, $3)
		on conflict (idempotency_key) do nothing`,
		libraryItemID, QueueRequested, "seerr-tv-"+externalID,
	); err != nil {
		return 0, false, err
	}

	if err = tx.Commit(); err != nil {
		return 0, false, err
	}
	return libraryItemID, true, nil
}

// TVShowEnrichment carries all enrichable TV show fields from TMDB/TVDB/Seerr.
type TVShowEnrichment struct {
	TMDBID               int64
	ShowTitle            string
	OriginalName         string
	Year                 int
	FirstAirDate         string   // "YYYY-MM-DD"
	LastAirDate          string   // "YYYY-MM-DD"
	IMDbID               string
	Overview             string
	Tagline              string
	Status               string   // "Returning Series", "Ended", etc.
	ContentRating        string   // "TV-MA", "TV-14", etc.
	OriginalLanguage     string
	Network              string
	EpisodeRunTime       int
	NumberOfSeasons      int
	NumberOfEpisodes     int
	InProduction         bool
	PosterURL            string
	BackdropURL          string
	TrailerURL           string
	Genres               []string
	AlternativeTitles    []string
	ProductionCompanies  []string
	Popularity           float64
	VoteAverage          float64
	VoteCount            int
	CastJSON             []byte   // JSON: [{name,character,profile_url}]
	RawTMDB              []byte
}

func (db *DB) EnrichEpisodeMetadata(ctx context.Context, libraryItemID, tmdbID int64, show string, year int, imdbID, episodeTitle string) error {
	return db.EnrichTVFull(ctx, libraryItemID, episodeTitle, TVShowEnrichment{
		TMDBID: tmdbID, ShowTitle: show, Year: year, IMDbID: imdbID,
	})
}

func (db *DB) EnrichTVFull(ctx context.Context, libraryItemID int64, episodeTitle string, e TVShowEnrichment) error {
	var firstAirDate, lastAirDate *string
	if e.FirstAirDate != "" {
		firstAirDate = &e.FirstAirDate
	}
	if e.LastAirDate != "" {
		lastAirDate = &e.LastAirDate
	}
	var castJSON, rawTMDB interface{}
	if len(e.CastJSON) > 0 {
		castJSON = e.CastJSON
	}
	if len(e.RawTMDB) > 0 {
		rawTMDB = e.RawTMDB
	}
	_, err := db.SQL.ExecContext(ctx, `
		update tv_shows tv
		set tmdb_id              = case when $2 > 0   then $2  else tv.tmdb_id              end,
		    title                = case when $3 <> ''  then $3  else tv.title                end,
		    original_name        = case when $4 <> ''  then $4  else tv.original_name        end,
		    release_year         = case when $5 > 0    then $5  else tv.release_year         end,
		    imdb_id              = case when $6 <> ''  then $6  else tv.imdb_id              end,
		    overview             = case when $7 <> ''  then $7  else tv.overview             end,
		    original_language    = case when $8 <> ''  then $8  else tv.original_language    end,
		    network              = case when $9 <> ''  then $9  else tv.network              end,
		    status               = case when $10 <> '' then $10 else tv.status               end,
		    episode_run_time     = case when $11 > 0   then $11 else tv.episode_run_time     end,
		    number_of_seasons    = case when $12 > 0   then $12 else tv.number_of_seasons    end,
		    number_of_episodes   = case when $13 > 0   then $13 else tv.number_of_episodes   end,
		    poster_url           = case when $14 <> '' then $14 else tv.poster_url           end,
		    backdrop_url         = case when $15 <> '' then $15 else tv.backdrop_url         end,
		    popularity           = case when $16 > 0   then $16 else tv.popularity           end,
		    genres               = case when array_length($17::text[], 1) > 0 then $17::text[] else tv.genres end,
		    alternative_titles   = case when array_length($18::text[], 1) > 0 then $18::text[] else tv.alternative_titles end,
		    tagline              = case when $19 <> '' then $19 else tv.tagline              end,
		    content_rating       = case when $20 <> '' then $20 else tv.content_rating       end,
		    trailer_url          = case when $21 <> '' then $21 else tv.trailer_url          end,
		    vote_average         = case when $22 > 0   then $22 else tv.vote_average         end,
		    vote_count           = case when $23::bigint > 0 then $23::bigint else tv.vote_count end,
		    in_production        = $24,
		    production_companies = case when array_length($25::text[], 1) > 0 then $25::text[] else tv.production_companies end,
		    first_air_date       = case when $26::date is not null then $26::date else tv.first_air_date end,
		    last_air_date        = case when $27::date is not null then $27::date else tv.last_air_date  end,
		    cast_json            = case when $28::jsonb is not null then $28::jsonb else tv.cast_json end,
		    raw_tmdb             = case when $29::jsonb is not null then $29::jsonb else tv.raw_tmdb end
		from library_items li
		join episodes ep on ep.id = li.episode_id
		where li.id = $1
		  and ep.tv_show_id = tv.id`,
		libraryItemID,
		e.TMDBID, e.ShowTitle, e.OriginalName, e.Year, e.IMDbID,
		e.Overview, e.OriginalLanguage, e.Network, e.Status,
		e.EpisodeRunTime, e.NumberOfSeasons, e.NumberOfEpisodes,
		e.PosterURL, e.BackdropURL, e.Popularity,
		pgTextArray(e.Genres), pgTextArray(e.AlternativeTitles),
		e.Tagline, e.ContentRating, e.TrailerURL,
		e.VoteAverage, e.VoteCount, e.InProduction,
		pgTextArray(e.ProductionCompanies),
		firstAirDate, lastAirDate, castJSON, rawTMDB,
	)
	if err != nil {
		return err
	}
	if strings.TrimSpace(episodeTitle) != "" {
		if _, err = db.SQL.ExecContext(ctx, `
			update episodes
			set title = $2
			where id = (select episode_id from library_items where id = $1)`, libraryItemID, episodeTitle,
		); err != nil {
			return err
		}
	}
	if strings.TrimSpace(e.ShowTitle) == "" {
		return nil
	}
	var (
		seasonNumber  int
		episodeNumber int
	)
	if err = db.SQL.QueryRowContext(ctx, `
		select e.season_number, e.episode_number
		from library_items li
		join episodes e on e.id = li.episode_id
		where li.id = $1`, libraryItemID,
	).Scan(&seasonNumber, &episodeNumber); err != nil {
		return err
	}
	title := e.ShowTitle
	if seasonNumber > 0 && episodeNumber > 0 {
		title = fmt.Sprintf("%s S%02dE%02d", e.ShowTitle, seasonNumber, episodeNumber)
	}
	_, err = db.SQL.ExecContext(ctx, `
		update library_items
		set title = $2
		where id = $1`, libraryItemID, title,
	)
	return err
}

func (db *DB) GetLibrarySearchInput(ctx context.Context, libraryItemID int64) (LibrarySearchInput, error) {
	var item LibrarySearchInput
	err := db.SQL.QueryRowContext(ctx, `
		select
			li.id,
			li.media_type,
			coalesce(li.title, ''),
			coalesce(m.imdb_id, ''),
			coalesce(m.release_year, 0),
			coalesce(m.tmdb_id, 0),
			coalesce(tv.title, ''),
			coalesce(e.title, ''),
			coalesce(tv.imdb_id, ''),
			coalesce(tv.tvdb_id, 0),
			coalesce(tv.tmdb_id, 0),
			coalesce(tv.release_year, 0),
			coalesce(e.season_number, 0),
			coalesce(e.episode_number, 0),
			coalesce(tv.id, 0),
			coalesce(m.alternative_titles, '{}') || coalesce(tv.alternative_titles, '{}')
		from library_items li
		left join movies m on m.id = li.movie_id
		left join episodes e on e.id = li.episode_id
		left join tv_shows tv on tv.id = e.tv_show_id
		where li.id = $1`, libraryItemID,
	).Scan(
		&item.LibraryItemID,
		&item.MediaType,
		&item.Title,
		&item.IMDbID,
		&item.MovieYear,
		&item.MovieTMDBID,
		&item.ShowTitle,
		&item.EpisodeTitle,
		&item.ShowIMDbID,
		&item.ShowTVDBID,
		&item.ShowTMDBID,
		&item.ShowYear,
		&item.SeasonNumber,
		&item.EpisodeNumber,
		&item.TVShowID,
		pgTextArrayScan(&item.AlternateTitles),
	)
	return item, err
}

func (db *DB) GetQueueRetryTarget(ctx context.Context, queueItemID int64) (QueueRetryTarget, error) {
	var item QueueRetryTarget
	var selectedRelease sql.NullInt64
	err := db.SQL.QueryRowContext(ctx, `
		select
			q.id,
			q.library_item_id,
			q.selected_release_id,
			li.media_type,
			q.idempotency_key
		from queue_items q
		join library_items li on li.id = q.library_item_id
		where q.id = $1`, queueItemID,
	).Scan(&item.QueueItemID, &item.LibraryItemID, &selectedRelease, &item.MediaType, &item.IdempotencyKey)
	if err != nil {
		return QueueRetryTarget{}, err
	}
	if selectedRelease.Valid {
		value := selectedRelease.Int64
		item.SelectedReleaseID = &value
	}
	return item, nil
}

func (db *DB) ListPendingLibrarySearchTargets(ctx context.Context) ([]PendingLibrarySearchTarget, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select item.library_item_id
		from (
			select distinct on (q.library_item_id)
				q.library_item_id,
				q.created_at,
				q.id
			from queue_items q
			join library_items li on li.id = q.library_item_id
			where li.available = false
			  and q.selected_release_id is null
			  and q.state in ($1, $2)
			  and li.media_type in ('movie', 'episode', 'tv')
			  -- Skip movies that haven't been released yet (release_date in the future).
			  -- These will never be on Usenet until after the theatrical release.
			  and not exists (
			      select 1 from movies m
			      where m.id = li.movie_id
			        and m.release_date is not null
			        and m.release_date > current_date
			  )
			  -- Add cooldown for failed items: only retry after 2 hours.
			  -- New 'requested' items have updated_at = created_at so they pass immediately.
			  and (q.state != $2 or q.updated_at < now() - interval '2 hours')
			order by q.library_item_id, q.created_at asc, q.id asc
		) item
		order by item.created_at asc, item.id asc`, QueueRequested, QueueFailed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingLibrarySearchTarget
	for rows.Next() {
		var item PendingLibrarySearchTarget
		if err := rows.Scan(&item.LibraryItemID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) ListFailedQueueRetryTargets(ctx context.Context) ([]FailedQueueRetryTarget, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			q.id,
			q.library_item_id,
			case
				-- 'requested' items with a selected release are stuck: interrupted
				-- before the fetch started. Treat as interrupted_by_restart so
				-- RetryQueueItem re-dispatches fetchAndImportSelectedRelease.
				when q.state = $2 and q.selected_release_id is not null
					then 'interrupted_by_restart'
				else coalesce(q.failure_reason, '')
			end as failure_reason
		from queue_items q
		join library_items li on li.id = q.library_item_id
		where li.available = false
		  and (
		    q.state = $1
		    or (q.state = $2 and q.selected_release_id is not null)
		  )
		order by q.updated_at asc, q.id asc`, QueueFailed, QueueRequested,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FailedQueueRetryTarget
	for rows.Next() {
		var item FailedQueueRetryTarget
		if err := rows.Scan(&item.QueueItemID, &item.LibraryItemID, &item.FailureReason); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) LookupCandidateHistory(ctx context.Context, libraryItemID int64) (map[string]CandidateHistory, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select
			coalesce(external_url, ''),
			max(failure_count),
			coalesce(max(nullif(last_failure_reason, '')), '')
		from release_candidates
		where library_item_id = $1
		  and coalesce(external_url, '') <> ''
		group by external_url`, libraryItemID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	history := map[string]CandidateHistory{}
	for rows.Next() {
		var item CandidateHistory
		if err := rows.Scan(&item.ExternalURL, &item.FailureCount, &item.LastFailureReason); err != nil {
			return nil, err
		}
		history[strings.TrimSpace(item.ExternalURL)] = item
	}
	return history, rows.Err()
}

func (db *DB) ReplaceSearchCandidates(ctx context.Context, libraryItemID int64, candidates []SearchCandidateRecord) (*int64, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', updated_at = now()
		where library_item_id = $1`, libraryItemID, QueueSearching); err != nil {
		return nil, err
	}
	if err = preDeleteVFRByLibraryItem(ctx, tx, libraryItemID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `delete from selected_releases where library_item_id = $1`, libraryItemID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `delete from release_candidates where library_item_id = $1`, libraryItemID); err != nil {
		return nil, err
	}

	blocked, err := loadBlocklistMap(ctx, tx)
	if err != nil {
		return nil, err
	}

	var selectedReleaseID *int64
	selectedAssigned := false
	for _, candidate := range candidates {
		if reason, ok := blockedReleaseReason(blocked, candidate); ok {
			candidate.Rejected = true
			if candidate.RejectReason == "" {
				candidate.RejectReason = reason
			}
		}
		shouldSelect := !selectedAssigned && !candidate.Rejected
		var releaseCandidateID int64
		if err = tx.QueryRowContext(ctx, `
			insert into release_candidates (
				library_item_id, title, score, selected, rejected, reject_reason,
				failure_count, last_failure_reason, external_url, indexer_name, size_bytes, posted_at
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			returning id`,
			libraryItemID,
			candidate.Title,
			candidate.Score,
			shouldSelect,
			candidate.Rejected,
			candidate.RejectReason,
			candidate.FailureCount,
			candidate.LastFailureReason,
			candidate.ExternalURL,
			candidate.IndexerName,
			candidate.SizeBytes,
			nullTime(candidate.PostedAt),
		).Scan(&releaseCandidateID); err != nil {
			return nil, err
		}
		if shouldSelect {
			var value int64
			if err = tx.QueryRowContext(ctx, `
				insert into selected_releases (library_item_id, release_candidate_id)
				values ($1, $2)
				returning id`, libraryItemID, releaseCandidateID).Scan(&value); err != nil {
				return nil, err
			}
			selectedReleaseID = &value
			selectedAssigned = true
		}
	}

	if selectedReleaseID != nil {
		if _, err = tx.ExecContext(ctx, `
			update queue_items
			set state = $2, selected_release_id = $3, updated_at = now()
			where library_item_id = $1`, libraryItemID, QueueSelected, *selectedReleaseID); err != nil {
			return nil, err
		}
	} else {
		failureReason := summarizeSearchFailureReason(candidates)
		if _, err = tx.ExecContext(ctx, `
			update queue_items
			set state = $2, failure_reason = $3, selected_release_id = null, updated_at = now()
			where library_item_id = $1`, libraryItemID, QueueFailed, failureReason); err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return selectedReleaseID, nil
}

func (db *DB) MarkLibrarySearchFailed(ctx context.Context, libraryItemID int64, reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "search_error"
	}
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = $3, selected_release_id = null, updated_at = now()
		where library_item_id = $1`, libraryItemID, QueueFailed, reason,
	)
	return err
}

func summarizeSearchFailureReason(candidates []SearchCandidateRecord) string {
	if len(candidates) == 0 {
		return "no_releases"
	}
	reasons := map[string]int{}
	for _, candidate := range candidates {
		if !candidate.Rejected {
			return ""
		}
		reason := strings.TrimSpace(strings.ToLower(candidate.RejectReason))
		if reason == "" {
			reason = "rejected"
		}
		reasons[reason]++
	}
	if len(reasons) == 0 {
		return "no_releases"
	}
	if len(reasons) == 1 {
		for reason := range reasons {
			if reason == "rejected" {
				return "all_candidates_rejected"
			}
			return "all_candidates_" + reason
		}
	}
	archiveOnly := true
	for reason := range reasons {
		if !strings.HasPrefix(reason, "archive_") {
			archiveOnly = false
			break
		}
	}
	if archiveOnly {
		return "all_candidates_archive_rejected"
	}
	return "all_candidates_rejected"
}

func (db *DB) GetSelectedReleaseSummary(ctx context.Context, selectedReleaseID int64) (ReleaseSummary, error) {
	var item ReleaseSummary
	var nzbDocument sql.NullInt64
	err := db.SQL.QueryRowContext(ctx, `
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
			rc.selected,
			rc.rejected,
			rc.reject_reason,
			rc.failure_count,
			rc.last_failure_reason,
			coalesce((select count(*) from archives a where a.selected_release_id = sr.id), 0),
			coalesce((select count(*) from archive_volumes av join archives a on a.id = av.archive_id where a.selected_release_id = sr.id), 0),
			coalesce((select string_agg(distinct a.status, ',' order by a.status) from archives a where a.selected_release_id = sr.id), ''),
			coalesce((select string_agg(distinct a.reject_reason, ',' order by a.reject_reason) from archives a where a.selected_release_id = sr.id and a.reject_reason <> ''), ''),
			coalesce((select count(*) from virtual_files vf where vf.selected_release_id = sr.id), 0),
			rc.created_at,
			n.id,
			coalesce(n.file_name, '')
		from selected_releases sr
		join release_candidates rc on rc.id = sr.release_candidate_id
		left join nzb_documents n on n.selected_release_id = sr.id
		where sr.id = $1`, selectedReleaseID,
	).Scan(
		&item.SelectedReleaseID,
		&item.ReleaseCandidateID,
		&item.LibraryItemID,
		&item.Title,
		&item.ExternalURL,
		&item.IndexerName,
		&item.SizeBytes,
		&item.PostedAt,
		&item.Score,
		&item.Selected,
		&item.Rejected,
		&item.RejectReason,
		&item.FailureCount,
		&item.LastFailureReason,
		&item.ArchiveCount,
		&item.ArchiveVolumeCount,
		&item.ArchiveStatuses,
		&item.ArchiveRejects,
		&item.VirtualFileCount,
		&item.CreatedAt,
		&nzbDocument,
		&item.NZBFileName,
	)
	if err != nil {
		return ReleaseSummary{}, err
	}
	if nzbDocument.Valid {
		value := nzbDocument.Int64
		item.NZBDocumentID = &value
	}
	return item, nil
}

func (db *DB) GetStoredNZBDocument(ctx context.Context, selectedReleaseID int64) (StoredNZBDocument, error) {
	var item StoredNZBDocument
	err := db.SQL.QueryRowContext(ctx, `
		select
			selected_release_id,
			coalesce(file_name, ''),
			coalesce(external_url, ''),
			xml
		from nzb_documents
		where selected_release_id = $1
		order by id desc
		limit 1`, selectedReleaseID,
	).Scan(&item.SelectedReleaseID, &item.FileName, &item.ExternalURL, &item.XML)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return StoredNZBDocument{}, fmt.Errorf("selected release %d has no stored nzb document", selectedReleaseID)
		}
		return StoredNZBDocument{}, err
	}
	if strings.TrimSpace(item.FileName) == "" {
		item.FileName = "selected.nzb"
	}
	return item, nil
}

func (db *DB) SelectReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*ReleaseSummary, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var (
		libraryItemID int64
		title         string
		externalURL   string
		indexerName   string
		sizeBytes     int64
		postedAt      time.Time
	)
	if err = tx.QueryRowContext(ctx, `
		select library_item_id, title, coalesce(external_url, ''), coalesce(indexer_name, ''), coalesce(size_bytes, 0), coalesce(posted_at, to_timestamp(0))
		from release_candidates
		where id = $1`, releaseCandidateID,
	).Scan(&libraryItemID, &title, &externalURL, &indexerName, &sizeBytes, &postedAt); err != nil {
		return nil, err
	}

	if err = preDeleteVFRByLibraryItem(ctx, tx, libraryItemID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		delete from selected_releases
		where library_item_id = $1`, libraryItemID,
	); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = false
		where library_item_id = $1`, libraryItemID,
	); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = true, rejected = false, reject_reason = ''
		where id = $1`, releaseCandidateID,
	); err != nil {
		return nil, err
	}
	if strings.TrimSpace(externalURL) != "" {
		for _, key := range blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt) {
			if _, err = tx.ExecContext(ctx, `
				delete from blocklist_items
				where key = $1`, key,
			); err != nil {
				return nil, err
			}
		}
	}

	var selectedReleaseID int64
	if err = tx.QueryRowContext(ctx, `
		insert into selected_releases (library_item_id, release_candidate_id)
		values ($1, $2)
		returning id`, libraryItemID, releaseCandidateID,
	).Scan(&selectedReleaseID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', selected_release_id = $3, updated_at = now()
		where library_item_id = $1`, libraryItemID, QueueSelected, selectedReleaseID,
	); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	item, err := db.GetSelectedReleaseSummary(ctx, selectedReleaseID)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (db *DB) PromoteBestRetryCandidate(ctx context.Context, libraryItemID int64) (*ReleaseSummary, error) {
	return db.promoteRetryCandidate(ctx, libraryItemID, 0, false)
}

func (db *DB) PromoteAlternativeRetryCandidate(ctx context.Context, libraryItemID int64, excludeReleaseCandidateID int64) (*ReleaseSummary, error) {
	return db.promoteRetryCandidate(ctx, libraryItemID, excludeReleaseCandidateID, true)
}

func (db *DB) promoteRetryCandidate(ctx context.Context, libraryItemID int64, excludeReleaseCandidateID int64, excludeCurrent bool) (*ReleaseSummary, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var releaseCandidateID int64
	query := `
		select id
		from release_candidates
		where library_item_id = $1
		  and rejected = false
		  and coalesce(external_url, '') <> ''
	`
	args := []any{libraryItemID}
	if excludeCurrent {
		query += ` and id <> $2`
		args = append(args, excludeReleaseCandidateID)
	}
	query += `
		order by failure_count asc, selected desc, score desc, created_at asc, id asc
		limit 1`
	err = tx.QueryRowContext(ctx, query, args...).Scan(&releaseCandidateID)
	if errors.Is(err, sql.ErrNoRows) {
		if err = tx.Rollback(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if err = preDeleteVFRByLibraryItem(ctx, tx, libraryItemID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		delete from selected_releases
		where library_item_id = $1`, libraryItemID,
	); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = false
		where library_item_id = $1`, libraryItemID,
	); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = true
		where id = $1`, releaseCandidateID,
	); err != nil {
		return nil, err
	}

	var selectedReleaseID int64
	if err = tx.QueryRowContext(ctx, `
		insert into selected_releases (library_item_id, release_candidate_id)
		values ($1, $2)
		returning id`, libraryItemID, releaseCandidateID,
	).Scan(&selectedReleaseID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', selected_release_id = $3, updated_at = now()
		where library_item_id = $1`, libraryItemID, QueueSelected, selectedReleaseID,
	); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}

	item, err := db.GetSelectedReleaseSummary(ctx, selectedReleaseID)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (db *DB) RejectReleaseCandidate(ctx context.Context, releaseCandidateID int64, reason string) (*ReleaseSummary, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var (
		libraryItemID int64
		title         string
		externalURL   string
		indexerName   string
		sizeBytes     int64
		postedAt      time.Time
	)
	if err = tx.QueryRowContext(ctx, `
		select library_item_id, title, coalesce(external_url, ''), coalesce(indexer_name, ''), coalesce(size_bytes, 0), coalesce(posted_at, to_timestamp(0))
		from release_candidates
		where id = $1`, releaseCandidateID,
	).Scan(&libraryItemID, &title, &externalURL, &indexerName, &sizeBytes, &postedAt); err != nil {
		return nil, err
	}

	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set rejected = true, reject_reason = $2, selected = false
		where id = $1`, releaseCandidateID, reason,
	); err != nil {
		return nil, err
	}
	if shouldPersistBlocklistReason(reason) && strings.TrimSpace(externalURL) != "" {
		for _, key := range blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt) {
			if _, err = tx.ExecContext(ctx, `
				insert into blocklist_items (key, reason)
				values ($1, $2)
				on conflict (key)
				do update set reason = excluded.reason, expires_at = null`,
				key, reason,
			); err != nil {
				return nil, err
			}
		}
	}
	// Pre-delete VFR for the selected_release being removed by this promote.
	if _, err = tx.ExecContext(ctx, `
		delete from virtual_file_ranges
		where nzb_segment_id in (
			select ns.id from nzb_segments ns
			join nzb_files nf on nf.id = ns.nzb_file_id
			join nzb_documents nd on nd.id = nf.nzb_document_id
			join selected_releases sr on sr.id = nd.selected_release_id
			where sr.release_candidate_id = $1
		)`, releaseCandidateID,
	); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		delete from virtual_file_ranges
		where virtual_file_id in (
			select id from virtual_files where selected_release_id in (
				select id from selected_releases where release_candidate_id = $1
			)
		)`, releaseCandidateID,
	); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		delete from selected_releases
		where release_candidate_id = $1`, releaseCandidateID,
	); err != nil {
		return nil, err
	}

	var nextCandidateID sql.NullInt64
	if err = tx.QueryRowContext(ctx, `
		select id
		from release_candidates
		where library_item_id = $1
		  and rejected = false
		  and id <> $2
		order by failure_count asc,
		         score desc,
		         posted_at desc nulls last,
		         created_at asc,
		         id asc
		limit 1`, libraryItemID, releaseCandidateID,
	).Scan(&nextCandidateID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	if !nextCandidateID.Valid {
		if _, err = tx.ExecContext(ctx, `
			update queue_items
			set state = $2, failure_reason = $3, selected_release_id = null, updated_at = now()
			where library_item_id = $1`, libraryItemID, QueueFailed, reason,
		); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = true
		where id = $1`, nextCandidateID.Int64,
	); err != nil {
		return nil, err
	}

	var selectedReleaseID int64
	if err = tx.QueryRowContext(ctx, `
		insert into selected_releases (library_item_id, release_candidate_id)
		values ($1, $2)
		returning id`, libraryItemID, nextCandidateID.Int64,
	).Scan(&selectedReleaseID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', selected_release_id = $3, updated_at = now()
		where library_item_id = $1`, libraryItemID, QueueSelected, selectedReleaseID,
	); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	item, err := db.GetSelectedReleaseSummary(ctx, selectedReleaseID)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (db *DB) RestoreReleaseCandidate(ctx context.Context, releaseCandidateID int64) error {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var (
		title       string
		externalURL string
		indexerName string
		sizeBytes   int64
		postedAt    time.Time
	)
	if err = tx.QueryRowContext(ctx, `
		select title, coalesce(external_url, ''), coalesce(indexer_name, ''), coalesce(size_bytes, 0), coalesce(posted_at, to_timestamp(0))
		from release_candidates
		where id = $1`, releaseCandidateID,
	).Scan(&title, &externalURL, &indexerName, &sizeBytes, &postedAt); err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set rejected = false, reject_reason = ''
		where id = $1`, releaseCandidateID,
	); err != nil {
		return err
	}
	if strings.TrimSpace(externalURL) != "" {
		for _, key := range blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt) {
			if _, err = tx.ExecContext(ctx, `
				delete from blocklist_items
				where key = $1`, key,
			); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (db *DB) RestoreRejectedReleaseCandidates(ctx context.Context, libraryItemID int64) (RejectedReleaseRestoreResult, error) {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return RejectedReleaseRestoreResult{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx, `
		select id, title, coalesce(external_url, ''), coalesce(indexer_name, ''), coalesce(size_bytes, 0), coalesce(posted_at, to_timestamp(0))
		from release_candidates
		where library_item_id = $1
		  and rejected = true`, libraryItemID,
	)
	if err != nil {
		return RejectedReleaseRestoreResult{}, err
	}
	defer rows.Close()

	var (
		restored int
		keys     []string
	)
	for rows.Next() {
		var (
			releaseCandidateID int64
			title              string
			externalURL        string
			indexerName        string
			sizeBytes          int64
			postedAt           time.Time
		)
		if err = rows.Scan(&releaseCandidateID, &title, &externalURL, &indexerName, &sizeBytes, &postedAt); err != nil {
			return RejectedReleaseRestoreResult{}, err
		}
		restored++
		if strings.TrimSpace(externalURL) != "" {
			keys = append(keys, blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt)...)
		}
	}
	if err = rows.Err(); err != nil {
		return RejectedReleaseRestoreResult{}, err
	}

	if restored == 0 {
		if err = tx.Commit(); err != nil {
			return RejectedReleaseRestoreResult{}, err
		}
		return RejectedReleaseRestoreResult{LibraryItemID: libraryItemID}, nil
	}

	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set rejected = false, reject_reason = ''
		where library_item_id = $1
		  and rejected = true`, libraryItemID,
	); err != nil {
		return RejectedReleaseRestoreResult{}, err
	}
	for _, key := range keys {
		if _, err = tx.ExecContext(ctx, `
			delete from blocklist_items
			where key = $1`, key,
		); err != nil {
			return RejectedReleaseRestoreResult{}, err
		}
	}

	if err = tx.Commit(); err != nil {
		return RejectedReleaseRestoreResult{}, err
	}
	return RejectedReleaseRestoreResult{LibraryItemID: libraryItemID, Restored: restored}, nil
}

func (db *DB) SkipReleaseCandidate(ctx context.Context, releaseCandidateID int64) (*ReleaseSummary, error) {
	var selectedReleaseID int64
	if err := db.SQL.QueryRowContext(ctx, `
		select sr.id
		from selected_releases sr
		where sr.release_candidate_id = $1`, releaseCandidateID,
	).Scan(&selectedReleaseID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("release candidate %d is not currently selected", releaseCandidateID)
		}
		return nil, err
	}
	return db.FailSelectedReleaseAndPromoteNext(ctx, selectedReleaseID, "manual_skip")
}

func (db *DB) FailSelectedReleaseAndPromoteNext(ctx context.Context, selectedReleaseID int64, reason string) (*ReleaseSummary, error) {
	// Cap at 90s: cascade-deleting nzb_segments/virtual_file_ranges for a large
	// release (100k+ segments) takes ~22s with proper indexes. 90s gives headroom
	// for the largest NZBs without hanging indefinitely on lock contention.
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var (
		libraryItemID      int64
		releaseCandidateID int64
		externalURL        string
	)
	if err = tx.QueryRowContext(ctx, `
		select sr.library_item_id, sr.release_candidate_id, coalesce(rc.external_url, '')
		from selected_releases sr
		join release_candidates rc on rc.id = sr.release_candidate_id
		where sr.id = $1`, selectedReleaseID,
	).Scan(&libraryItemID, &releaseCandidateID, &externalURL); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Concurrent worker already deleted this selected_release — nothing to fail.
			err = nil
			return nil, nil
		}
		return nil, err
	}

	hardReject := isHardRejectReason(reason)
	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = false,
			rejected = case when $3 then true else rejected end,
			reject_reason = case when $3 then $2 else reject_reason end,
			failure_count = failure_count + 1,
			last_failure_reason = $2
		where id = $1`, releaseCandidateID, reason, hardReject,
	); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		insert into failed_releases (release_candidate_id, reason)
		values ($1, $2)
		on conflict do nothing`, releaseCandidateID, reason,
	); err != nil && !strings.Contains(err.Error(), "23503") {
		// 23503 = FK violation (candidate was deleted by concurrent worker); safe to skip
		return nil, err
	}
	if hardReject && strings.TrimSpace(externalURL) != "" {
		var title, indexerName string
		var sizeBytes int64
		var postedAt time.Time
		if err = tx.QueryRowContext(ctx, `
			select title, coalesce(indexer_name, ''), coalesce(size_bytes, 0), coalesce(posted_at, to_timestamp(0))
			from release_candidates
			where id = $1`, releaseCandidateID,
		).Scan(&title, &indexerName, &sizeBytes, &postedAt); err != nil {
			return nil, err
		}
		for _, key := range blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt) {
			if _, err = tx.ExecContext(ctx, `
				insert into blocklist_items (key, reason)
				values ($1, $2)
				on conflict (key)
				do update set reason = excluded.reason, expires_at = null`,
				key, reason,
			); err != nil {
				return nil, err
			}
		}
	}
	if err = preDeleteVFRBySelectedRelease(ctx, tx, selectedReleaseID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		delete from selected_releases
		where id = $1`, selectedReleaseID,
	); err != nil {
		return nil, err
	}

	var nextCandidateID sql.NullInt64
	if err = tx.QueryRowContext(ctx, `
		select id
		from release_candidates
		where library_item_id = $1
		  and rejected = false
		  and id <> $2
		order by failure_count asc,
		         score desc,
		         posted_at desc nulls last,
		         created_at asc,
		         id asc
		limit 1`, libraryItemID, releaseCandidateID,
	).Scan(&nextCandidateID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	if !nextCandidateID.Valid {
		if _, err = tx.ExecContext(ctx, `
			update queue_items
			set state = $2, failure_reason = $3, selected_release_id = null, updated_at = now()
			where library_item_id = $1`, libraryItemID, QueueFailed, reason,
		); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = true
		where id = $1`, nextCandidateID.Int64,
	); err != nil {
		return nil, err
	}

	var nextSelectedReleaseID int64
	if err = tx.QueryRowContext(ctx, `
		insert into selected_releases (library_item_id, release_candidate_id)
		values ($1, $2)
		returning id`, libraryItemID, nextCandidateID.Int64,
	).Scan(&nextSelectedReleaseID); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', selected_release_id = $3, updated_at = now()
		where library_item_id = $1`, libraryItemID, QueueSelected, nextSelectedReleaseID,
	); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}

	next, err := db.GetSelectedReleaseSummary(ctx, nextSelectedReleaseID)
	if err != nil {
		return nil, err
	}
	return &next, nil
}

func isHardRejectReason(reason string) bool {
	reason = strings.TrimSpace(strings.ToLower(reason))
	return strings.HasPrefix(reason, "archive_")
}

func shouldPersistBlocklistReason(reason string) bool {
	reason = strings.TrimSpace(strings.ToLower(reason))
	return strings.HasPrefix(reason, "archive_") || strings.HasPrefix(reason, "manual_")
}

func blocklistKeyForExternalURL(rawURL string) string {
	return "external_url:" + strings.TrimSpace(rawURL)
}

func blocklistReleaseSignatureKey(title, indexerName string, sizeBytes int64, postedAt time.Time) string {
	normalizedTitle := normalizeReleaseTitle(title)
	if normalizedTitle == "" {
		return ""
	}
	sizeBucket := "0"
	if sizeBytes > 0 {
		sizeBucket = fmt.Sprintf("%d", sizeBytes/(1024*1024))
	}
	dateBucket := "none"
	if !postedAt.IsZero() {
		dateBucket = postedAt.UTC().Format("2006-01-02")
	}
	indexerBucket := normalizeReleaseTitle(indexerName)
	return "release_signature:" + strings.Join([]string{
		normalizedTitle,
		indexerBucket,
		sizeBucket,
		dateBucket,
	}, "|")
}

func blocklistKeysForRelease(title, externalURL, indexerName string, sizeBytes int64, postedAt time.Time) []string {
	keys := make([]string, 0, 2)
	if strings.TrimSpace(externalURL) != "" {
		keys = append(keys, blocklistKeyForExternalURL(externalURL))
	}
	if signature := blocklistReleaseSignatureKey(title, indexerName, sizeBytes, postedAt); signature != "" {
		keys = append(keys, signature)
	}
	return keys
}

func blockedReleaseReason(blocked map[string]string, candidate SearchCandidateRecord) (string, bool) {
	for _, key := range blocklistKeysForRelease(candidate.Title, candidate.ExternalURL, candidate.IndexerName, candidate.SizeBytes, candidate.PostedAt) {
		if reason, ok := blocked[key]; ok {
			return reason, true
		}
	}
	return "", false
}

func normalizeReleaseTitle(value string) string {
	replacer := strings.NewReplacer(".", " ", "_", " ", "-", " ", "[", " ", "]", " ", "(", " ", ")", " ", "{", " ", "}", " ")
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(strings.TrimSpace(value)))), " ")
}

func loadBlocklistMap(ctx context.Context, tx *sql.Tx) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `
		select key, reason
		from blocklist_items
		where expires_at is null or expires_at > now()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var key, reason string
		if err := rows.Scan(&key, &reason); err != nil {
			return nil, err
		}
		out[key] = reason
	}
	return out, rows.Err()
}

func (db *DB) BlocklistQueueSelectedRelease(ctx context.Context, queueItemID int64, reason string) error {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var (
		selectedReleaseID sql.NullInt64
		title             string
		externalURL       string
		indexerName       string
		sizeBytes         int64
		postedAt          time.Time
	)
	if err = tx.QueryRowContext(ctx, `
		select q.selected_release_id, coalesce(rc.title, ''), coalesce(rc.external_url, ''), coalesce(rc.indexer_name, ''), coalesce(rc.size_bytes, 0), coalesce(rc.posted_at, to_timestamp(0))
		from queue_items q
		left join selected_releases sr on sr.id = q.selected_release_id
		left join release_candidates rc on rc.id = sr.release_candidate_id
		where q.id = $1`, queueItemID,
	).Scan(&selectedReleaseID, &title, &externalURL, &indexerName, &sizeBytes, &postedAt); err != nil {
		return err
	}

	if selectedReleaseID.Valid && strings.TrimSpace(externalURL) != "" {
		for _, key := range blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt) {
			if _, err = tx.ExecContext(ctx, `
				insert into blocklist_items (key, reason)
				values ($1, $2)
				on conflict (key)
				do update set reason = excluded.reason, expires_at = null`, key, reason,
			); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (db *DB) ClearQueueSelectedRelease(ctx context.Context, queueItemID int64) error {
	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var (
		selectedReleaseID sql.NullInt64
		libraryItemID     int64
	)
	if err = tx.QueryRowContext(ctx, `
		select library_item_id, selected_release_id
		from queue_items
		where id = $1`, queueItemID,
	).Scan(&libraryItemID, &selectedReleaseID); err != nil {
		return err
	}

	if selectedReleaseID.Valid {
		if err = preDeleteVFRBySelectedRelease(ctx, tx, selectedReleaseID.Int64); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `delete from selected_releases where id = $1`, selectedReleaseID.Int64); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `
		update queue_items
		set selected_release_id = null, updated_at = now()
		where id = $1`, queueItemID,
	); err != nil {
		return err
	}

	return tx.Commit()
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

// MetadataBackfillTarget is one library item needing metadata re-enrichment.
type MetadataBackfillTarget struct {
	LibraryItemID int64
	MediaType     string
	TMDBID        int64
	TVDBID        int64
	EpisodeTitle  string
}

// ListMetadataBackfillTargets returns library items whose movie/show rows are
// missing newly-added metadata columns (tagline, release_date, etc.).
// Deduplicated by TMDB ID so each show/movie is only returned once.
func (db *DB) ListMetadataBackfillTargets(ctx context.Context) ([]MetadataBackfillTarget, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select li.id, 'movie', coalesce(m.tmdb_id,0), 0, ''
		from library_items li
		join movies m on m.id = li.movie_id
		where m.tmdb_id > 0 and (m.tagline is null or m.release_date is null)
		group by li.id, m.tmdb_id
		union all
		select min(li.id), 'episode', coalesce(tv.tmdb_id,0), coalesce(tv.tvdb_id,0), coalesce(min(ep.title),'')
		from library_items li
		join episodes ep on ep.id = li.episode_id
		join tv_shows tv on tv.id = ep.tv_show_id
		where tv.tmdb_id > 0 and (tv.tagline is null or tv.first_air_date is null)
		group by tv.tmdb_id, tv.tvdb_id
		limit 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MetadataBackfillTarget
	for rows.Next() {
		var t MetadataBackfillTarget
		if err := rows.Scan(&t.LibraryItemID, &t.MediaType, &t.TMDBID, &t.TVDBID, &t.EpisodeTitle); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ShowWithMissingEpisodes describes a TV show that has episodes not yet in the library.
type ShowWithMissingEpisodes struct {
	TVShowID  int64
	TMDBID    int64
	ShowTitle string
}

// ListShowsWithMissingEpisodes returns TV shows that either have fewer episode
// records than TMDB reports, or have episodes with NULL air_date (so we can
// backfill air dates from TMDB on the next FillMissingEpisodes pass).
func (db *DB) ListShowsWithMissingEpisodes(ctx context.Context) ([]ShowWithMissingEpisodes, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select distinct
			tv.id,
			coalesce(tv.tmdb_id, 0),
			tv.title
		from tv_shows tv
		where tv.tmdb_id > 0
		  and tv.number_of_episodes > 0
		  and (
		      tv.number_of_episodes > (
		          select count(*)
		          from episodes e
		          where e.tv_show_id = tv.id
		            and e.season_number > 0
		            and e.episode_number > 0
		      )
		      or exists (
		          select 1 from episodes e
		          where e.tv_show_id = tv.id
		            and e.season_number > 0
		            and e.episode_number > 0
		            and e.air_date is null
		      )
		  )
		order by tv.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShowWithMissingEpisodes
	for rows.Next() {
		var s ShowWithMissingEpisodes
		if err := rows.Scan(&s.TVShowID, &s.TMDBID, &s.ShowTitle); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// EnsureEpisodeLibraryItem creates an episode record and a library_item for the
// given TV show + season + episode if they don't already exist, then queues the
// item for search. Returns (true, nil) when a new item was created.
func (db *DB) EnsureEpisodeLibraryItem(ctx context.Context, tvShowID int64, showTitle string, seasonNum, episodeNum int, episodeTitle, airDate string) (bool, error) {
	// Upsert the episode record — also store the air_date when provided.
	var episodeID int64
	var airDateVal interface{}
	if len(airDate) >= 10 {
		airDateVal = airDate[:10]
	}
	err := db.SQL.QueryRowContext(ctx, `
		INSERT INTO episodes (tv_show_id, season_number, episode_number, title, air_date)
		VALUES ($1, $2, $3, $4, $5::date)
		ON CONFLICT (tv_show_id, season_number, episode_number) DO UPDATE
		  SET title    = CASE WHEN excluded.title != '' THEN excluded.title ELSE episodes.title END,
		      air_date = CASE WHEN excluded.air_date IS NOT NULL THEN excluded.air_date ELSE episodes.air_date END
		RETURNING id`, tvShowID, seasonNum, episodeNum, episodeTitle, airDateVal).Scan(&episodeID)
	if err != nil {
		return false, err
	}

	// Check if a library item already exists for this episode.
	var existingID int64
	_ = db.SQL.QueryRowContext(ctx, `SELECT id FROM library_items WHERE episode_id = $1`, episodeID).Scan(&existingID)
	if existingID > 0 {
		return false, nil // already tracked
	}

	// Create the library_item.
	title := showTitle
	if seasonNum > 0 && episodeNum > 0 {
		title = fmt.Sprintf("%s S%02dE%02d", showTitle, seasonNum, episodeNum)
	}
	var libItemID int64
	err = db.SQL.QueryRowContext(ctx, `
		INSERT INTO library_items (media_type, episode_id, title)
		VALUES ('episode', $1, $2)
		ON CONFLICT (episode_id) WHERE episode_id IS NOT NULL DO NOTHING
		RETURNING id`, episodeID, title).Scan(&libItemID)
	if err != nil || libItemID == 0 {
		return false, err
	}

	// Queue it for search.
	ikey := fmt.Sprintf("tmdb-ep-%d-%d-%d", tvShowID, seasonNum, episodeNum)
	_, err = db.SQL.ExecContext(ctx, `
		INSERT INTO queue_items (library_item_id, state, idempotency_key)
		VALUES ($1, 'requested', $2)
		ON CONFLICT (idempotency_key) DO NOTHING`, libItemID, ikey)
	return err == nil, err
}

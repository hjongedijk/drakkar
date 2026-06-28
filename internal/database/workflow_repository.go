package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// blocklistCache is a short-lived in-process cache for loadBlocklistMap (O-10).
// The blocklist is read on every search but rarely changes; a 30-second TTL
// avoids full table scans per candidate batch while keeping data fresh.
var (
	blocklistCacheMu  sync.Mutex
	blocklistCached   map[string]string
	blocklistCachedAt time.Time
)

// preDeleteVFRBySelectedRelease was a bulk pre-delete of virtual_file_ranges rows
// before cascading selected_release deletes. The virtual_file_ranges table was
// removed by migration 000041 (segment data is now inline in nzb_files), so this
// function is now a no-op kept for call-site compatibility.
func preDeleteVFRBySelectedRelease(_ context.Context, _ *sql.Tx, _ int64) error {
	return nil
}

// preDeleteVFRByLibraryItem was a bulk pre-delete of virtual_file_ranges rows.
// The virtual_file_ranges table was removed by migration 000041; no-op.
func preDeleteVFRByLibraryItem(_ context.Context, _ *sql.Tx, _ int64) error {
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
			qp.id,
			coalesce(qp.name, ''),
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
		left join quality_profiles qp on qp.id = li.quality_profile_id
		order by mr.created_at desc, mr.id desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MediaRequestSummary
	for rows.Next() {
		var (
			item             MediaRequestSummary
			libraryItemID    sql.NullInt64
			qualityProfileID sql.NullInt64
			queueState       string
		)
		if err := rows.Scan(
			&item.ID,
			&item.ExternalID,
			&item.RequestType,
			&item.Title,
			&item.MediaType,
			&libraryItemID,
			&qualityProfileID,
			&item.QualityProfileName,
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
		if qualityProfileID.Valid {
			value := qualityProfileID.Int64
			item.QualityProfileID = &value
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
			select li.id from library_items li
			join movies m on m.id = li.movie_id
			where m.tmdb_id = $1
			limit 1`, tmdbID).Scan(&libraryItemID)
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
		on conflict (library_item_id) do update set
			state      = case when queue_items.state = 'failed' then 'requested' else queue_items.state end,
			updated_at = case when queue_items.state = 'failed' then now() else queue_items.updated_at end`,
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
	TMDBID              int64
	Title               string
	OriginalTitle       string
	Year                int
	ReleaseDate         string // "YYYY-MM-DD"
	IMDbID              string
	Overview            string
	Tagline             string
	Status              string // "Released", "In Production", etc.
	ContentRating       string // "PG-13", "R", etc.
	OriginalLanguage    string
	RuntimeMinutes      int
	PosterURL           string
	BackdropURL         string
	TrailerURL          string
	Genres              []string
	AlternativeTitles   []string
	ProductionCompanies []string
	Popularity          float64
	VoteAverage         float64
	VoteCount           int
	Budget              int64
	Revenue             int64
	CastJSON            []byte // JSON: [{name,character,profile_url}]
	RawTMDB             []byte // full /movie/:id TMDB JSON response
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
			select li.id from library_items li
			join episodes e on e.id = li.episode_id
			join tv_shows ts on ts.id = e.tv_show_id
			where ts.tvdb_id = $1 and e.season_number = $2 and e.episode_number = $3
			limit 1`, tvdbID, season, episode).Scan(&libraryItemID)
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
	var existingTmdbID int64
	err = tx.QueryRowContext(ctx, `select id, coalesce(tmdb_id, 0) from tv_shows where tvdb_id = $1`, tvdbID).Scan(&showID, &existingTmdbID)
	if err == nil && tmdbID > 0 && existingTmdbID > 0 && existingTmdbID != tmdbID {
		// tvdb_id found but belongs to a different show (tmdb_id mismatch) — look up by tmdb_id instead
		err = tx.QueryRowContext(ctx, `select id from tv_shows where tmdb_id = $1`, tmdbID).Scan(&showID)
		if errors.Is(err, sql.ErrNoRows) {
			err = tx.QueryRowContext(ctx, `
				insert into tv_shows (tmdb_id, title, release_year)
				values ($1, $2, $3)
				returning id`, tmdbID, show, year).Scan(&showID)
		}
	} else if errors.Is(err, sql.ErrNoRows) {
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
		on conflict (library_item_id) do update set
			state      = case when queue_items.state = 'failed' then 'requested' else queue_items.state end,
			updated_at = case when queue_items.state = 'failed' then now() else queue_items.updated_at end`,
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
	TMDBID              int64
	ShowTitle           string
	OriginalName        string
	Year                int
	FirstAirDate        string // "YYYY-MM-DD"
	LastAirDate         string // "YYYY-MM-DD"
	IMDbID              string
	Overview            string
	Tagline             string
	Status              string // "Returning Series", "Ended", etc.
	ContentRating       string // "TV-MA", "TV-14", etc.
	OriginalLanguage    string
	Network             string
	EpisodeRunTime      int
	NumberOfSeasons     int
	NumberOfEpisodes    int
	InProduction        bool
	PosterURL           string
	BackdropURL         string
	TrailerURL          string
	Genres              []string
	AlternativeTitles   []string
	ProductionCompanies []string
	Popularity          float64
	VoteAverage         float64
	VoteCount           int
	CastJSON            []byte // JSON: [{name,character,profile_url}]
	RawTMDB             []byte
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
			coalesce(m.alternative_titles, '{}') || coalesce(tv.alternative_titles, '{}'),
			coalesce(m.runtime_minutes, 0)
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
		&item.RuntimeMinutes,
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
		select item.library_item_id, item.media_type, coalesce(item.tv_show_id, 0), coalesce(item.season_number, 0), item.selected, coalesce(item.selected_release_id, 0), coalesce(item.external_url, ''), item.state, item.updated_at
		from (
			select distinct on (q.library_item_id)
				q.library_item_id,
				li.media_type,
				tv.id as tv_show_id,
				ep.season_number,
				(q.selected_release_id is not null and q.state in ($1, $3)) as selected,
				q.selected_release_id,
				rc.external_url,
				q.state,
				q.updated_at,
				q.created_at,
				q.id
			from queue_items q
			join library_items li on li.id = q.library_item_id
			left join episodes ep on ep.id = li.episode_id
			left join tv_shows tv on tv.id = ep.tv_show_id
			left join selected_releases sr on sr.id = q.selected_release_id
			left join release_candidates rc on rc.id = sr.release_candidate_id
			where li.available = false
			  and li.media_type in ('movie', 'episode', 'tv')
			  and (
			    -- Normal pending items: no release selected yet.
			    -- last_searched_at cooldown (1 h) mirrors Sonarr/Radarr LastSearchTime:
			    -- once searched, skip until the cooldown expires to avoid hammering Hydra2.
			    (q.selected_release_id is null and q.state in ($1, $2)
			     and (q.state != $2 or q.updated_at < now() - interval '2 hours')
			     and (q.last_searched_at is null or q.last_searched_at < now() - interval '1 hour'))
			    -- Resume items: release already selected, but queue item is still
			    -- in requested. These should be dispatched immediately so the
			    -- worker can continue fetch/import without waiting for retry pass.
			    or (q.state = $1 and q.selected_release_id is not null)
			    -- Stranded selected items: release chosen but download not yet started.
			    -- Submitted directly to the downloadDispatcher (bypassing BullMQ) so
			    -- a stalled BullMQ lock cannot block their progress.
			    or q.state = $3
			  )
			  -- Skip movies that haven't been released yet.
			  and not exists (
			      select 1 from movies m
			      where m.id = li.movie_id
			        and m.release_date is not null
			        and m.release_date > current_date
			  )
			  -- Skip TV episodes that haven't aired yet (air_date in the future).
			  -- NULL air_date = unknown, search anyway (mirrors Sonarr behaviour).
			  -- monitoring_mode='future' explicitly opts into pre-air searching.
			  and (
			      li.media_type != 'episode'
			      or ep.id is null
			      or ep.air_date is null
			      or ep.air_date <= current_date
			      or coalesce(tv.monitoring_mode, 'all') = 'future'
			  )
			  -- TV monitoring mode filter (applies only to episode items).
			  -- 'all'     → no extra filter (default)
			  -- 'future'  → only episodes airing in the future
			  -- 'missing' → only not-yet-available episodes (already covered by li.available=false)
			  -- 'recent'  → aired within 30 days
			  -- 'pilot'   → only S01E01
			  -- 'none'    → skip all episodes for this show
			  and (
			    li.media_type != 'episode'
			    or tv.id is null
			    or coalesce(tv.monitoring_mode, 'all') = 'all'
			    or (coalesce(tv.monitoring_mode, 'all') = 'missing')
			    or (coalesce(tv.monitoring_mode, 'all') = 'future'  and ep.air_date >= current_date)
			    or (coalesce(tv.monitoring_mode, 'all') = 'recent'  and ep.air_date >= current_date - interval '30 days')
			    or (coalesce(tv.monitoring_mode, 'all') = 'pilot'   and ep.season_number = 1 and ep.episode_number = 1)
			  )
			  and not (
			    li.media_type = 'episode'
			    and tv.id is not null
			    and coalesce(tv.monitoring_mode, 'all') = 'none'
			  )
			order by q.library_item_id,
			         (q.state = $3) desc,
			         q.updated_at asc,
			         q.created_at asc,
			         q.id asc
		) item
		order by item.selected desc, item.updated_at asc, item.created_at asc, item.id asc`, QueueRequested, QueueFailed, QueueSelected)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingLibrarySearchTarget
	for rows.Next() {
		var item PendingLibrarySearchTarget
		if err := rows.Scan(&item.LibraryItemID, &item.MediaType, &item.TVShowID, &item.SeasonNumber, &item.Selected, &item.SelectedReleaseID, &item.ExternalURL, &item.State, &item.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) CountActiveSearchBacklog(ctx context.Context) (int, error) {
	var count int
	err := db.SQL.QueryRowContext(ctx, `
		select count(distinct q.library_item_id)
		from queue_items q
		join library_items li on li.id = q.library_item_id
		where li.available = false
		  and li.media_type in ('movie', 'episode', 'tv')
		  and q.state in ($1, $2, $3, $4, $5, $6, $7, $8)`,
		QueueRequested,
		QueueSelected,
		QueueFetchingNZB,
		QueuePreflight,
		QueueSearching,
		QueueRanking,
		QueueIndexing,
		QueuePublishing,
	).Scan(&count)
	return count, err
}

func (db *DB) CountSelectedQueueBacklog(ctx context.Context) (int, error) {
	var count int
	err := db.SQL.QueryRowContext(ctx, `
		select count(distinct q.library_item_id)
		from queue_items q
		join library_items li on li.id = q.library_item_id
		where li.available = false
		  and li.media_type in ('movie', 'episode', 'tv')
		  and q.state = $1`,
		QueueSelected,
	).Scan(&count)
	return count, err
}

// ListFailedQueueRetryTargets returns failed items eligible for retry.
// limit caps the result set (0 = unlimited). The scheduled retry pass uses a
// small limit so each run completes within the timer interval; user-triggered
// bulk actions pass 0 to process all items.
func (db *DB) ListFailedQueueRetryTargets(ctx context.Context, limit int) ([]FailedQueueRetryTarget, error) {
	query := `
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
			end as failure_reason,
			q.selected_release_id is not null as has_selected_release,
			coalesce(rc.failure_count, 0) as candidate_failure_count
		from queue_items q
		join library_items li on li.id = q.library_item_id
		left join selected_releases sr on sr.id = q.selected_release_id
		left join release_candidates rc on rc.id = sr.release_candidate_id
		where li.available = false
		  and (
		    q.state = $1
		    or (q.state = $2 and q.selected_release_id is not null and q.updated_at < now() - interval '2 minutes')
		  )
		order by
			-- Prioritise restart/stale interruptions first: they are cheap (no Hydra
			-- call) and must not be pushed past the per-pass limit by older failures.
			case when q.failure_reason in ('interrupted_by_restart', 'stale_worker')
			          or (q.state = $2 and q.selected_release_id is not null)
			     then 0 else 1 end asc,
			q.updated_at asc, q.id asc`
	args := []any{QueueFailed, QueueRequested}
	if limit > 0 {
		query += ` LIMIT $3`
		args = append(args, limit)
	}
	rows, err := db.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FailedQueueRetryTarget
	for rows.Next() {
		var item FailedQueueRetryTarget
		if err := rows.Scan(&item.QueueItemID, &item.LibraryItemID, &item.FailureReason,
			&item.HasSelectedRelease, &item.CandidateFailureCount); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (db *DB) ListSelectedQueueRetryTargets(ctx context.Context, limit int) ([]SelectedQueueRetryTarget, error) {
	query := `
		select
			q.id,
			q.library_item_id,
			q.state
		from queue_items q
		join library_items li on li.id = q.library_item_id
		where li.available = false
		  and q.selected_release_id is not null
		  and (
		    q.state = $1
		    or q.state = $2
		    or (q.state = $3 and q.updated_at < now() - interval '2 minutes')
		  )
		order by
			-- selected items first: they have a release chosen and just need a download slot.
			-- failed second: need a retry but are ready to go.
			-- requested last: BullMQ is already handling these; we only fast-lane them here.
			case q.state
				when $3 then 0
				when $1 then 1
				else 2
			end,
			q.updated_at asc,
			q.id asc`
	args := []any{QueueFailed, QueueRequested, QueueSelected}
	if limit > 0 {
		query += ` LIMIT $4`
		args = append(args, limit)
	}
	rows, err := db.SQL.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SelectedQueueRetryTarget
	for rows.Next() {
		var item SelectedQueueRetryTarget
		if err := rows.Scan(&item.QueueItemID, &item.LibraryItemID, &item.State); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ListUpgradableLibraryItems returns library_item IDs that are available but
// whose quality profile has allow_upgrade=true and whose latest queue item is
// still in state 'available' (i.e. not already being re-downloaded).
func (db *DB) ListUpgradableLibraryItems(ctx context.Context) ([]int64, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select li.id
		from library_items li
		join quality_profiles qp on qp.id = coalesce(
			li.quality_profile_id,
			(select id from quality_profiles where is_default = true order by id limit 1)
		)
		join lateral (
			select state from queue_items
			where library_item_id = li.id
			order by created_at desc
			limit 1
		) q on true
		where li.available = true
		  and qp.allow_upgrade = true
		  and q.state = $1`, QueueAvailable,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
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
				library_item_id, title, score, custom_format_score, selected, rejected, reject_reason,
				failure_count, last_failure_reason, external_url, indexer_name, size_bytes, posted_at, resolution, explanations, compatibility_warnings
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
			returning id`,
			libraryItemID,
			candidate.Title,
			candidate.Score,
			candidate.CustomFormatScore,
			shouldSelect,
			candidate.Rejected,
			candidate.RejectReason,
			candidate.FailureCount,
			candidate.LastFailureReason,
			candidate.ExternalURL,
			candidate.IndexerName,
			candidate.SizeBytes,
			nullTime(candidate.PostedAt),
			candidate.Resolution,
			candidate.Explanations,
			func() []string {
				if candidate.CompatibilityWarnings == nil {
					return []string{}
				}
				return candidate.CompatibilityWarnings
			}(),
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
			if _, err = tx.ExecContext(ctx, `
				insert into grab_history (library_item_id, release_candidate_id, title, indexer_name, score, resolution)
				values ($1, $2, $3, $4, $5, $6)`,
				libraryItemID, releaseCandidateID,
				candidate.Title, candidate.IndexerName, candidate.Score, candidate.Resolution,
			); err != nil {
				return nil, err
			}
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

func (db *DB) GetGrabHistory(ctx context.Context, libraryItemID int64) ([]GrabHistoryEntry, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		SELECT id, library_item_id, release_candidate_id, title, indexer_name, score, resolution, grabbed_at
		FROM grab_history
		WHERE library_item_id = $1
		ORDER BY grabbed_at DESC
		LIMIT 50`, libraryItemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GrabHistoryEntry
	for rows.Next() {
		var e GrabHistoryEntry
		var rcID sql.NullInt64
		if err := rows.Scan(&e.ID, &e.LibraryItemID, &rcID, &e.Title, &e.IndexerName, &e.Score, &e.Resolution, &e.GrabbedAt); err != nil {
			return nil, err
		}
		if rcID.Valid {
			v := rcID.Int64
			e.ReleaseCandidateID = &v
		}
		out = append(out, e)
	}
	return out, rows.Err()
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
	if err != nil {
		return err
	}
	// Clean up any stale selected_releases rows so the item re-enters the normal
	// search cycle on the next pass rather than getting stuck re-attempting the
	// same failed release.
	_, err = db.SQL.ExecContext(ctx, `
		delete from selected_releases where library_item_id = $1`, libraryItemID,
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
	)
	if err != nil {
		return ReleaseSummary{}, err
	}
	if nzbDocument.Valid {
		value := nzbDocument.Int64
		item.NZBDocumentID = &value
	}
	item.Explanations = releaseSummaryExplanations(item)
	return item, nil
}

func (db *DB) GetLatestSelectedReleaseSummaryByLibraryItem(ctx context.Context, libraryItemID int64) (*ReleaseSummary, error) {
	var selectedReleaseID int64
	err := db.SQL.QueryRowContext(ctx, `
		select sr.id
		from selected_releases sr
		where sr.library_item_id = $1
		order by sr.id desc
		limit 1`, libraryItemID,
	).Scan(&selectedReleaseID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	item, err := db.GetSelectedReleaseSummary(ctx, selectedReleaseID)
	if err != nil {
		return nil, err
	}
	return &item, nil
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
	if item.XML, err = decompressNZBXML(item.XML); err != nil {
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
	// Cap at 90s: deleting nzb_documents and their cascaded nzb_files rows for
	// a large release can take significant time. 90s gives headroom without
	// hanging indefinitely on lock contention.
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	// Pre-load the blocklist map before opening the transaction. This is a
	// large read (1500+ rows) and doing it inside the transaction held write
	// locks on release_candidates, causing "driver: bad connection" when many
	// workers hit the same indexer group simultaneously.
	blocked, err := loadBlocklistMapUncached(ctx, db.SQL)
	if err != nil {
		return nil, fmt.Errorf("fail/blocklist-preload (sr=%d): %w", selectedReleaseID, err)
	}

	tx, err := db.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fail/begin (sr=%d): %w", selectedReleaseID, err)
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
		return nil, fmt.Errorf("fail/select-sr (sr=%d): %w", selectedReleaseID, err)
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
		return nil, fmt.Errorf("fail/update-rc (rc=%d): %w", releaseCandidateID, err)
	}
	if _, err = tx.ExecContext(ctx, `
		insert into failed_releases (release_candidate_id, reason)
		values ($1, $2)
		on conflict do nothing`, releaseCandidateID, reason,
	); err != nil && !isFKViolation(err) {
		// FK violation = candidate was deleted by concurrent worker; safe to skip
		return nil, fmt.Errorf("fail/insert-fr (rc=%d): %w", releaseCandidateID, err)
	}

	// Collect blocklist keys for ALL failures (matches Sonarr: blocklist on any
	// download failure). Hard rejects are permanent; soft failures get a 7-day TTL
	// so the same NZB isn't retried immediately but can resurface if re-uploaded.
	// Inserts happen after commit to avoid row-lock contention.
	var pendingBlocklistKeys []string
	pendingBlocklistTTL := 0 // 0 = permanent
	if !hardReject {
		pendingBlocklistTTL = 7 // soft failure: 7-day TTL
	}
	if strings.TrimSpace(externalURL) != "" {
		var title, indexerName string
		var sizeBytes int64
		var postedAt time.Time
		if err = tx.QueryRowContext(ctx, `
			select title, coalesce(indexer_name, ''), coalesce(size_bytes, 0), coalesce(posted_at, to_timestamp(0))
			from release_candidates
			where id = $1`, releaseCandidateID,
		).Scan(&title, &indexerName, &sizeBytes, &postedAt); err != nil {
			return nil, fmt.Errorf("fail/select-rc-title (rc=%d): %w", releaseCandidateID, err)
		}
		pendingBlocklistKeys = blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt)
	}
	if err = preDeleteVFRBySelectedRelease(ctx, tx, selectedReleaseID); err != nil {
		return nil, fmt.Errorf("fail/pre-delete-vfr (sr=%d): %w", selectedReleaseID, err)
	}
	if _, err = tx.ExecContext(ctx, `
		delete from selected_releases
		where id = $1`, selectedReleaseID,
	); err != nil {
		return nil, fmt.Errorf("fail/delete-sr (sr=%d): %w", selectedReleaseID, err)
	}

	// Collect blocked candidates and the next viable candidate in a single read
	// pass. rows must be closed before issuing any ExecContext on this
	// transaction — pgx holds a pgConn lock for the duration of the rows
	// resultReader, and ExecContext on the same connection returns
	// driver.ErrBadConn (connLockError.SafeToRetry = true) while that lock
	// is held.
	type blockedEntry struct {
		id     int64
		reason string
	}
	var (
		nextCandidateID sql.NullInt64
		blockedEntries  []blockedEntry
	)
	rows, err := tx.QueryContext(ctx, `
		select id, title, coalesce(external_url, ''), coalesce(indexer_name, ''), coalesce(size_bytes, 0), coalesce(posted_at, to_timestamp(0))
		from release_candidates
		where library_item_id = $1
		  and rejected = false
		  and id <> $2
		order by failure_count asc,
		         score desc,
		         posted_at desc nulls last,
		         created_at asc,
		         id asc`, libraryItemID, releaseCandidateID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var candidateID int64
		var candidate SearchCandidateRecord
		if err = rows.Scan(&candidateID, &candidate.Title, &candidate.ExternalURL, &candidate.IndexerName, &candidate.SizeBytes, &candidate.PostedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if blockedReason, isBlocked := blockedReleaseReason(blocked, candidate); isBlocked {
			blockedEntries = append(blockedEntries, blockedEntry{id: candidateID, reason: blockedReason})
			continue
		}
		nextCandidateID = sql.NullInt64{Int64: candidateID, Valid: true}
		break
	}
	// Close rows before any ExecContext to release the pgConn lock.
	if rowsErr := rows.Err(); rowsErr != nil {
		rows.Close()
		return nil, rowsErr
	}
	rows.Close()

	// Now safe to issue writes: mark blocked candidates as rejected.
	for _, be := range blockedEntries {
		if _, err = tx.ExecContext(ctx, `
			update release_candidates
			set rejected = true,
			    reject_reason = $2,
			    selected = false
			where id = $1`, be.id, be.reason,
		); err != nil {
			return nil, err
		}
	}

	if !nextCandidateID.Valid {
		if _, err = tx.ExecContext(ctx, `
			update queue_items
			set state = $2, failure_reason = $3, selected_release_id = null, updated_at = now()
			where library_item_id = $1`, libraryItemID, QueueFailed, reason,
		); err != nil {
			return nil, fmt.Errorf("fail/update-qi-failed (li=%d): %w", libraryItemID, err)
		}
		if err = tx.Commit(); err != nil {
			return nil, fmt.Errorf("fail/commit-no-next (li=%d): %w", libraryItemID, err)
		}
		db.flushBlocklistKeys(pendingBlocklistKeys, reason, pendingBlocklistTTL)
		return nil, nil
	}

	if _, err = tx.ExecContext(ctx, `
		update release_candidates
		set selected = true
		where id = $1`, nextCandidateID.Int64,
	); err != nil {
		return nil, fmt.Errorf("fail/update-rc-next (rc=%d): %w", nextCandidateID.Int64, err)
	}

	var nextSelectedReleaseID int64
	if err = tx.QueryRowContext(ctx, `
		insert into selected_releases (library_item_id, release_candidate_id)
		values ($1, $2)
		returning id`, libraryItemID, nextCandidateID.Int64,
	).Scan(&nextSelectedReleaseID); err != nil {
		return nil, fmt.Errorf("fail/insert-sr-next (li=%d): %w", libraryItemID, err)
	}
	if _, err = tx.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', selected_release_id = $3, updated_at = now()
		where library_item_id = $1`, libraryItemID, QueueSelected, nextSelectedReleaseID,
	); err != nil {
		return nil, fmt.Errorf("fail/update-qi-next (li=%d): %w", libraryItemID, err)
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("fail/commit-next (li=%d): %w", libraryItemID, err)
	}
	db.flushBlocklistKeys(pendingBlocklistKeys, reason, pendingBlocklistTTL)

	next, err := db.GetSelectedReleaseSummary(ctx, nextSelectedReleaseID)
	if err != nil {
		return nil, err
	}
	return &next, nil
}

// isFKViolation returns true when err is a PostgreSQL foreign-key constraint violation (SQLSTATE 23503).
func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.SQLState() == "23503"
}

func isHardRejectReason(reason string) bool {
	r := strings.TrimSpace(strings.ToLower(reason))
	if isRetryablePreflightReason(r) {
		return false
	}
	if isPermanentArchiveRejectReason(r) {
		return true
	}
	if strings.Contains(r, "invalid media payload") ||
		strings.Contains(r, "file header does not match") ||
		strings.Contains(r, "returned no readable bytes") ||
		strings.Contains(r, "all zero bytes") {
		return true
	}
	// Missing/expired articles are permanent — NNTP doesn't bring them back.
	if strings.Contains(r, "missing_articles") ||
		strings.Contains(r, "nntp_article_unavailable") ||
		strings.Contains(r, "article missing") ||
		strings.Contains(r, "article not found") ||
		strings.Contains(r, "crc mismatch") ||
		strings.Contains(r, "430") {
		return true
	}
	// Any NZB fetch HTTP error except 403 (quota exhausted — provider-level, not
	// URL-level) is treated as permanent for that specific URL.
	return strings.Contains(r, "nzb fetch status") && !strings.Contains(r, "status 403")
}

func shouldPersistBlocklistReason(reason string) bool {
	r := strings.TrimSpace(strings.ToLower(reason))
	if isRetryablePreflightReason(r) {
		return false
	}
	if isPermanentArchiveRejectReason(r) || strings.HasPrefix(r, "manual_") {
		return true
	}
	if strings.Contains(r, "invalid media payload") ||
		strings.Contains(r, "file header does not match") ||
		strings.Contains(r, "returned no readable bytes") ||
		strings.Contains(r, "all zero bytes") {
		return true
	}
	if strings.Contains(r, "article missing") ||
		strings.Contains(r, "article not found") ||
		strings.Contains(r, "crc mismatch") ||
		strings.Contains(r, "430") {
		return true
	}
	// Any NZB fetch HTTP error except 403 — blocklist the URL permanently.
	return strings.Contains(r, "nzb fetch status") && !strings.Contains(r, "status 403")
}

func isPermanentArchiveRejectReason(reason string) bool {
	switch strings.TrimSpace(strings.ToLower(reason)) {
	case "archive_encrypted", "archive_solid_unsupported", "archive_compression_unsupported":
		return true
	default:
		return false
	}
}

func isRetryablePreflightReason(reason string) bool {
	r := strings.TrimSpace(strings.ToLower(reason))
	if !(strings.HasPrefix(r, "early preflight:") ||
		strings.HasPrefix(r, "preflight:")) {
		return false
	}
	return strings.Contains(r, "article missing") ||
		strings.Contains(r, "article not found") ||
		strings.Contains(r, "430")
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

func blocklistReleaseFamilyKey(title string, sizeBytes int64, postedAt time.Time) string {
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
	return "release_family:" + strings.Join([]string{
		normalizedTitle,
		sizeBucket,
		dateBucket,
	}, "|")
}

func blocklistReleasePatternKey(title string, sizeBytes int64, postedAt time.Time) string {
	pattern := normalizeReleasePattern(title)
	if pattern == "" {
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
	return "release_pattern:" + strings.Join([]string{
		pattern,
		sizeBucket,
		dateBucket,
	}, "|")
}

func blocklistKeysForRelease(title, externalURL, indexerName string, sizeBytes int64, postedAt time.Time) []string {
	keys := make([]string, 0, 4)
	if strings.TrimSpace(externalURL) != "" {
		keys = append(keys, blocklistKeyForExternalURL(externalURL))
	}
	if signature := blocklistReleaseSignatureKey(title, indexerName, sizeBytes, postedAt); signature != "" {
		keys = append(keys, signature)
	}
	if pattern := blocklistReleasePatternKey(title, sizeBytes, postedAt); pattern != "" {
		keys = append(keys, pattern)
	}
	if family := blocklistReleaseFamilyKey(title, sizeBytes, postedAt); family != "" {
		keys = append(keys, family)
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

func normalizeReleasePattern(value string) string {
	tokens := strings.Fields(normalizeReleaseTitle(value))
	if len(tokens) == 0 {
		return ""
	}

	var out []string
	var sawEpisode bool
	var sawResolution bool
	var sawSource bool
	for _, token := range tokens {
		switch {
		case isSeasonEpisodeToken(token):
			out = append(out, token)
			sawEpisode = true
		case isResolutionToken(token):
			if !sawResolution {
				out = append(out, token)
				sawResolution = true
			}
		case isSourceToken(token):
			if !sawSource {
				out = append(out, canonicalSourceToken(token))
				sawSource = true
			}
		case !sawEpisode:
			out = append(out, token)
		}
	}
	if len(out) == 0 {
		return ""
	}
	return strings.Join(out, " ")
}

func isSeasonEpisodeToken(token string) bool {
	if len(token) >= 6 && token[0] == 's' {
		hasE := false
		for i := 1; i < len(token); i++ {
			if token[i] == 'e' {
				hasE = i > 1 && i < len(token)-1
				break
			}
		}
		if hasE {
			return true
		}
	}
	if strings.Count(token, "x") == 1 {
		parts := strings.Split(token, "x")
		return len(parts) == 2 && parts[0] != "" && parts[1] != ""
	}
	return false
}

func isResolutionToken(token string) bool {
	switch token {
	case "2160p", "1080p", "720p", "576p", "480p":
		return true
	default:
		return false
	}
}

func isSourceToken(token string) bool {
	switch token {
	case "bluray", "remux", "web", "webdl", "webrip", "hdtv", "bdrip", "dvdrip":
		return true
	default:
		return false
	}
}

func canonicalSourceToken(token string) string {
	switch token {
	case "web", "webdl", "webrip":
		return "web"
	case "bluray", "bdrip":
		return "bluray"
	default:
		return token
	}
}

func loadBlocklistMap(ctx context.Context, tx *sql.Tx) (map[string]string, error) {
	const cacheTTL = 30 * time.Second
	blocklistCacheMu.Lock()
	if blocklistCached != nil && time.Since(blocklistCachedAt) < cacheTTL {
		out := make(map[string]string, len(blocklistCached))
		for k, v := range blocklistCached {
			out[k] = v
		}
		blocklistCacheMu.Unlock()
		return out, nil
	}
	blocklistCacheMu.Unlock()

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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	cached := make(map[string]string, len(out))
	for k, v := range out {
		cached[k] = v
	}
	blocklistCacheMu.Lock()
	blocklistCached = cached
	blocklistCachedAt = time.Now()
	blocklistCacheMu.Unlock()

	return out, nil
}

// sqlQuerier is satisfied by both *sql.DB and *sql.Tx.
type sqlQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func loadBlocklistMapUncached(ctx context.Context, q sqlQuerier) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, `
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// flushBlocklistKeys inserts collected blocklist keys outside any transaction.
// ttlDays=0 means permanent (NULL expires_at); ttlDays>0 expires after that many days.
// Errors are swallowed: the item is already failed; a missing entry just means
// the release might surface again next cycle.
func (db *DB) flushBlocklistKeys(keys []string, reason string, ttlDays int) {
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		_, _ = db.SQL.ExecContext(context.Background(), `
			insert into blocklist_items (key, reason, expires_at)
			values ($1, $2, case when $3 > 0 then now() + ($3 * interval '1 day') else null end)
			on conflict (key)
			do update set reason = excluded.reason, expires_at = excluded.expires_at`,
			key, reason, ttlDays)
	}
	invalidateBlocklistCache()
}

func invalidateBlocklistCache() {
	blocklistCacheMu.Lock()
	blocklistCached = nil
	blocklistCachedAt = time.Time{}
	blocklistCacheMu.Unlock()
}

func (db *DB) BlocklistQueueSelectedRelease(ctx context.Context, queueItemID int64, reason string, ttlDays int) error {
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
		// Blocklist the specifically-selected release.
		for _, key := range blocklistKeysForRelease(title, externalURL, indexerName, sizeBytes, postedAt) {
			if _, err = tx.ExecContext(ctx, `
				insert into blocklist_items (key, reason, expires_at)
				values ($1, $2, case when $3 > 0 then now() + ($3 * interval '1 day') else null end)
				on conflict (key)
				do update set reason = excluded.reason, expires_at = excluded.expires_at`, key, reason, ttlDays,
			); err != nil {
				return err
			}
		}
	} else {
		// No selected release (e.g. all_candidates_wrong_title): blocklist all
		// rejected candidates for this library item that have a known URL so
		// they are not reconsidered in future search rounds.
		var libraryItemID int64
		if err = tx.QueryRowContext(ctx, `select library_item_id from queue_items where id = $1`, queueItemID).Scan(&libraryItemID); err != nil {
			return err
		}
		candRows, qErr := tx.QueryContext(ctx, `
			select coalesce(title,''), coalesce(external_url,''), coalesce(indexer_name,''), coalesce(size_bytes,0), coalesce(posted_at, to_timestamp(0))
			from release_candidates
			where library_item_id = $1
			  and coalesce(external_url,'') <> ''`, libraryItemID)
		if qErr != nil {
			err = qErr
			return err
		}
		// Collect keys during iteration; close rows before any ExecContext to
		// release the pgConn lock (connLockError.SafeToRetry=true → driver.ErrBadConn).
		var pendingKeys []string
		for candRows.Next() {
			var ct, cu, ci string
			var cs int64
			var cp time.Time
			if scanErr := candRows.Scan(&ct, &cu, &ci, &cs, &cp); scanErr != nil {
				candRows.Close()
				err = scanErr
				return err
			}
			pendingKeys = append(pendingKeys, blocklistKeysForRelease(ct, cu, ci, cs, cp)...)
		}
		if rowsErr := candRows.Err(); rowsErr != nil {
			candRows.Close()
			err = rowsErr
			return err
		}
		candRows.Close()
		for _, key := range pendingKeys {
			if _, err = tx.ExecContext(ctx, `
				insert into blocklist_items (key, reason, expires_at)
				values ($1, $2, case when $3 > 0 then now() + ($3 * interval '1 day') else null end)
				on conflict (key)
				do update set reason = excluded.reason, expires_at = excluded.expires_at`, key, reason, ttlDays,
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
		// Had a selected release: clear it and leave state as-is so the item
		// can be re-dispatched by the normal retry loop.
		if _, err = tx.ExecContext(ctx, `
			update queue_items
			set selected_release_id = null, updated_at = now()
			where id = $1`, queueItemID,
		); err != nil {
			return err
		}
	} else {
		// No selected release: reset to requested so the item leaves the
		// failed state and re-enters the normal search cycle instead of
		// looping forever in AutoManageFailedQueue.
		if _, err = tx.ExecContext(ctx, `
			update queue_items
			set state = $2, failure_reason = '', selected_release_id = null, updated_at = now()
			where id = $1`, queueItemID, QueueRequested,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (db *DB) RequeueSelectedRelease(ctx context.Context, queueItemID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		update queue_items
		set state = $2, failure_reason = '', updated_at = now()
		where id = $1
		  and selected_release_id is not null`,
		queueItemID, QueueRequested,
	)
	return err
}

// ResetLibraryItemState wipes the selected_release (and its cascading NZB data)
// for the library item's queue entry, resets the queue state back to 'requested',
// and marks the library item as unavailable so it re-enters the normal search cycle.
// It does NOT touch symlink_publications or the filesystem — the caller is responsible
// for removing symlinks before calling this.
func (db *DB) ResetLibraryItemState(ctx context.Context, libraryItemID int64) error {
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
		queueItemID       int64
		selectedReleaseID sql.NullInt64
	)
	if err = tx.QueryRowContext(ctx, `
		SELECT id, selected_release_id
		FROM queue_items
		WHERE library_item_id = $1
		ORDER BY id DESC
		LIMIT 1`, libraryItemID,
	).Scan(&queueItemID, &selectedReleaseID); err != nil {
		return err
	}

	if selectedReleaseID.Valid {
		if err = preDeleteVFRBySelectedRelease(ctx, tx, selectedReleaseID.Int64); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM selected_releases WHERE id = $1`, selectedReleaseID.Int64); err != nil {
			return err
		}
	}

	if _, err = tx.ExecContext(ctx, `
		UPDATE queue_items
		SET state = $2, failure_reason = '', selected_release_id = NULL, updated_at = now()
		WHERE id = $1`, queueItemID, QueueRequested,
	); err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, `
		UPDATE library_items SET available = false WHERE id = $1`, libraryItemID,
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

type MissingEpisodeBatchInput struct {
	SeasonNumber  int    `json:"season_number"`
	EpisodeNumber int    `json:"episode_number"`
	Title         string `json:"title"`
	AirDate       string `json:"air_date"`
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

func (db *DB) GetShowWithMissingEpisodes(ctx context.Context, tvShowID int64) (*ShowWithMissingEpisodes, error) {
	var s ShowWithMissingEpisodes
	err := db.SQL.QueryRowContext(ctx, `
		select tv.id, coalesce(tv.tmdb_id, 0), tv.title
		from tv_shows tv
		where tv.id = $1
		  and tv.tmdb_id > 0`, tvShowID,
	).Scan(&s.TVShowID, &s.TMDBID, &s.ShowTitle)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &s, nil
}

func (db *DB) ListPendingTVShowLibraryItemIDs(ctx context.Context, tvShowID int64) ([]int64, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select distinct li.id
		from library_items li
		join episodes ep on ep.id = li.episode_id
		join queue_items q on q.library_item_id = li.id
		where ep.tv_show_id = $1
		  and li.available = false
		  and q.state in ($2, $3, $4)
		order by li.id asc`,
		tvShowID,
		QueueRequested,
		QueueFailed,
		QueueSelected,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// EnsureEpisodeLibraryItemsBatch upserts episode rows for a season, inserts any
// missing library_items in bulk, queues them for search, and returns newly
// created library_item IDs for immediate work-queue dispatch.
func (db *DB) EnsureEpisodeLibraryItemsBatch(ctx context.Context, tvShowID int64, showTitle string, episodes []MissingEpisodeBatchInput) ([]int64, error) {
	if len(episodes) == 0 {
		return nil, nil
	}
	payload, err := json.Marshal(episodes)
	if err != nil {
		return nil, err
	}
	rows, err := db.SQL.QueryContext(ctx, `
		WITH input AS (
			SELECT
				x.season_number,
				x.episode_number,
				coalesce(x.title, '') AS title,
				CASE
					WHEN length(coalesce(x.air_date, '')) >= 10 THEN substring(x.air_date from 1 for 10)::date
					ELSE NULL
				END AS air_date
			FROM jsonb_to_recordset($3::jsonb) AS x(
				season_number integer,
				episode_number integer,
				title text,
				air_date text
			)
			WHERE x.episode_number > 0
		),
		upserted_episodes AS (
			INSERT INTO episodes (tv_show_id, season_number, episode_number, title, air_date)
			SELECT $1, i.season_number, i.episode_number, i.title, i.air_date
			FROM input i
			ON CONFLICT (tv_show_id, season_number, episode_number) DO UPDATE
			  SET title    = CASE WHEN excluded.title != '' THEN excluded.title ELSE episodes.title END,
			      air_date = CASE WHEN excluded.air_date IS NOT NULL THEN excluded.air_date ELSE episodes.air_date END
			RETURNING id
		),
		episode_rows AS (
			SELECT e.id, e.season_number, e.episode_number
			FROM episodes e
			JOIN input i
			  ON i.season_number = e.season_number
			 AND i.episode_number = e.episode_number
			WHERE e.tv_show_id = $1
		),
		inserted_library AS (
			INSERT INTO library_items (media_type, episode_id, title)
			SELECT
				'episode',
				er.id,
				format('%s S%02sE%02s', $2, er.season_number, er.episode_number)
			FROM episode_rows er
			ON CONFLICT (episode_id) WHERE episode_id IS NOT NULL DO NOTHING
			RETURNING id, episode_id
		),
		queued AS (
			INSERT INTO queue_items (library_item_id, state, idempotency_key)
			SELECT
				il.id,
				'requested',
				format('tmdb-ep-%s-%s-%s', $1::bigint, e.season_number, e.episode_number)
			FROM inserted_library il
			JOIN episodes e ON e.id = il.episode_id
			ON CONFLICT (library_item_id) DO UPDATE SET
				state      = CASE WHEN queue_items.state = 'failed' THEN 'requested' ELSE queue_items.state END,
				updated_at = CASE WHEN queue_items.state = 'failed' THEN now() ELSE queue_items.updated_at END
			RETURNING library_item_id
		)
		SELECT library_item_id
		FROM queued
		ORDER BY library_item_id ASC`, tvShowID, showTitle, payload)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var createdIDs []int64
	for rows.Next() {
		var libraryItemID int64
		if err := rows.Scan(&libraryItemID); err != nil {
			return nil, err
		}
		createdIDs = append(createdIDs, libraryItemID)
	}
	return createdIDs, rows.Err()
}

// EnsureEpisodeLibraryItem creates an episode record and a library_item for the
// given TV show + season + episode if they don't already exist, then queues the
// item for search. Returns (true, nil) when a new item was created.
func (db *DB) EnsureEpisodeLibraryItem(ctx context.Context, tvShowID int64, showTitle string, seasonNum, episodeNum int, episodeTitle, airDate string) (bool, error) {
	createdIDs, err := db.EnsureEpisodeLibraryItemsBatch(ctx, tvShowID, showTitle, []MissingEpisodeBatchInput{{
		SeasonNumber:  seasonNum,
		EpisodeNumber: episodeNum,
		Title:         episodeTitle,
		AirDate:       airDate,
	}})
	if err != nil {
		return false, err
	}
	return len(createdIDs) > 0, nil
}

// SetTVShowMonitoringMode updates the monitoring_mode column on a tv_show.
// Valid values: 'all', 'future', 'missing', 'recent', 'pilot', 'none'.
func (db *DB) SetTVShowMonitoringMode(ctx context.Context, tvShowID int64, mode string) error {
	_, err := db.SQL.ExecContext(ctx,
		`UPDATE tv_shows SET monitoring_mode = $1 WHERE id = $2`, mode, tvShowID)
	return err
}

// ListMovieTmdbIDs returns the TMDB ID for every tracked movie (tmdb_id > 0).
func (db *DB) ListMovieTmdbIDs(ctx context.Context) ([]int64, error) {
	rows, err := db.SQL.QueryContext(ctx, `select tmdb_id from movies where tmdb_id > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// TVShowSeerrInfo holds the data needed to push a TV show to Seerr.
type TVShowSeerrInfo struct {
	TMDBID  int64
	Seasons []int
}

// ListTVShowTmdbIDsWithSeasons returns each tracked TV show's TMDB ID and the
// distinct season numbers (>0) present in the episodes table.
func (db *DB) ListTVShowTmdbIDsWithSeasons(ctx context.Context) ([]TVShowSeerrInfo, error) {
	rows, err := db.SQL.QueryContext(ctx, `
		select distinct ts.tmdb_id, ep.season_number
		from tv_shows ts
		join episodes ep on ep.tv_show_id = ts.id
		where ts.tmdb_id > 0
		  and ep.season_number > 0
		order by ts.tmdb_id, ep.season_number`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seasonsByShow := make(map[int64][]int)
	var order []int64
	for rows.Next() {
		var tmdbID int64
		var season int
		if err := rows.Scan(&tmdbID, &season); err != nil {
			return nil, err
		}
		if _, seen := seasonsByShow[tmdbID]; !seen {
			order = append(order, tmdbID)
		}
		seasonsByShow[tmdbID] = append(seasonsByShow[tmdbID], season)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]TVShowSeerrInfo, 0, len(order))
	for _, tmdbID := range order {
		out = append(out, TVShowSeerrInfo{TMDBID: tmdbID, Seasons: seasonsByShow[tmdbID]})
	}
	return out, nil
}

// TouchQueueItemSearched records that a Hydra2 search was just attempted for
// this library item. Equivalent to Sonarr/Radarr's LastSearchTime per episode/movie:
// prevents the backlog scheduler from re-queuing the same item within the cooldown.
func (db *DB) TouchQueueItemSearched(ctx context.Context, libraryItemID int64) error {
	_, err := db.SQL.ExecContext(ctx, `
		UPDATE queue_items SET last_searched_at = now()
		WHERE library_item_id = $1 AND state NOT IN ('available', 'failed')`,
		libraryItemID)
	return err
}

-- Enriched metadata schema — aligns Drakkar's movies/tv_shows/seasons/episodes
-- tables with the reference project's normalized media schema.
-- All changes are additive (ALTER TABLE ADD COLUMN IF NOT EXISTS) so existing
-- data is preserved and the migration is safe to apply on a live database.

-- ─── movies ──────────────────────────────────────────────────────────────────

ALTER TABLE movies
    ADD COLUMN IF NOT EXISTS release_date        date,
    ADD COLUMN IF NOT EXISTS tagline             text,
    ADD COLUMN IF NOT EXISTS status              text,            -- "Released", "In Production", etc.
    ADD COLUMN IF NOT EXISTS content_rating      text,            -- "PG-13", "R", etc.
    ADD COLUMN IF NOT EXISTS vote_count          integer,
    ADD COLUMN IF NOT EXISTS budget              bigint,
    ADD COLUMN IF NOT EXISTS revenue             bigint,
    ADD COLUMN IF NOT EXISTS production_companies text[],
    ADD COLUMN IF NOT EXISTS cast_json           jsonb,           -- [{name, character, profile_url}]
    ADD COLUMN IF NOT EXISTS crew_json           jsonb,           -- [{name, job, department}]
    ADD COLUMN IF NOT EXISTS trailer_url         text,
    ADD COLUMN IF NOT EXISTS raw_seerr           jsonb,           -- full Seerr media payload
    ADD COLUMN IF NOT EXISTS raw_tmdb            jsonb;           -- full TMDB /movie/:id payload

-- Back-fill release_date from release_year where possible
UPDATE movies SET release_date = make_date(release_year, 1, 1)
WHERE release_date IS NULL AND release_year IS NOT NULL AND release_year > 1900;

-- ─── tv_shows ────────────────────────────────────────────────────────────────

ALTER TABLE tv_shows
    ADD COLUMN IF NOT EXISTS first_air_date      date,
    ADD COLUMN IF NOT EXISTS last_air_date       date,
    ADD COLUMN IF NOT EXISTS tagline             text,
    ADD COLUMN IF NOT EXISTS content_rating      text,            -- "TV-MA", "TV-14", etc.
    ADD COLUMN IF NOT EXISTS vote_average        numeric(4,2),
    ADD COLUMN IF NOT EXISTS vote_count          integer,
    ADD COLUMN IF NOT EXISTS in_production       boolean,
    ADD COLUMN IF NOT EXISTS production_companies text[],
    ADD COLUMN IF NOT EXISTS cast_json           jsonb,
    ADD COLUMN IF NOT EXISTS trailer_url         text,
    ADD COLUMN IF NOT EXISTS raw_seerr           jsonb,
    ADD COLUMN IF NOT EXISTS raw_tmdb            jsonb;

UPDATE tv_shows SET first_air_date = make_date(release_year, 1, 1)
WHERE first_air_date IS NULL AND release_year IS NOT NULL AND release_year > 1900;

-- ─── seasons (was nearly empty — add full metadata) ──────────────────────────

ALTER TABLE seasons
    ADD COLUMN IF NOT EXISTS tmdb_id             bigint,
    ADD COLUMN IF NOT EXISTS tvdb_id             bigint,
    ADD COLUMN IF NOT EXISTS title               text,
    ADD COLUMN IF NOT EXISTS overview            text,
    ADD COLUMN IF NOT EXISTS air_date            date,
    ADD COLUMN IF NOT EXISTS episode_count       integer,
    ADD COLUMN IF NOT EXISTS poster_url          text,
    ADD COLUMN IF NOT EXISTS raw_tmdb            jsonb;

CREATE UNIQUE INDEX IF NOT EXISTS seasons_tmdb_id_key ON seasons (tmdb_id) WHERE tmdb_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS seasons_tv_show_id_idx ON seasons (tv_show_id);

-- ─── episodes ─────────────────────────────────────────────────────────────────

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS imdb_id             text,
    ADD COLUMN IF NOT EXISTS season_id           bigint REFERENCES seasons(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS episode_title       text,            -- alternate/local title
    ADD COLUMN IF NOT EXISTS vote_count          integer,
    ADD COLUMN IF NOT EXISTS production_code     text,
    ADD COLUMN IF NOT EXISTS raw_tmdb            jsonb;

CREATE INDEX IF NOT EXISTS episodes_season_id_idx    ON episodes (season_id);
CREATE INDEX IF NOT EXISTS episodes_tv_show_id_s_e   ON episodes (tv_show_id, season_number, episode_number);
CREATE INDEX IF NOT EXISTS episodes_air_date_idx     ON episodes (air_date);

-- ─── performance indexes on new columns ──────────────────────────────────────

CREATE INDEX IF NOT EXISTS movies_release_date_idx   ON movies (release_date);
CREATE INDEX IF NOT EXISTS movies_status_idx         ON movies (status);
CREATE INDEX IF NOT EXISTS tv_shows_first_air_date_idx ON tv_shows (first_air_date);
CREATE INDEX IF NOT EXISTS tv_shows_status_idx       ON tv_shows (status);

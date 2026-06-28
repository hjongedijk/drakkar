-- Track when a pending queue item was last searched via Hydra2.
-- Equivalent to Sonarr/Radarr's LastSearchTime per episode/movie:
-- prevents re-searching the same item within the cooldown window.
alter table queue_items add column if not exists last_searched_at timestamptz;

create index if not exists idx_queue_items_last_searched_at
    on queue_items(last_searched_at) where last_searched_at is not null;

-- Index to speed up lateral join fetching the latest queue item per library item.
-- Used by movieCards, recentlyAdded, and LibraryDetail queries.
CREATE INDEX IF NOT EXISTS idx_queue_items_library_item_id
    ON queue_items (library_item_id, id DESC);

-- Add indexes on FK columns used in cascade DELETE paths.
-- Without these, cascading from selected_releases through nzb_documents →
-- nzb_files → nzb_segments → virtual_file_ranges requires sequential scans
-- of multi-million-row tables for every delete operation (minutes instead of ms).
CREATE INDEX IF NOT EXISTS idx_nzb_documents_selected_release_id ON nzb_documents(selected_release_id);
CREATE INDEX IF NOT EXISTS idx_nzb_files_nzb_document_id ON nzb_files(nzb_document_id);
CREATE INDEX IF NOT EXISTS idx_virtual_file_ranges_nzb_segment_id ON virtual_file_ranges(nzb_segment_id);
CREATE INDEX IF NOT EXISTS idx_virtual_file_ranges_virtual_file_id ON virtual_file_ranges(virtual_file_id);
CREATE INDEX IF NOT EXISTS idx_queue_items_selected_release_id ON queue_items(selected_release_id);
CREATE INDEX IF NOT EXISTS idx_archives_selected_release_id ON archives(selected_release_id);
CREATE INDEX IF NOT EXISTS idx_failed_releases_release_candidate_id ON failed_releases(release_candidate_id);
-- release_candidates.library_item_id: used in DELETE, SELECT and JOIN on every search cycle.
-- With 8.5M rows and no index, every search was doing a full table scan.
CREATE INDEX IF NOT EXISTS idx_release_candidates_library_item_id ON release_candidates(library_item_id);

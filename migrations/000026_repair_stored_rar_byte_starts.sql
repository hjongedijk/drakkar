-- Repair stored_rar VFR rows where migration 000025 failed to set segment_byte_start
-- because ae.path = vf.file_name didn't match for some releases. Falls back to
-- matching by file size, preferring exact path match via DISTINCT ON ordering.
-- Only touches rows where segment_byte_start is still 0 and the computed value > 0
-- (i.e. only the first segment of each RAR volume, where the header skip is needed).
WITH candidates AS (
    SELECT DISTINCT ON (vfr.id)
        vfr.id,
        ar.archive_offset + (vfr.range_start - ar.entry_offset) - ns.decoded_start_offset AS new_sbs
    FROM virtual_file_ranges vfr
    JOIN nzb_segments ns ON ns.id = vfr.nzb_segment_id
    JOIN virtual_files vf ON vf.id = vfr.virtual_file_id
    JOIN archives a ON a.selected_release_id = vf.selected_release_id
    JOIN archive_entries ae ON ae.archive_id = a.id
                            AND ae.size_bytes = vf.size_bytes
    JOIN archive_ranges ar ON ar.archive_entry_id = ae.id
    WHERE vf.reader_kind = 'stored_rar'
      AND vfr.segment_byte_start = 0
      AND vfr.range_start >= ar.entry_offset
      AND vfr.range_start < ar.entry_offset + ar.length_bytes
    ORDER BY vfr.id, (ae.path = vf.file_name) DESC
)
UPDATE virtual_file_ranges vfr
SET segment_byte_start = c.new_sbs
FROM candidates c
WHERE vfr.id = c.id
  AND c.new_sbs > 0;

-- Track which nzb_files have been successfully calibrated so startup
-- calibration can skip files that are already correct.
--
-- All existing files are marked as calibrated because prior runs have
-- already corrected any wrong yEnc offset estimates. Only newly imported
-- files (calibrated_at IS NULL) need calibration at next startup.
ALTER TABLE nzb_files ADD COLUMN IF NOT EXISTS calibrated_at timestamptz;
UPDATE nzb_files SET calibrated_at = now() WHERE calibrated_at IS NULL;

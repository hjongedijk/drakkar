package archive

import "github.com/hjongedijk/drakkar/internal/database"

// DetectImportedArchives groups NZB files by RAR archive membership.
// Delegates to database.DetectImportedArchives so callers outside the
// database package can use the same logic without an import cycle.
func DetectImportedArchives(files []database.ImportedNZBFile) []database.ImportedArchive {
	return database.DetectImportedArchives(files)
}

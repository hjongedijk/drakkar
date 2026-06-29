package database

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hjongedijk/drakkar/internal/par2"
	"github.com/hjongedijk/drakkar/internal/stream"
)

const (
	inspectHeaderLimit    = 256 * 1024
	inspectHeaderLimitMax = 64 * 1024 * 1024
)

var (
	errArchiveHeadersInvalid         = errors.New("archive_headers_invalid")
	errArchiveCompressionUnsupported = errors.New("archive_compression_unsupported")
	errArchiveSolidUnsupported       = errors.New("archive_solid_unsupported")
	errArchiveEncrypted              = errors.New("archive_encrypted")
	errArchiveVideoNotFound          = errors.New("archive_video_not_found")
	errNNTPArticleUnavailable        = errors.New("nntp_article_unavailable")
)

func inspectImportedArchives(ctx context.Context, archives []ImportedArchive, files []ImportedNZBFile, fetcher stream.SegmentFetcher) []ImportedArchive {
	if len(archives) == 0 {
		return nil
	}
	if fetcher == nil {
		for i := range archives {
			if archives[i].Status == "" {
				archives[i].Status = "pending"
			}
		}
		return archives
	}
	fileByName := make(map[string]ImportedNZBFile, len(files))
	for _, file := range files {
		fileByName[file.FileName] = file
	}
	// Fetch all par2 index files in parallel, then update fileByName.
	enrichFileByNameFromPar2(ctx, files, fileByName, fetcher)
	// Inspect archives in parallel (fileByName is read-only from here).
	out := make([]ImportedArchive, len(archives))
	const maxParallelInspections = 4
	sem := make(chan struct{}, maxParallelInspections)
	var wg sync.WaitGroup
	for i, item := range archives {
		i, item := i, item
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			inspected := item
			inspectCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			err := inspectArchive(inspectCtx, &inspected, fileByName, fetcher)
			cancel()
			if err != nil {
				inspected.Status = "rejected"
				inspected.RejectReason = err.Error()
			}
			out[i] = inspected
		}()
	}
	wg.Wait()
	return out
}

func inspectArchive(ctx context.Context, archive *ImportedArchive, fileByName map[string]ImportedNZBFile, fetcher stream.SegmentFetcher) error {
	if archive == nil {
		return nil
	}
	if !hasContiguousVolumes(archive.Volumes) {
		return errArchiveHeadersInvalid
	}
	if len(archive.Volumes) == 0 {
		return errArchiveHeadersInvalid
	}
	switch archive.Kind {
	case "rar":
		return inspectRARArchive(ctx, archive, fileByName, fetcher)
	case "7z":
		return inspect7zArchive(ctx, archive, fileByName, fetcher)
	default:
		return errArchiveHeadersInvalid
	}
}

func inspectRARArchive(ctx context.Context, archive *ImportedArchive, fileByName map[string]ImportedNZBFile, fetcher stream.SegmentFetcher) error {
	first, ok := fileByName[archive.Volumes[0].Path]
	if !ok {
		return errArchiveHeadersInvalid
	}
	volumeSizes := make(map[int]int64, len(archive.Volumes))
	for _, volume := range archive.Volumes {
		file, ok := fileByName[volume.Path]
		if !ok {
			return errArchiveHeadersInvalid
		}
		volumeSizes[volume.VolumeIndex] = importedFileEffectiveSize(ctx, file, fetcher)
	}

	entries, err := inspectRARWithRetries(ctx, first, fetcher)
	if err != nil {
		return err
	}

	// Keep the classic first-volume flow as a fallback for older scene archives
	// where continuation parts may not expose standalone file headers.
	volumeDataOffsets := fetchContinuationOffsets(ctx, archive.Volumes[1:], fileByName, fetcher)
	if len(archive.Volumes) > 1 {
		for i := range entries {
			e := &entries[i]
			if e.CompressionMethod == "m0" && e.SizeBytes > e.PackedSizeBytes {
				e.PackedSizeBytes = e.SizeBytes
			}
		}
	}
	legacyEntries := make([]ImportedArchiveEntry, len(entries))
	copy(legacyEntries, entries)
	assignArchiveRanges(legacyEntries, volumeSizes, volumeDataOffsets)

	var volumeEntries []ImportedArchiveEntry
	for i := range entries {
		entry := entries[i]
		entry.VolumeIndex = 0
		volumeEntries = append(volumeEntries, entry)
	}
	for _, volume := range archive.Volumes {
		if volume.VolumeIndex == 0 {
			continue
		}
		file := fileByName[volume.Path]
		partEntries, partErr := inspectRARWithRetries(ctx, file, fetcher)
		if partErr != nil {
			if errors.Is(partErr, errArchiveHeadersInvalid) || errors.Is(partErr, errArchiveVideoNotFound) {
				continue
			}
			return partErr
		}
		for i := range partEntries {
			partEntries[i].VolumeIndex = volume.VolumeIndex
		}
		volumeEntries = append(volumeEntries, partEntries...)
	}

	finalEntries := legacyEntries
	if len(volumeEntries) > len(entries) {
		aggregatedEntries, aggErr := aggregateRARVolumeEntries(volumeEntries, volumeSizes)
		if aggErr == nil {
			finalEntries = aggregatedEntries
		}
	}
	if err := validatePlayableArchiveEntries(finalEntries); err != nil {
		return err
	}
	archive.Entries = finalEntries
	archive.Status = "supported"
	archive.RejectReason = ""
	return nil
}

func inspectRARWithRetries(ctx context.Context, file ImportedNZBFile, fetcher stream.SegmentFetcher) ([]ImportedArchiveEntry, error) {
	available := importedFileEffectiveSize(ctx, file, fetcher)
	if available <= 0 {
		return nil, errArchiveHeadersInvalid
	}
	limits := []int64{
		inspectHeaderLimit,
		1024 * 1024,
		4 * 1024 * 1024,
		16 * 1024 * 1024,
		32 * 1024 * 1024,
		inspectHeaderLimitMax,
	}
	lastLimit := int64(0)
	for _, limit := range limits {
		if limit > available {
			limit = available
		}
		if limit <= 0 || limit == lastLimit {
			continue
		}
		lastLimit = limit
		prefix, err := readImportedFilePrefix(ctx, file, limit, fetcher)
		if err != nil {
			return nil, normalizeArchiveFetchError(err)
		}
		entries, err := inspectRARPrefix(prefix)
		if err == nil || !shouldRetryRARInspect(err) || limit == available {
			return entries, err
		}
	}
	return nil, errArchiveHeadersInvalid
}

func shouldRetryRARInspect(err error) bool {
	return errors.Is(err, errArchiveHeadersInvalid) || errors.Is(err, errArchiveVideoNotFound)
}

func normalizeArchiveFetchError(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(msg, "article missing") ||
		strings.Contains(msg, "article not found") ||
		strings.Contains(msg, "status 430") ||
		strings.Contains(msg, " 430") {
		return fmt.Errorf("%w: %v", errNNTPArticleUnavailable, err)
	}
	return err
}

func inspectRARPrefix(prefix []byte) ([]ImportedArchiveEntry, error) {
	if len(prefix) >= 8 && string(prefix[:8]) == "Rar!\x1a\x07\x01\x00" {
		return inspectRAR5(prefix)
	}
	return inspectRAR4(prefix)
}

// fetchContinuationOffsets fetches 512-byte prefixes from continuation volumes
// (vol 1, 2, …) in parallel and returns the byte offset where data starts in
// each volume. Volumes whose fetch fails are omitted (offset falls back to 0).
func fetchContinuationOffsets(ctx context.Context, volumes []ImportedArchiveVolume, fileByName map[string]ImportedNZBFile, fetcher stream.SegmentFetcher) map[int]int64 {
	if len(volumes) == 0 {
		return nil
	}
	type result struct {
		index     int
		dataStart int64
	}
	results := make(chan result, len(volumes))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, vol := range volumes {
		vol := vol
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			f, ok := fileByName[vol.Path]
			if !ok {
				results <- result{vol.VolumeIndex, 0}
				return
			}
			p, err := readImportedFilePrefix(ctx, f, 512, fetcher)
			if err != nil {
				results <- result{vol.VolumeIndex, 0}
				return
			}
			var start int64
			if len(p) >= 8 && string(p[:8]) == "Rar!\x1a\x07\x01\x00" {
				start, _ = rar5FindDataStart(p)
			} else {
				start, _ = rar4FindDataStart(p)
			}
			results <- result{vol.VolumeIndex, start}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	offsets := make(map[int]int64, len(volumes))
	for r := range results {
		if r.dataStart > 0 {
			offsets[r.index] = r.dataStart
		}
	}
	return offsets
}

// rar5FindDataStart parses a RAR5 volume prefix and returns the byte offset
// where the first file block's data area begins.
func rar5FindDataStart(raw []byte) (int64, error) {
	if len(raw) < 8 || string(raw[:8]) != "Rar!\x1a\x07\x01\x00" {
		return 0, errArchiveHeadersInvalid
	}
	pos := 8
	for pos+5 <= len(raw) {
		pos += 4 // skip CRC32
		headerSize, hsLen := rar5ReadVint(raw, pos)
		if hsLen == 0 || headerSize <= 0 {
			break
		}
		pos += hsLen
		bodyEnd := pos + int(headerSize)
		if bodyEnd > len(raw) {
			break
		}
		headType, n := rar5ReadVint(raw, pos)
		if n == 0 {
			break
		}
		pos += n
		headFlags, n := rar5ReadVint(raw, pos)
		if n == 0 {
			break
		}
		pos += n
		if headFlags&0x0001 != 0 {
			if _, n2 := rar5ReadVint(raw, pos); n2 == 0 {
				break
			} else {
				pos += n2
			}
		}
		var dataAreaSize int64
		if headFlags&0x0002 != 0 {
			v, n2 := rar5ReadVint(raw, pos)
			if n2 == 0 {
				break
			}
			dataAreaSize = v
			pos += n2
		}
		if headType == 2 {
			return int64(bodyEnd), nil
		}
		if headType == 5 {
			break
		}
		pos = bodyEnd + int(dataAreaSize)
	}
	return 0, errArchiveHeadersInvalid
}

// rar4FindDataStart parses a RAR4 volume prefix and returns the byte offset
// where the first file block's (type 0x74) data begins.
func rar4FindDataStart(raw []byte) (int64, error) {
	if len(raw) < 7 || string(raw[:7]) != "Rar!\x1a\x07\x00" {
		return 0, errArchiveHeadersInvalid
	}
	offset := 7
	for offset+7 <= len(raw) {
		headType := raw[offset+2]
		headFlags := binary.LittleEndian.Uint16(raw[offset+3 : offset+5])
		headSize := int(binary.LittleEndian.Uint16(raw[offset+5 : offset+7]))
		if headSize < 7 {
			break
		}
		if headType == 0x74 {
			return int64(offset + headSize), nil
		}
		if headType == 0x7b {
			break
		}
		var addSize int64
		if headFlags&0x8000 != 0 && offset+headSize+4 <= len(raw) {
			addSize = int64(binary.LittleEndian.Uint32(raw[offset+headSize:]))
		}
		next := offset + headSize + int(addSize)
		if next <= offset {
			break
		}
		offset = next
	}
	return 0, errArchiveHeadersInvalid
}

func aggregateRARVolumeEntries(parts []ImportedArchiveEntry, volumeSizes map[int]int64) ([]ImportedArchiveEntry, error) {
	if len(parts) == 0 {
		return nil, errArchiveHeadersInvalid
	}
	grouped := make(map[string][]ImportedArchiveEntry)
	for _, part := range parts {
		grouped[part.Path] = append(grouped[part.Path], part)
	}
	out := make([]ImportedArchiveEntry, 0, len(grouped))
	for _, groupedParts := range grouped {
		sort.SliceStable(groupedParts, func(i, j int) bool {
			if groupedParts[i].VolumeIndex != groupedParts[j].VolumeIndex {
				return groupedParts[i].VolumeIndex < groupedParts[j].VolumeIndex
			}
			return groupedParts[i].ArchiveOffset < groupedParts[j].ArchiveOffset
		})
		first := groupedParts[0]
		entry := ImportedArchiveEntry{
			Path:              first.Path,
			SizeBytes:         first.SizeBytes,
			CompressionMethod: first.CompressionMethod,
			Encrypted:         first.Encrypted,
			Solid:             first.Solid,
			VolumeIndex:       first.VolumeIndex,
			ArchiveOffset:     first.ArchiveOffset,
		}
		entryOffset := int64(0)
		for i, part := range groupedParts {
			if part.Path != entry.Path || part.CompressionMethod != entry.CompressionMethod {
				return nil, errArchiveHeadersInvalid
			}
			if part.Encrypted != entry.Encrypted || part.Solid != entry.Solid {
				return nil, errArchiveHeadersInvalid
			}
			if i > 0 && part.VolumeIndex <= groupedParts[i-1].VolumeIndex {
				return nil, errArchiveHeadersInvalid
			}
			if part.SizeBytes > entry.SizeBytes {
				entry.SizeBytes = part.SizeBytes
			}
			partPackedSize := part.PackedSizeBytes
			if partPackedSize <= 0 {
				return nil, errArchiveHeadersInvalid
			}
			if size, ok := volumeSizes[part.VolumeIndex]; ok {
				available := size - part.ArchiveOffset
				if available <= 0 {
					return nil, errArchiveHeadersInvalid
				}
				if partPackedSize > available {
					partPackedSize = available
				}
			}
			entry.Ranges = append(entry.Ranges, ImportedArchiveRange{
				VolumeIndex:   part.VolumeIndex,
				EntryOffset:   entryOffset,
				ArchiveOffset: part.ArchiveOffset,
				LengthBytes:   partPackedSize,
			})
			entryOffset += partPackedSize
			entry.PackedSizeBytes += partPackedSize
		}
		if entry.SizeBytes > 0 {
			diff := entry.SizeBytes - entry.PackedSizeBytes
			if diff < 0 {
				diff = -diff
			}
			if diff <= 16 && len(entry.Ranges) > 0 {
				entry.PackedSizeBytes = entry.SizeBytes
				last := &entry.Ranges[len(entry.Ranges)-1]
				last.LengthBytes += entry.SizeBytes - entryOffset
			}
		}
		out = append(out, entry)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return strings.ToLower(out[i].Path) < strings.ToLower(out[j].Path)
	})
	return out, nil
}

// isPar2IndexFile returns true for the primary .par2 index file.
// .vol*.par2 recovery files contain no FileDesc packets and are excluded.
func isPar2IndexFile(name string) bool {
	base := strings.ToLower(filepath.Base(strings.TrimSpace(name)))
	if !strings.HasSuffix(base, ".par2") {
		return false
	}
	stem := base[:len(base)-5]
	return !strings.Contains(stem, ".vol")
}

// enrichFileByNameFromPar2 fetches all par2 index files found in files in
// parallel, parses their FileDesc packets in-memory (no disk write), and
// updates fileByName with the authoritative FileLength for known filenames.
// For obfuscated NZBs where no exact name matches, a size-based alias is added
// so archive inspection can locate volumes under their real names.
func enrichFileByNameFromPar2(ctx context.Context, files []ImportedNZBFile, fileByName map[string]ImportedNZBFile, fetcher stream.SegmentFetcher) {
	var par2Files []ImportedNZBFile
	for _, file := range files {
		if isPar2IndexFile(file.FileName) {
			par2Files = append(par2Files, file)
		}
	}
	if len(par2Files) == 0 {
		return
	}
	// Fetch all par2 index files concurrently (typically 1-3 per release).
	allDescs := make([][]par2.FileDesc, len(par2Files))
	var wg sync.WaitGroup
	for i, file := range par2Files {
		i, file := i, file
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 512 KB covers all FileDesc packets in any realistic release.
			readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			data, err := readImportedFilePrefix(readCtx, file, 512*1024, fetcher)
			cancel()
			if err != nil {
				return
			}
			allDescs[i] = par2.ParseFileDescs(data)
		}()
	}
	wg.Wait()
	for _, descs := range allDescs {
		for _, d := range descs {
			if d.FileName == "" {
				continue
			}
			if entry, ok := fileByName[d.FileName]; ok {
				// Exact name match: use par2 FileLength as authoritative size.
				entry.FileSizeBytes = int64(d.FileLength)
				fileByName[d.FileName] = entry
				continue
			}
			// Size-based alias for obfuscated NZBs: find the NZB file whose
			// estimated size is within 5% of the par2 FileLength and hasn't
			// already been aliased, then expose it under the real filename.
			if d.FileLength == 0 {
				continue
			}
			fl := int64(d.FileLength)
			for _, f := range files {
				if _, exists := fileByName[d.FileName]; exists {
					break
				}
				diff := f.FileSizeBytes - fl
				if diff < 0 {
					diff = -diff
				}
				if diff*20 <= fl { // within 5%
					aliased := f
					aliased.FileName = d.FileName
					aliased.FileSizeBytes = fl
					fileByName[d.FileName] = aliased
				}
			}
		}
	}
}

func readImportedFilePrefix(ctx context.Context, file ImportedNZBFile, limit int64, fetcher stream.SegmentFetcher) ([]byte, error) {
	size := importedFileSegmentEnd(file)
	if file.FileSizeBytes > size {
		size = file.FileSizeBytes
	}
	if limit <= 0 || size <= 0 {
		return nil, errors.New("invalid archive size")
	}
	if limit > size {
		limit = size
	}
	spans := make([]stream.SegmentSpan, 0, len(file.Segments))
	for _, segment := range file.Segments {
		spans = append(spans, stream.SegmentSpan{
			MessageID: segment.MessageID,
			Start:     segment.DecodedStartOffset,
			End:       segment.DecodedEndOffset,
		})
	}
	ranges, err := stream.ResolveRange(spans, 0, limit)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, limit)
	for _, item := range ranges {
		block, err := fetcher.FetchRange(ctx, item)
		if err != nil {
			return nil, err
		}
		out = append(out, block...)
		if int64(len(out)) >= limit {
			return out[:limit], nil
		}
	}
	if int64(len(out)) < limit {
		return nil, errors.New("short archive header fetch")
	}
	return out[:limit], nil
}

func importedFileSegmentEnd(file ImportedNZBFile) int64 {
	var end int64
	for _, segment := range file.Segments {
		if segment.DecodedEndOffset > end {
			end = segment.DecodedEndOffset
		}
	}
	return end
}

func importedFileEffectiveSize(ctx context.Context, file ImportedNZBFile, fetcher stream.SegmentFetcher) int64 {
	size := importedFileSegmentEnd(file)
	if file.FileSizeBytes > size {
		size = file.FileSizeBytes
	}
	actual := importedFileActualSize(ctx, file, fetcher)
	if actual > size {
		size = actual
	}
	return size
}

// rar5ReadVint reads a RAR5 variable-length integer starting at pos.
// Returns (value, bytesRead); bytesRead==0 means insufficient data or overflow.
func rar5ReadVint(data []byte, pos int) (int64, int) {
	var result int64
	var shift uint
	for i := 0; i < 8; i++ {
		if pos+i >= len(data) {
			return 0, 0
		}
		b := data[pos+i]
		result |= int64(b&0x7F) << shift
		shift += 7
		if b&0x80 == 0 {
			return result, i + 1
		}
	}
	return 0, 0
}

func inspectRAR5(raw []byte) ([]ImportedArchiveEntry, error) {
	if len(raw) < 8 || string(raw[:8]) != "Rar!\x1a\x07\x01\x00" {
		return nil, errArchiveHeadersInvalid
	}
	pos := 8
	var entries []ImportedArchiveEntry
	playableFound := false

	for pos+5 <= len(raw) {
		pos += 4 // skip CRC32

		headerSize, hsLen := rar5ReadVint(raw, pos)
		if hsLen == 0 || headerSize <= 0 {
			break
		}
		pos += hsLen
		bodyStart := pos
		bodyEnd := bodyStart + int(headerSize)
		if bodyEnd > len(raw) {
			break
		}

		headType, n := rar5ReadVint(raw, pos)
		if n == 0 {
			break
		}
		pos += n

		headFlags, n := rar5ReadVint(raw, pos)
		if n == 0 {
			break
		}
		pos += n

		var extraAreaSize int64
		if headFlags&0x0001 != 0 {
			v, n2 := rar5ReadVint(raw, pos)
			if n2 == 0 {
				break
			}
			extraAreaSize = v
			pos += n2
		}

		var dataAreaSize int64
		if headFlags&0x0002 != 0 {
			v, n2 := rar5ReadVint(raw, pos)
			if n2 == 0 {
				break
			}
			dataAreaSize = v
			pos += n2
		}

		// dataAreaStart is the byte offset in this archive volume where packed
		// file data begins — immediately after the full header block.
		dataAreaStart := int64(bodyEnd)
		// type-specific fields occupy [pos, bodyEnd-extraAreaSize)
		typeEnd := bodyEnd - int(extraAreaSize)

		switch headType {
		case 4: // encryption header → whole archive is encrypted
			return nil, errArchiveEncrypted
		case 2: // file header
			entry, isDir, err := parseRAR5FileHeader(raw, pos, typeEnd, dataAreaStart, dataAreaSize)
			if err == nil && !isDir {
				entries = append(entries, entry)
				if isPlayableArchiveEntry(entry.Path) {
					playableFound = true
					if entry.Encrypted {
						return nil, errArchiveEncrypted
					}
					if entry.Solid {
						return nil, errArchiveSolidUnsupported
					}
					if entry.CompressionMethod != "m0" {
						return nil, errArchiveCompressionUnsupported
					}
				}
			}
		case 5: // end of archive
			goto done
		}

		pos = bodyEnd + int(dataAreaSize)
	}
done:
	if len(entries) == 0 {
		return nil, errArchiveHeadersInvalid
	}
	if !playableFound {
		return entries, errArchiveVideoNotFound
	}
	return entries, nil
}

func parseRAR5FileHeader(raw []byte, pos, end int, dataStart, dataAreaSize int64) (ImportedArchiveEntry, bool, error) {
	fileFlags, n := rar5ReadVint(raw, pos)
	if n == 0 || pos+n > end {
		return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
	}
	pos += n

	var unpackedSize int64
	if fileFlags&0x0008 == 0 { // size is known
		v, n2 := rar5ReadVint(raw, pos)
		if n2 == 0 || pos+n2 > end {
			return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
		}
		unpackedSize = v
		pos += n2
	}

	// attributes vint
	if _, n = rar5ReadVint(raw, pos); n == 0 || pos+n > end {
		return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
	}
	pos += n

	// optional mtime (uint32 LE)
	if fileFlags&0x0002 != 0 {
		if pos+4 > end {
			return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
		}
		pos += 4
	}

	// optional file CRC32 (uint32 LE)
	if fileFlags&0x0004 != 0 {
		if pos+4 > end {
			return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
		}
		pos += 4
	}

	// compression info vint
	// bits 0-5: version, bit 6: solid, bits 7-9: method (0=store)
	comprInfo, n := rar5ReadVint(raw, pos)
	if n == 0 || pos+n > end {
		return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
	}
	pos += n
	solid := comprInfo&0x40 != 0
	method := int((comprInfo >> 7) & 0x7)

	// host os vint
	if _, n = rar5ReadVint(raw, pos); n == 0 || pos+n > end {
		return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
	}
	pos += n

	// name: length vint + bytes
	nameLen, n := rar5ReadVint(raw, pos)
	if n == 0 || pos+n > end || int(nameLen) < 0 || pos+n+int(nameLen) > end {
		return ImportedArchiveEntry{}, false, errArchiveHeadersInvalid
	}
	pos += n
	name := string(raw[pos : pos+int(nameLen)])

	methodName := fmt.Sprintf("0x%02x", method)
	if method == 0 {
		methodName = "m0"
	}
	isDir := fileFlags&0x0001 != 0

	return ImportedArchiveEntry{
		Path:              filepath.Base(strings.ReplaceAll(name, `\`, "/")),
		SizeBytes:         unpackedSize,
		PackedSizeBytes:   dataAreaSize,
		CompressionMethod: methodName,
		Encrypted:         false, // archive-level encryption caught by type-4 block
		Solid:             solid,
		VolumeIndex:       0,
		ArchiveOffset:     dataStart,
	}, isDir, nil
}

func inspectRAR4(raw []byte) ([]ImportedArchiveEntry, error) {
	if len(raw) < 13 || string(raw[:7]) != "Rar!\x1a\x07\x00" {
		return nil, errArchiveHeadersInvalid
	}
	offset := 7
	var (
		mainFlags     uint16
		entries       []ImportedArchiveEntry
		playableFound bool
	)
	for offset+7 <= len(raw) {
		headType := raw[offset+2]
		headFlags := binary.LittleEndian.Uint16(raw[offset+3 : offset+5])
		headSize := int(binary.LittleEndian.Uint16(raw[offset+5 : offset+7]))
		if headSize < 7 || offset+headSize > len(raw) {
			return nil, errArchiveHeadersInvalid
		}
		body := raw[offset+7 : offset+headSize]
		switch headType {
		case 0x73:
			mainFlags = headFlags
		case 0x74:
			entry, packedSize, err := parseRAR4FileHeader(body, headFlags, mainFlags, int64(offset+headSize))
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
			if isPlayableArchiveEntry(entry.Path) {
				playableFound = true
				if entry.Encrypted {
					return nil, errArchiveEncrypted
				}
				if entry.Solid {
					return nil, errArchiveSolidUnsupported
				}
				if entry.CompressionMethod != "m0" {
					return nil, errArchiveCompressionUnsupported
				}
			}
			offset += headSize + int(packedSize) //nolint:gosec // packedSize fits int on all supported platforms
			continue
		case 0x7b:
			offset = len(raw)
			continue
		}
		offset += headSize
	}
	if len(entries) == 0 {
		return nil, errArchiveHeadersInvalid
	}
	if !playableFound {
		return entries, errArchiveVideoNotFound
	}
	return entries, nil
}

func parseRAR4FileHeader(body []byte, headFlags, mainFlags uint16, dataOffset int64) (ImportedArchiveEntry, int64, error) {
	if len(body) < 25 {
		return ImportedArchiveEntry{}, 0, errArchiveHeadersInvalid
	}
	packedSize := uint64(binary.LittleEndian.Uint32(body[0:4]))
	unpackedSize := uint64(binary.LittleEndian.Uint32(body[4:8]))
	method := body[18]
	nameSize := int(binary.LittleEndian.Uint16(body[19:21]))
	pos := 25
	if headFlags&0x0100 != 0 {
		if len(body) < pos+8 {
			return ImportedArchiveEntry{}, 0, errArchiveHeadersInvalid
		}
		highPacked := uint64(binary.LittleEndian.Uint32(body[pos : pos+4]))
		highUnpacked := uint64(binary.LittleEndian.Uint32(body[pos+4 : pos+8]))
		packedSize |= highPacked << 32
		unpackedSize |= highUnpacked << 32
		pos += 8
	}
	if len(body) < pos+nameSize {
		return ImportedArchiveEntry{}, 0, errArchiveHeadersInvalid
	}
	name := string(body[pos : pos+nameSize])
	return ImportedArchiveEntry{
		Path:              filepath.Base(strings.ReplaceAll(name, `\`, "/")),
		SizeBytes:         int64(unpackedSize),
		PackedSizeBytes:   int64(packedSize),
		CompressionMethod: rarMethodName(method),
		Encrypted:         headFlags&0x0004 != 0 || mainFlags&0x0080 != 0,
		Solid:             mainFlags&0x0008 != 0,
		VolumeIndex:       0,
		ArchiveOffset:     dataOffset,
	}, int64(packedSize), nil
}

func assignArchiveRanges(entries []ImportedArchiveEntry, volumeSizes map[int]int64, volumeDataOffsets map[int]int64) {
	for i := range entries {
		entry := &entries[i]
		if entry.PackedSizeBytes <= 0 || entry.VolumeIndex < 0 {
			continue
		}
		remaining := entry.PackedSizeBytes
		entryOffset := int64(0)
		volumeIndex := entry.VolumeIndex
		archiveOffset := entry.ArchiveOffset
		for remaining > 0 {
			// For continuation volumes use the fetched data-start offset.
			if volumeIndex != entry.VolumeIndex {
				if off, ok := volumeDataOffsets[volumeIndex]; ok {
					archiveOffset = off
				}
			}
			volumeSize, ok := volumeSizes[volumeIndex]
			if !ok || archiveOffset >= volumeSize {
				entry.Ranges = nil
				break
			}
			available := volumeSize - archiveOffset
			length := remaining
			if length > available {
				length = available
			}
			entry.Ranges = append(entry.Ranges, ImportedArchiveRange{
				VolumeIndex:   volumeIndex,
				EntryOffset:   entryOffset,
				ArchiveOffset: archiveOffset,
				LengthBytes:   length,
			})
			remaining -= length
			entryOffset += length
			volumeIndex++
			archiveOffset = 0
		}
		if remaining > 0 {
			entry.Ranges = nil
		}
	}
}

func validatePlayableArchiveEntries(entries []ImportedArchiveEntry) error {
	for _, entry := range entries {
		if !isPlayableArchiveEntry(entry.Path) {
			continue
		}
		if !hasCompleteArchiveMapping(entry) {
			return errArchiveHeadersInvalid
		}
	}
	return nil
}

func hasCompleteArchiveMapping(entry ImportedArchiveEntry) bool {
	if entry.PackedSizeBytes < 0 {
		return false
	}
	if entry.PackedSizeBytes == 0 {
		return len(entry.Ranges) == 0
	}
	if len(entry.Ranges) == 0 {
		return false
	}
	expectedOffset := int64(0)
	var total int64
	for _, item := range entry.Ranges {
		if item.EntryOffset != expectedOffset || item.LengthBytes <= 0 {
			return false
		}
		expectedOffset += item.LengthBytes
		total += item.LengthBytes
	}
	return total == entry.PackedSizeBytes
}

func rarMethodName(method byte) string {
	switch method {
	case 0x30:
		return "m0"
	case 0x31:
		return "m1"
	case 0x32:
		return "m2"
	case 0x33:
		return "m3"
	case 0x34:
		return "m4"
	case 0x35:
		return "m5"
	default:
		return fmt.Sprintf("0x%02x", method)
	}
}

func hasContiguousVolumes(volumes []ImportedArchiveVolume) bool {
	if len(volumes) == 0 {
		return false
	}
	for i, volume := range volumes {
		if volume.VolumeIndex != i {
			return false
		}
	}
	return true
}

func isPlayableArchiveEntry(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".mkv", ".mp4", ".avi":
		return true
	default:
		return false
	}
}

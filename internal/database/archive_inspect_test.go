package database

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/hjongedijk/drakkar/internal/stream"
)

type fetcherStub struct {
	data []byte
	err  error
}

func (f fetcherStub) FetchRange(ctx context.Context, segment stream.SegmentRange) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append([]byte(nil), f.data[segment.RangeStart:segment.RangeEnd]...), nil
}

func TestInspectImportedArchivesStoredRAR(t *testing.T) {
	raw := buildRAR4(false, false, 0x30, "Movie.mkv", 1024)
	files := []ImportedNZBFile{{
		FileName:      "Movie.part01.rar",
		FileSizeBytes: int64(len(raw)),
		Segments: []ImportedNZBSegment{{
			MessageID:          "<one@test>",
			DecodedStartOffset: 0,
			DecodedEndOffset:   int64(len(raw)),
		}},
	}}
	archives := inspectImportedArchives(context.Background(), []ImportedArchive{{
		Kind:   "rar",
		Status: "pending",
		Volumes: []ImportedArchiveVolume{
			{Path: "Movie.part01.rar", VolumeIndex: 0},
		},
	}}, files, fetcherStub{data: raw})
	if len(archives) != 1 {
		t.Fatalf("unexpected archives %+v", archives)
	}
	if archives[0].Status != "supported" || archives[0].RejectReason != "" {
		t.Fatalf("unexpected archive %+v", archives[0])
	}
	if len(archives[0].Entries) != 1 || archives[0].Entries[0].CompressionMethod != "m0" {
		t.Fatalf("unexpected entries %+v", archives[0].Entries)
	}
	if archives[0].Entries[0].PackedSizeBytes != 1024 || archives[0].Entries[0].VolumeIndex != 0 {
		t.Fatalf("unexpected entry source metadata %+v", archives[0].Entries[0])
	}
	if archives[0].Entries[0].ArchiveOffset <= 0 {
		t.Fatalf("expected positive archive offset, got %+v", archives[0].Entries[0])
	}
	if len(archives[0].Entries[0].Ranges) != 1 {
		t.Fatalf("unexpected ranges %+v", archives[0].Entries[0].Ranges)
	}
	if archives[0].Entries[0].Ranges[0].EntryOffset != 0 || archives[0].Entries[0].Ranges[0].LengthBytes != 1024 {
		t.Fatalf("unexpected first range %+v", archives[0].Entries[0].Ranges[0])
	}
}

func TestInspectImportedArchivesRejectsCompressedRAR(t *testing.T) {
	raw := buildRAR4(false, false, 0x33, "Movie.mkv", 1024)
	files := []ImportedNZBFile{{
		FileName:      "Movie.part01.rar",
		FileSizeBytes: int64(len(raw)),
		Segments: []ImportedNZBSegment{{
			MessageID:          "<one@test>",
			DecodedStartOffset: 0,
			DecodedEndOffset:   int64(len(raw)),
		}},
	}}
	archives := inspectImportedArchives(context.Background(), []ImportedArchive{{
		Kind: "rar",
		Volumes: []ImportedArchiveVolume{
			{Path: "Movie.part01.rar", VolumeIndex: 0},
		},
	}}, files, fetcherStub{data: raw})
	if archives[0].Status != "rejected" || archives[0].RejectReason != "archive_compression_unsupported" {
		t.Fatalf("unexpected archive %+v", archives[0])
	}
}

func TestInspectImportedArchivesRejectsInvalidHeaders(t *testing.T) {
	archives := inspectImportedArchives(context.Background(), []ImportedArchive{{
		Kind: "rar",
		Volumes: []ImportedArchiveVolume{
			{Path: "Movie.part01.rar", VolumeIndex: 0},
		},
	}}, []ImportedNZBFile{{
		FileName:      "Movie.part01.rar",
		FileSizeBytes: 16,
		Segments: []ImportedNZBSegment{{
			MessageID:          "<one@test>",
			DecodedStartOffset: 0,
			DecodedEndOffset:   16,
		}},
	}}, fetcherStub{data: []byte("not-rar-header!!")})
	if archives[0].Status != "rejected" || archives[0].RejectReason != "archive_headers_invalid" {
		t.Fatalf("unexpected archive %+v", archives[0])
	}
}

func TestInspectImportedArchivesRejectsIncompleteStoredMapping(t *testing.T) {
	raw := buildRAR4(false, false, 0x30, "Movie.mkv", 1024)
	archives := inspectImportedArchives(context.Background(), []ImportedArchive{{
		Kind: "rar",
		Volumes: []ImportedArchiveVolume{
			{Path: "Movie.part01.rar", VolumeIndex: 0},
		},
	}}, []ImportedNZBFile{{
		FileName:      "Movie.part01.rar",
		FileSizeBytes: 128,
		Segments: []ImportedNZBSegment{{
			MessageID:          "<one@test>",
			DecodedStartOffset: 0,
			DecodedEndOffset:   int64(len(raw)),
		}},
	}}, fetcherStub{data: raw})
	if archives[0].Status != "rejected" || archives[0].RejectReason != "archive_headers_invalid" {
		t.Fatalf("unexpected archive %+v", archives[0])
	}
}

func TestInspectImportedArchivesLeavesPendingWithoutFetcher(t *testing.T) {
	archives := inspectImportedArchives(context.Background(), []ImportedArchive{{
		Kind: "rar",
		Volumes: []ImportedArchiveVolume{
			{Path: "Movie.part01.rar", VolumeIndex: 0},
		},
	}}, nil, nil)
	if archives[0].Status != "pending" {
		t.Fatalf("unexpected archive %+v", archives[0])
	}
}

func TestReadImportedFilePrefixShortFetch(t *testing.T) {
	file := ImportedNZBFile{
		FileName:      "Movie.part01.rar",
		FileSizeBytes: 8,
		Segments: []ImportedNZBSegment{{
			MessageID:          "<one@test>",
			DecodedStartOffset: 0,
			DecodedEndOffset:   8,
		}},
	}
	_, err := readImportedFilePrefix(context.Background(), file, 8, fetcherStub{err: errors.New("boom")})
	if err == nil {
		t.Fatal("expected fetch error")
	}
}

func TestAssignArchiveRangesAcrossVolumes(t *testing.T) {
	entries := []ImportedArchiveEntry{{
		Path:            "Movie.mkv",
		PackedSizeBytes: 120,
		VolumeIndex:     0,
		ArchiveOffset:   80,
	}}
	assignArchiveRanges(entries, map[int]int64{
		0: 100,
		1: 150,
	}, nil)
	if len(entries[0].Ranges) != 2 {
		t.Fatalf("unexpected ranges %+v", entries[0].Ranges)
	}
	if entries[0].Ranges[0].LengthBytes != 20 || entries[0].Ranges[1].EntryOffset != 20 || entries[0].Ranges[1].LengthBytes != 100 {
		t.Fatalf("unexpected cross-volume mapping %+v", entries[0].Ranges)
	}
}

func TestHasCompleteArchiveMapping(t *testing.T) {
	if !hasCompleteArchiveMapping(ImportedArchiveEntry{
		PackedSizeBytes: 120,
		Ranges: []ImportedArchiveRange{
			{EntryOffset: 0, LengthBytes: 20},
			{EntryOffset: 20, LengthBytes: 100},
		},
	}) {
		t.Fatal("expected mapping to be complete")
	}
	if hasCompleteArchiveMapping(ImportedArchiveEntry{
		PackedSizeBytes: 120,
		Ranges: []ImportedArchiveRange{
			{EntryOffset: 0, LengthBytes: 20},
			{EntryOffset: 30, LengthBytes: 90},
		},
	}) {
		t.Fatal("expected mapping gap to be incomplete")
	}
}

func buildRAR4(solid bool, encrypted bool, method byte, name string, payloadSize uint32) []byte {
	raw := append([]byte{}, []byte("Rar!\x1a\x07\x00")...)
	mainFlags := uint16(0x0100)
	if solid {
		mainFlags |= 0x0008
	}
	if encrypted {
		mainFlags |= 0x0080
	}
	raw = append(raw, rarBlock(0x73, mainFlags, make([]byte, 6))...)
	body := make([]byte, 25+len(name))
	binary.LittleEndian.PutUint32(body[0:4], payloadSize)
	binary.LittleEndian.PutUint32(body[4:8], payloadSize)
	body[18] = method
	binary.LittleEndian.PutUint16(body[19:21], uint16(len(name)))
	copy(body[25:], []byte(name))
	fileFlags := uint16(0)
	if encrypted {
		fileFlags |= 0x0004
	}
	raw = append(raw, rarBlock(0x74, fileFlags, body)...)
	raw = append(raw, make([]byte, int(payloadSize))...)
	raw = append(raw, rarBlock(0x7b, 0, nil)...)
	return raw
}

func rarBlock(headType byte, flags uint16, body []byte) []byte {
	raw := make([]byte, 7+len(body))
	raw[2] = headType
	binary.LittleEndian.PutUint16(raw[3:5], flags)
	binary.LittleEndian.PutUint16(raw[5:7], uint16(len(raw)))
	copy(raw[7:], body)
	return raw
}

package database

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// zstdMagic is the first 4 bytes of every zstandard frame (0xFD2FB528 little-endian).
var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

var (
	nzbZstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	nzbZstdDecoder, _ = zstd.NewReader(nil)
)

// compressNZBXML compresses raw NZB XML with zstd. NZB files are repetitive XML
// so typical compression ratio is ~70-80%.
func compressNZBXML(src []byte) []byte {
	return nzbZstdEncoder.EncodeAll(src, make([]byte, 0, len(src)/4))
}

// decompressNZBXML decompresses zstd-compressed NZB XML. If the data does not
// start with the zstd magic number it is returned as-is — this provides
// transparent backward compatibility with existing uncompressed rows in the DB.
func decompressNZBXML(src []byte) ([]byte, error) {
	if len(src) < 4 || !bytes.Equal(src[:4], zstdMagic) {
		return src, nil
	}
	out, err := nzbZstdDecoder.DecodeAll(src, make([]byte, 0, len(src)*4))
	if err != nil {
		return nil, fmt.Errorf("decompress nzb xml: %w", err)
	}
	return out, nil
}

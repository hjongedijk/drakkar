package nzb

import (
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

type Document struct {
	XMLName xml.Name  `xml:"nzb"`
	Files   []NZBFile `xml:"file"`
}

type NZBFile struct {
	Subject  string       `xml:"subject,attr"`
	Poster   string       `xml:"poster,attr"`
	Date     int64        `xml:"date,attr"`
	Groups   []string     `xml:"groups>group"`
	Segments []NZBSegment `xml:"segments>segment"`
}

type NZBSegment struct {
	Number      int    `xml:"number,attr"`
	Bytes       int64  `xml:"bytes,attr"`
	MessageID   string `xml:",chardata"`
	DecodedFrom int64  `xml:"-"`
	DecodedTo   int64  `xml:"-"`
}

// newznabError represents the Newznab API error response format:
// <error code="100" description="Incorrect user credentials" />
type newznabError struct {
	XMLName     xml.Name `xml:"error"`
	Code        string   `xml:"code,attr"`
	Description string   `xml:"description,attr"`
}

func Parse(r io.Reader) (*Document, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("parse nzb xml: read body: %w", err)
	}
	var doc Document
	if err := xml.Unmarshal(data, &doc); err != nil {
		// Check if it's a Newznab API error response rather than an NZB.
		var apiErr newznabError
		if xmlErr := xml.Unmarshal(data, &apiErr); xmlErr == nil && apiErr.Code != "" {
			return nil, fmt.Errorf("indexer error %s: %s", apiErr.Code, apiErr.Description)
		}
		return nil, fmt.Errorf("parse nzb xml: %w", err)
	}
	for i := range doc.Files {
		sort.Slice(doc.Files[i].Segments, func(a, b int) bool {
			return doc.Files[i].Segments[a].Number < doc.Files[i].Segments[b].Number
		})
		var offset int64
		for j := range doc.Files[i].Segments {
			decodedSize := estimateDecodedSize(doc.Files[i].Segments[j].Bytes)
			doc.Files[i].Segments[j].DecodedFrom = offset
			doc.Files[i].Segments[j].DecodedTo = offset + decodedSize
			offset += decodedSize
			doc.Files[i].Segments[j].MessageID = strings.TrimSpace(doc.Files[i].Segments[j].MessageID)
		}
	}
	return &doc, nil
}

// estimateDecodedSize converts the NZB segment encoded byte count to an
// approximate decoded size. yEnc overhead is ~3% (line-breaks + escape chars),
// so the decoded payload is roughly 97% of the encoded article body.
// The actual size is confirmed by the yEnc =ypart header but is not available
// at parse time; the preflight step may update stored offsets after fetching.
func estimateDecodedSize(encoded int64) int64 {
	if encoded <= 0 {
		return 0
	}
	return int64(float64(encoded) * 0.97)
}

func SegmentNumbers(doc *Document) []int {
	var out []int
	for _, file := range doc.Files {
		for _, seg := range file.Segments {
			out = append(out, seg.Number)
		}
	}
	sort.Ints(out)
	return out
}

func ParseSubjectFilename(subject string) string {
	start := strings.Index(subject, "\"")
	end := strings.LastIndex(subject, "\"")
	if start >= 0 && end > start {
		return subject[start+1 : end]
	}
	fields := strings.Fields(subject)
	if len(fields) == 0 {
		return ""
	}
	return strings.Trim(fields[0], "\"")
}

func ToInt(value string) int {
	n, _ := strconv.Atoi(value)
	return n
}

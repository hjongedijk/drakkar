package nntp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/hjongedijk/drakkar/internal/metrics"
	"github.com/hjongedijk/drakkar/internal/stream"
	"github.com/hjongedijk/drakkar/internal/yenc"
)

type ArticleSource interface {
	Body(ctx context.Context, messageID string) ([]byte, error)
}

type PriorityArticleSource interface {
	ArticleSource
	BodyPriority(ctx context.Context, messageID string, priority stream.FetchPriority) ([]byte, error)
}

type SegmentFetcher struct {
	source DecodedArticleSource
}

type SpanAwareSegmentFetcher interface {
	FetchRange(ctx context.Context, segment stream.SegmentRange) ([]byte, error)
	FetchRangeInfo(ctx context.Context, segment stream.SegmentRange) ([]byte, stream.SegmentSpan, error)
}

func NewSegmentFetcher(source DecodedArticleSource) *SegmentFetcher {
	return &SegmentFetcher{source: source}
}

// DecodedArticle fetches and returns the full decoded article body.
func (f *SegmentFetcher) DecodedArticle(ctx context.Context, messageID string) ([]byte, error) {
	if f == nil || f.source == nil {
		return nil, errors.New("nntp source unavailable")
	}
	if ps, ok := f.source.(PriorityDecodedArticleSource); ok {
		return ps.DecodedBodyPriority(ctx, messageID, stream.PriorityBackground)
	}
	return f.source.DecodedBody(ctx, messageID)
}

// DecodedSize fetches the full decoded article and returns its byte length.
// This is used during calibration to determine the actual decoded size of a
// segment rather than relying on estimates from the NZB bytes attribute.
func (f *SegmentFetcher) DecodedSize(ctx context.Context, messageID string) (int64, error) {
	decoded, err := f.DecodedArticle(ctx, messageID)
	if err != nil {
		return 0, err
	}
	return int64(len(decoded)), nil
}

// Exists verifies that the article exists without forcing a full decoded-body
// download when the underlying source supports NNTP STAT.
func (f *SegmentFetcher) Exists(ctx context.Context, messageID string) error {
	if f == nil || f.source == nil {
		return errors.New("nntp source unavailable")
	}
	if statSource, ok := f.source.(interface {
		Stat(context.Context, string) error
	}); ok {
		return statSource.Stat(ctx, messageID)
	}
	_, err := f.DecodedSize(ctx, messageID)
	return err
}

func (f *SegmentFetcher) FetchRange(ctx context.Context, segment stream.SegmentRange) ([]byte, error) {
	return f.FetchRangePriority(ctx, segment, stream.PriorityInteractive)
}

func (f *SegmentFetcher) FetchRangePriority(ctx context.Context, segment stream.SegmentRange, priority stream.FetchPriority) ([]byte, error) {
	block, _, err := f.FetchRangeInfoPriority(ctx, segment, priority)
	return block, err
}

func (f *SegmentFetcher) FetchRangeInfo(ctx context.Context, segment stream.SegmentRange) ([]byte, stream.SegmentSpan, error) {
	return f.FetchRangeInfoPriority(ctx, segment, stream.PriorityInteractive)
}

func (f *SegmentFetcher) FetchRangeInfoPriority(ctx context.Context, segment stream.SegmentRange, priority stream.FetchPriority) ([]byte, stream.SegmentSpan, error) {
	if f == nil || f.source == nil {
		return nil, stream.SegmentSpan{}, errors.New("nntp source unavailable")
	}
	decodedSource, hasPriorityDecoded := f.source.(PriorityDecodedArticleSource)
	infoSource, infoOK := f.source.(PriorityDecodedArticleInfoSource)
	var (
		decoded []byte
		info    yenc.PartInfo
		err     error
	)
	if infoOK {
		decoded, info, err = infoSource.DecodedBodyInfoPriority(ctx, segment.MessageID, priority)
	} else if basicInfo, ok := f.source.(DecodedArticleInfoSource); ok {
		decoded, info, err = basicInfo.DecodedBodyInfo(ctx, segment.MessageID)
	} else if hasPriorityDecoded {
		decoded, err = decodedSource.DecodedBodyPriority(ctx, segment.MessageID, priority)
	} else {
		decoded, err = f.source.DecodedBody(ctx, segment.MessageID)
	}
	if err != nil {
		metrics.M.NNTPFetchFailures.Add(1)
		// context.Canceled is normal: parallel connections race to fetch the
		// same segment; the losers are cancelled after the winner succeeds.
		// Log those at DEBUG to avoid flooding the console.
		if errors.Is(err, context.Canceled) {
			slog.Debug("nntp fetch canceled", "messageID", segment.MessageID)
		} else {
			slog.Warn("nntp fetch failed", "messageID", segment.MessageID, "err", err)
		}
		return nil, stream.SegmentSpan{}, fmt.Errorf("fetch decoded article %s: %w", segment.MessageID, err)
	}
	metrics.M.NNTPArticlesFetched.Add(1)
	metrics.M.NNTPBytesFetched.Add(int64(len(decoded)))
	actualStart := segment.SegmentStart
	actualEnd := segment.SegmentEnd
	if info.Valid() {
		actualStart = info.DecodedStart()
		actualEnd = actualStart + int64(len(decoded))
	}
	start := int(segment.RangeStart - actualStart)
	end := int(segment.RangeEnd - actualStart)
	if end < start {
		return nil, stream.SegmentSpan{SegmentID: segment.SegmentID, MessageID: segment.MessageID, Start: actualStart, End: actualEnd}, errors.New("invalid segment range")
	}
	// start < 0 means the actual decoded content of this segment begins AFTER
	// our estimated RangeStart. The span table is stale — return empty so
	// realignSpans + retry in DirectNzbReader can find the correct segment.
	if start < 0 {
		return []byte{}, stream.SegmentSpan{SegmentID: segment.SegmentID, MessageID: segment.MessageID, Start: actualStart, End: actualEnd}, nil
	}
	// Clamp end to actual decoded size. Estimated offsets may be ~0.15% too large
	// (yEnc decode ratio varies per article). We return what exists rather than
	// failing so callers can gracefully handle the boundary.
	if end > len(decoded) {
		end = len(decoded)
	}
	if start >= end {
		return []byte{}, stream.SegmentSpan{SegmentID: segment.SegmentID, MessageID: segment.MessageID, Start: actualStart, End: actualEnd}, nil
	}
	out := make([]byte, end-start)
	copy(out, decoded[start:end])
	return out, stream.SegmentSpan{SegmentID: segment.SegmentID, MessageID: segment.MessageID, Start: actualStart, End: actualEnd}, nil
}

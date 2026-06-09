package nntp

import (
	"context"
	"errors"
	"sync"

	"github.com/hjongedijk/drakkar/internal/cache"
	"github.com/hjongedijk/drakkar/internal/metrics"
	"github.com/hjongedijk/drakkar/internal/stream"
	"github.com/hjongedijk/drakkar/internal/yenc"
)

type DiskCachedDecodedSource struct {
	source       ArticleSource
	cache        *cache.FileCache
	singleflight *cache.SingleFlight
	infoMu       sync.Mutex
	partInfo     map[string]yenc.PartInfo
}

func NewDiskCachedDecodedSource(source ArticleSource, root string, maxBytes int64) *DiskCachedDecodedSource {
	return &DiskCachedDecodedSource{
		source:       source,
		cache:        cache.NewFileCache(root, maxBytes),
		singleflight: cache.NewSingleFlight(),
		partInfo:     make(map[string]yenc.PartInfo),
	}
}

func (s *DiskCachedDecodedSource) DecodedBody(ctx context.Context, messageID string) ([]byte, error) {
	return s.DecodedBodyPriority(ctx, messageID, stream.PriorityInteractive)
}

func (s *DiskCachedDecodedSource) DecodedBodyPriority(ctx context.Context, messageID string, priority stream.FetchPriority) ([]byte, error) {
	body, _, err := s.DecodedBodyInfoPriority(ctx, messageID, priority)
	return body, err
}

func (s *DiskCachedDecodedSource) DecodedBodyInfo(ctx context.Context, messageID string) ([]byte, yenc.PartInfo, error) {
	return s.DecodedBodyInfoPriority(ctx, messageID, stream.PriorityInteractive)
}

func (s *DiskCachedDecodedSource) DecodedBodyInfoPriority(ctx context.Context, messageID string, priority stream.FetchPriority) ([]byte, yenc.PartInfo, error) {
	if s == nil || s.source == nil {
		return nil, yenc.PartInfo{}, errors.New("disk cached source unavailable")
	}
	if body, ok, err := s.cache.Get(messageID); err == nil && ok {
		metrics.M.CacheHits.Add(1)
		if info, ok := s.lookupPartInfo(messageID); ok {
			return body, info, nil
		}
		return s.fillPartInfoFromRaw(ctx, messageID, priority, body)
	} else if err != nil {
		return nil, yenc.PartInfo{}, err
	}
	metrics.M.CacheMisses.Add(1)
	value, err := s.singleflight.Do(ctx, messageID, func(ctx context.Context) ([]byte, error) {
		if body, ok, err := s.cache.Get(messageID); err == nil && ok {
			metrics.M.CacheHits.Add(1)
			return body, nil
		} else if err != nil {
			return nil, err
		}
		var (
			raw []byte
			err error
		)
		if prioritySource, ok := s.source.(PriorityArticleSource); ok {
			raw, err = prioritySource.BodyPriority(ctx, messageID, priority)
		} else {
			raw, err = s.source.Body(ctx, messageID)
		}
		if err != nil {
			return nil, err
		}
		info, _ := yenc.ParsePartInfo(raw)
		decoded, err := yenc.DecodeArticle(raw)
		if err != nil {
			return nil, err
		}
		_ = s.cache.Put(messageID, decoded) // no-op when disk cache root is empty
		s.storePartInfo(messageID, info)
		return decoded, nil
	})
	if err != nil {
		return nil, yenc.PartInfo{}, err
	}
	if info, ok := s.lookupPartInfo(messageID); ok {
		return value, info, nil
	}
	return s.fillPartInfoFromRaw(ctx, messageID, priority, value)
}

func (s *DiskCachedDecodedSource) fillPartInfoFromRaw(ctx context.Context, messageID string, priority stream.FetchPriority, decoded []byte) ([]byte, yenc.PartInfo, error) {
	raw, err := s.fetchRaw(ctx, messageID, priority)
	if err != nil {
		return decoded, yenc.PartInfo{}, nil
	}
	info, _ := yenc.ParsePartInfo(raw)
	s.storePartInfo(messageID, info)
	return decoded, info, nil
}

func (s *DiskCachedDecodedSource) fetchRaw(ctx context.Context, messageID string, priority stream.FetchPriority) ([]byte, error) {
	if prioritySource, ok := s.source.(PriorityArticleSource); ok {
		return prioritySource.BodyPriority(ctx, messageID, priority)
	}
	return s.source.Body(ctx, messageID)
}

func (s *DiskCachedDecodedSource) lookupPartInfo(messageID string) (yenc.PartInfo, bool) {
	s.infoMu.Lock()
	defer s.infoMu.Unlock()
	info, ok := s.partInfo[messageID]
	return info, ok
}

func (s *DiskCachedDecodedSource) storePartInfo(messageID string, info yenc.PartInfo) {
	s.infoMu.Lock()
	defer s.infoMu.Unlock()
	s.partInfo[messageID] = info
}

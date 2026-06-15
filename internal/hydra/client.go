package hydra

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/hjongedijk/drakkar/internal/config"
)

// defaultSearchInterval is the minimum gap between consecutive Hydra API calls.
// This serialises concurrent BullMQ worker goroutines through a single queue,
// preventing simultaneous episode searches from flooding indexers.
var defaultSearchInterval = 2 * time.Second

var ErrRateLimited = errors.New("nzbhydra2 rate limited")

var (
	movieCategories = []string{"2030", "2040", "2045", "2050", "2060"}
	tvCategories    = []string{"5030", "5040", "5045", "5080"}
	rateLimitBackoff = []time.Duration{
		15 * time.Minute,
		30 * time.Minute,
		60 * time.Minute,
		3 * time.Hour,
		6 * time.Hour,
		12 * time.Hour,
		24 * time.Hour,
	}
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client

	searchInterval time.Duration // 0 = no throttle (Sonarr/Radarr behaviour)

	rateMu        sync.Mutex
	lastCall      time.Time
	cooldownUntil time.Time
	rateLimitHits int

	cacheMu        sync.Mutex
	searchCacheTTL time.Duration
	feedCacheTTL   time.Duration
	feedMaxResults int
	searchCache    map[string]cachedResults
	feedCache      map[string]cachedResults
}

type cachedResults struct {
	results   []SearchResult
	expiresAt time.Time
}

type SearchResult struct {
	Title        string
	Link         string
	Indexer      string
	SizeBytes    int64
	PublishedAt  time.Time
	Grabs        int
	IndexerScore int
	Passworded   bool
}

type SearchRequest struct {
	MediaType     string
	Query         string
	IMDbID        string
	TMDBID        int64  // tmdbid= parameter (Radarr/Sonarr first-tier ID search)
	TVDBID        int64
	SeasonNumber  int
	EpisodeNumber int
}

func NewClient(cfg config.ServiceConfig) *Client {
	searchCacheTTL := time.Duration(cfg.SearchCacheTTLSeconds) * time.Second
	if searchCacheTTL <= 0 {
		searchCacheTTL = time.Hour
	}
	feedCacheTTL := time.Duration(cfg.FeedCacheTTLSeconds) * time.Second
	if feedCacheTTL <= 0 {
		feedCacheTTL = time.Hour
	}
	feedMaxResults := cfg.FeedMaxResults
	if feedMaxResults <= 0 {
		feedMaxResults = 1200
	}
	return &Client{
		baseURL: strings.TrimRight(cfg.URL, "/"),
		apiKey:  cfg.APIKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		searchInterval: defaultSearchInterval,
		searchCacheTTL: searchCacheTTL,
		feedCacheTTL:   feedCacheTTL,
		feedMaxResults: feedMaxResults,
		searchCache:    make(map[string]cachedResults),
		feedCache:      make(map[string]cachedResults),
	}
}

// SetSearchDelay configures the minimum delay between consecutive Hydra API
// calls. 0 means no delay (matches Sonarr/Radarr behaviour).
func (c *Client) SetSearchDelay(d time.Duration) {
	c.rateMu.Lock()
	c.searchInterval = d
	c.rateMu.Unlock()
}

func (c *Client) Name() string {
	return "nzbhydra2"
}

func (c *Client) Probe(ctx context.Context) error {
	u, err := c.apiURL()
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("t", "caps")
	if c.apiKey != "" {
		q.Set("apikey", c.apiKey)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("nzbhydra2 caps status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) SearchRecent(ctx context.Context, mediaType string) ([]SearchResult, error) {
	if cached, ok := c.lookupFeedCache(mediaType); ok {
		return cached, nil
	}
	if err := c.throttle(ctx); err != nil {
		return nil, err
	}
	u, err := c.apiURL()
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("t", "search")
	q.Set("cat", recentCategory(mediaType))
	q.Set("limit", fmt.Sprintf("%d", c.feedMaxResults))
	q.Set("extended", "1")
	if c.apiKey != "" {
		q.Set("apikey", c.apiKey)
	}
	u.RawQuery = q.Encode()
	results, err := c.doSearchRequest(ctx, u)
	if err == nil {
		c.storeFeedCache(mediaType, results)
	}
	return results, err
}

// throttle enforces searchInterval between consecutive Hydra API calls.
func (c *Client) throttle(ctx context.Context) error {
	c.rateMu.Lock()
	if time.Now().Before(c.cooldownUntil) {
		c.rateMu.Unlock()
		return fmt.Errorf("%w until %s", ErrRateLimited, c.cooldownUntil.UTC().Format(time.RFC3339))
	}
	now := time.Now()
	next := c.lastCall.Add(c.searchInterval)
	if next.Before(now) {
		next = now
	}
	c.lastCall = next
	c.rateMu.Unlock()
	wait := time.Until(next)
	if wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}

func (c *Client) Search(ctx context.Context, request SearchRequest) ([]SearchResult, error) {
	if cached, ok := c.lookupSearchCache(request); ok {
		return cached, nil
	}
	if err := c.throttle(ctx); err != nil {
		return nil, err
	}
	u, err := c.apiURL()
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("t", requestType(request))
	if cats := searchCategories(request.MediaType); cats != "" {
		q.Set("cat", cats)
	}
	if strings.TrimSpace(request.Query) != "" {
		q.Set("q", request.Query)
	}
	if imdbID := normalizeIMDbID(request.IMDbID); imdbID != "" {
		q.Set("imdbid", imdbID)
	}
	// tmdbid= is the primary ID for both Radarr (movies) and Sonarr (TV).
	// NZBHydra2 forwards it to indexers that support it.
	if request.TMDBID > 0 {
		q.Set("tmdbid", fmt.Sprintf("%d", request.TMDBID))
	}
	if strings.EqualFold(request.MediaType, "episode") || strings.EqualFold(request.MediaType, "tv") {
		if request.TVDBID > 0 {
			q.Set("tvdbid", fmt.Sprintf("%d", request.TVDBID))
		}
		if request.SeasonNumber > 0 {
			q.Set("season", fmt.Sprintf("%d", request.SeasonNumber))
		}
		if request.EpisodeNumber > 0 {
			q.Set("ep", fmt.Sprintf("%d", request.EpisodeNumber))
		}
	}
	q.Set("limit", "100")
	q.Set("extended", "1")
	if c.apiKey != "" {
		q.Set("apikey", c.apiKey)
	}
	u.RawQuery = q.Encode()
	results, err := c.doSearchRequest(ctx, u)
	if err == nil {
		c.storeSearchCache(request, results)
	}
	return results, err
}

func (c *Client) doSearchRequest(ctx context.Context, u *url.URL) ([]SearchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		c.startCooldown()
		return nil, fmt.Errorf("%w: nzbhydra2 search status %d", ErrRateLimited, resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("nzbhydra2 search status %d", resp.StatusCode)
	}
	c.recordSuccess()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(string(body))
	if strings.HasPrefix(trimmed, "<") {
		return parseXMLResults(body)
	}
	return parseJSONResults(body)
}

func (c *Client) startCooldown() {
	c.rateMu.Lock()
	defer c.rateMu.Unlock()
	level := c.rateLimitHits
	if level >= len(rateLimitBackoff) {
		level = len(rateLimitBackoff) - 1
	}
	until := time.Now().Add(rateLimitBackoff[level])
	if until.After(c.cooldownUntil) {
		c.cooldownUntil = until
	}
	if c.rateLimitHits < len(rateLimitBackoff)-1 {
		c.rateLimitHits++
	}
}

func (c *Client) recordSuccess() {
	c.rateMu.Lock()
	defer c.rateMu.Unlock()
	if c.rateLimitHits > 0 {
		c.rateLimitHits--
	}
	if time.Now().After(c.cooldownUntil) {
		c.cooldownUntil = time.Time{}
	}
}

func (c *Client) lookupSearchCache(request SearchRequest) ([]SearchResult, bool) {
	return c.lookupCache(c.searchCache, searchCacheKey(request))
}

func (c *Client) storeSearchCache(request SearchRequest, results []SearchResult) {
	c.storeCache(c.searchCache, searchCacheKey(request), results, c.searchCacheTTL)
}

func (c *Client) lookupFeedCache(mediaType string) ([]SearchResult, bool) {
	return c.lookupCache(c.feedCache, strings.ToLower(strings.TrimSpace(mediaType)))
}

func (c *Client) storeFeedCache(mediaType string, results []SearchResult) {
	c.storeCache(c.feedCache, strings.ToLower(strings.TrimSpace(mediaType)), results, c.feedCacheTTL)
}

func (c *Client) lookupCache(cache map[string]cachedResults, key string) ([]SearchResult, bool) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	entry, ok := cache[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(cache, key)
		return nil, false
	}
	return cloneResults(entry.results), true
}

func (c *Client) storeCache(cache map[string]cachedResults, key string, results []SearchResult, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	cache[key] = cachedResults{
		results:   cloneResults(results),
		expiresAt: time.Now().Add(ttl),
	}
}

func cloneResults(results []SearchResult) []SearchResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]SearchResult, len(results))
	copy(out, results)
	return out
}

func searchCacheKey(request SearchRequest) string {
	return strings.Join([]string{
		strings.ToLower(strings.TrimSpace(request.MediaType)),
		strings.ToLower(strings.TrimSpace(request.Query)),
		strings.ToLower(strings.TrimSpace(normalizeIMDbID(request.IMDbID))),
		fmt.Sprintf("%d", request.TMDBID),
		fmt.Sprintf("%d", request.TVDBID),
		fmt.Sprintf("%d", request.SeasonNumber),
		fmt.Sprintf("%d", request.EpisodeNumber),
	}, "|")
}

func requestType(request SearchRequest) string {
	switch strings.ToLower(strings.TrimSpace(request.MediaType)) {
	case "movie":
		return "movie"
	case "episode", "tv":
		return "tvsearch"
	default:
		return "search"
	}
}

func recentCategory(mediaType string) string {
	return searchCategories(mediaType)
}

func searchCategories(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "movie":
		return strings.Join(movieCategories, ",")
	case "episode", "tv":
		return strings.Join(tvCategories, ",")
	default:
		return ""
	}
}

func normalizeIMDbID(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.TrimPrefix(value, "tt")
	if value == "" {
		return ""
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return value
}

func (c *Client) apiURL() (*url.URL, error) {
	base := strings.TrimRight(c.baseURL, "/")
	if strings.HasSuffix(strings.ToLower(base), "/api") {
		return url.Parse(base)
	}
	return url.Parse(base + "/api")
}

func parseJSONResults(body []byte) ([]SearchResult, error) {
	var payload struct {
		Results []struct {
			Title        string `json:"title"`
			Link         string `json:"link"`
			Indexer      string `json:"indexer"`
			Size         int64  `json:"size"`
			Grabs        int    `json:"grabs"`
			Password     int    `json:"password"`
			IndexerScore int    `json:"hydraIndexerScore"`
			PubDate      string `json:"pubDate"`
			Published    string `json:"publishedDate"`
			Epoch        int64  `json:"epoch"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	out := make([]SearchResult, 0, len(payload.Results))
	for _, item := range payload.Results {
		out = append(out, SearchResult{
			Title:        item.Title,
			Link:         item.Link,
			Indexer:      item.Indexer,
			SizeBytes:    item.Size,
			PublishedAt:  parsePublished(item.Epoch, item.PubDate, item.Published),
			Grabs:        item.Grabs,
			IndexerScore: item.IndexerScore,
			Passworded:   item.Password != 0,
		})
	}
	return out, nil
}

func parseXMLResults(body []byte) ([]SearchResult, error) {
	var payload struct {
		Channel struct {
			Items []struct {
				Title   string `xml:"title"`
				Link    string `xml:"link"`
				PubDate string `xml:"pubDate"`
				Attrs   []struct {
					Name  string `xml:"name,attr"`
					Value string `xml:"value,attr"`
				} `xml:"attr"`
			} `xml:"item"`
		} `xml:"channel"`
	}
	if err := xml.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	out := make([]SearchResult, 0, len(payload.Channel.Items))
	for _, item := range payload.Channel.Items {
		result := SearchResult{
			Title:       item.Title,
			Link:        item.Link,
			PublishedAt: parsePublished(0, item.PubDate),
		}
		for _, attr := range item.Attrs {
			switch strings.ToLower(strings.TrimSpace(attr.Name)) {
			case "indexer", "hydraindexername":
				result.Indexer = attr.Value
			case "size":
				fmt.Sscan(attr.Value, &result.SizeBytes)
			case "grabs":
				fmt.Sscan(attr.Value, &result.Grabs)
			case "hydraindexerscore":
				fmt.Sscan(attr.Value, &result.IndexerScore)
			case "password":
				var v int
				fmt.Sscan(attr.Value, &v)
				result.Passworded = v != 0
			}
		}
		out = append(out, result)
	}
	return out, nil
}

func parsePublished(epoch int64, values ...string) time.Time {
	if epoch > 0 {
		return time.Unix(epoch, 0).UTC()
	}
	for _, value := range values {
		if value == "" {
			continue
		}
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			return parsed.UTC()
		}
		if parsed, err := time.Parse(time.RFC1123Z, value); err == nil {
			return parsed.UTC()
		}
		if parsed, err := time.Parse(time.RFC1123, value); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

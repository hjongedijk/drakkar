package seerr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hjongedijk/drakkar/internal/config"
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

var errRequestNotVisible = errors.New("seerr request not yet visible")

type Request struct {
	ID            int64
	Type          string
	Status        int
	MediaTitle    string
	MediaYear     int
	TMDBID        int64
	TVDBID        int64
	SeasonNumber  int
	EpisodeNumber int
	EpisodeTitle  string
	Seasons       []int // set for season-level requests (no individual episodes)
}

func NewClient(cfg config.ServiceConfig) *Client {
	return &Client{
		baseURL: strings.TrimRight(cfg.URL, "/"),
		apiKey:  cfg.APIKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) Name() string {
	return "seerr"
}

func (c *Client) Probe(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/status", nil)
	if err != nil {
		return err
	}
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("seerr status probe status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) PendingRequests(ctx context.Context) ([]Request, error) {
	const pageSize = 5000
	var out []Request
	for skip := 0; ; skip += pageSize {
		payload, err := c.fetchRequestPage(ctx, skip, pageSize)
		if err != nil {
			return nil, err
		}
		for _, item := range payload.Results {
			// Only skip explicitly declined requests (status 3).
			// Import pending (1), approved (2), available (4), and failed (5) so
			// Drakkar can serve all items regardless of what another downloader did.
			if item.Status == 3 {
				continue
			}
			title := strings.TrimSpace(item.Media.Title)
			if title == "" {
				title = strings.TrimSpace(item.Media.Name)
			}
			year := parseYear(item.Media.ReleaseDate)
			if year == 0 {
				year = parseYear(item.Media.FirstAirDate)
			}
			request := Request{
				ID:         item.ID,
				Type:       item.Type,
				Status:     item.Status,
				MediaTitle: title,
				MediaYear:  year,
				TMDBID:     item.Media.TMDBID,
				TVDBID:     item.Media.TVDBID,
			}
			if len(item.Episodes) > 0 {
				request.SeasonNumber = item.Episodes[0].SeasonNumber
				request.EpisodeNumber = item.Episodes[0].EpisodeNumber
				request.EpisodeTitle = item.Episodes[0].Name
			} else if len(item.Seasons) > 0 {
				for _, s := range item.Seasons {
					if s.SeasonNumber > 0 {
						request.Seasons = append(request.Seasons, s.SeasonNumber)
					}
				}
			}
			out = append(out, request)
		}
		if payload.PageInfo.Results <= skip+pageSize || len(payload.Results) == 0 {
			break
		}
	}
	return out, nil
}

type requestListPayload struct {
	PageInfo struct {
		Results  int `json:"results"`
		PageSize int `json:"pageSize"`
		Page     int `json:"page"`
	} `json:"pageInfo"`
	Results []struct {
		ID     int64  `json:"id"`
		Type   string `json:"type"`
		Status int    `json:"status"`
		Media  struct {
			TMDBID       int64  `json:"tmdbId"`
			TVDBID       int64  `json:"tvdbId"`
			Title        string `json:"title"`
			Name         string `json:"name"`
			ReleaseDate  string `json:"releaseDate"`
			FirstAirDate string `json:"firstAirDate"`
		} `json:"media"`
		Episodes []struct {
			SeasonNumber  int    `json:"seasonNumber"`
			EpisodeNumber int    `json:"episodeNumber"`
			Name          string `json:"name"`
		} `json:"episodes"`
		Seasons []struct {
			SeasonNumber int `json:"seasonNumber"`
			Status       int `json:"status"`
		} `json:"seasons"`
	} `json:"results"`
}

func (c *Client) fetchRequestPage(ctx context.Context, skip, take int) (requestListPayload, error) {
	u, err := url.Parse(c.baseURL + "/api/v1/request")
	if err != nil {
		return requestListPayload{}, err
	}
	q := u.Query()
	q.Set("take", strconv.Itoa(take))
	q.Set("skip", strconv.Itoa(skip))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return requestListPayload{}, err
	}
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return requestListPayload{}, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return requestListPayload{}, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return requestListPayload{}, classifySeerrHTTPError("request list", resp.StatusCode, body)
	}
	if err := detectSeerrResponseError("request list", resp.StatusCode, body); err != nil {
		return requestListPayload{}, err
	}

	var payload requestListPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return requestListPayload{}, err
	}
	return payload, nil
}

func (c *Client) CreateRequest(ctx context.Context, mediaType string, tmdbID int64) error {
	body := map[string]any{
		"mediaType": mediaType,
		"mediaId":   tmdbID,
	}
	if mediaType == "tv" {
		body["seasons"] = "all"
	}
	match := func(request Request) bool {
		return strings.EqualFold(request.Type, mediaType) && request.TMDBID == tmdbID
	}
	return c.createRequestWithRecovery(ctx, body, match)
}

func (c *Client) CreateTVSeasonRequest(ctx context.Context, tmdbID int64, seasons []int) error {
	body := map[string]any{
		"mediaType": "tv",
		"mediaId":   tmdbID,
		"seasons":   seasons,
	}
	expected := make(map[int]struct{}, len(seasons))
	for _, season := range seasons {
		expected[season] = struct{}{}
	}
	match := func(request Request) bool {
		if !strings.EqualFold(request.Type, "tv") || request.TMDBID != tmdbID {
			return false
		}
		if len(expected) == 0 {
			return true
		}
		_, ok := expected[request.SeasonNumber]
		return ok
	}
	return c.createRequestWithRecovery(ctx, body, match)
}

// CreateTVSeasonRequestNoWait posts a season request to Seerr without waiting
// for the request to become visible in the request list. Use this for bulk
// imports where visibility confirmation is not needed per-item.
func (c *Client) CreateTVSeasonRequestNoWait(ctx context.Context, tmdbID int64, seasons []int) error {
	body := map[string]any{
		"mediaType": "tv",
		"mediaId":   tmdbID,
		"seasons":   seasons,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return c.postCreateRequest(ctx, data)
}

// PartialTVItem represents a TV show that is partially available in Plex
// (Seerr status=4) with no explicit download request.
type PartialTVItem struct {
	TMDBID         int64
	TVDBID         int64
	PartialSeasons []int // season numbers where status != 5 (not fully available)
}

// PartialTVItems returns TV shows tracked by Seerr that are partially available
// (have some episodes in Plex but not all). Items with no seasons of interest
// (all seasons already status=5) are omitted.
func (c *Client) PartialTVItems(ctx context.Context) ([]PartialTVItem, error) {
	const pageSize = 500
	var out []PartialTVItem
	for skip := 0; ; skip += pageSize {
		payload, err := c.fetchPartialMediaPage(ctx, skip, pageSize)
		if err != nil {
			return nil, err
		}
		for _, r := range payload.Results {
			if r.MediaType != "tv" {
				continue
			}
			var partial []int
			for _, s := range r.Seasons {
				if s.Status != 5 && s.SeasonNumber > 0 {
					partial = append(partial, s.SeasonNumber)
				}
			}
			if len(partial) == 0 {
				continue
			}
			out = append(out, PartialTVItem{
				TMDBID:         r.TMDBiD,
				TVDBID:         r.TVDBiD,
				PartialSeasons: partial,
			})
		}
		if payload.PageInfo.Results <= skip+pageSize || len(payload.Results) == 0 {
			break
		}
	}
	return out, nil
}

type partialMediaPayload struct {
	PageInfo struct {
		Pages    int `json:"pages"`
		PageSize int `json:"pageSize"`
		Results  int `json:"results"`
		Page     int `json:"page"`
	} `json:"pageInfo"`
	Results []struct {
		MediaType string `json:"mediaType"`
		TMDBiD    int64  `json:"tmdbId"`
		TVDBiD    int64  `json:"tvdbId"`
		Status    int    `json:"status"`
		Seasons   []struct {
			SeasonNumber int `json:"seasonNumber"`
			Status       int `json:"status"`
		} `json:"seasons"`
	} `json:"results"`
}

func (c *Client) fetchPartialMediaPage(ctx context.Context, skip, pageSize int) (partialMediaPayload, error) {
	u, err := url.Parse(c.baseURL + "/api/v1/media")
	if err != nil {
		return partialMediaPayload{}, err
	}
	q := u.Query()
	q.Set("filter", "partial")
	q.Set("take", strconv.Itoa(pageSize))
	q.Set("skip", strconv.Itoa(skip))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return partialMediaPayload{}, err
	}
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return partialMediaPayload{}, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return partialMediaPayload{}, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return partialMediaPayload{}, classifySeerrHTTPError("partial media", resp.StatusCode, body)
	}
	if err := detectSeerrResponseError("partial media", resp.StatusCode, body); err != nil {
		return partialMediaPayload{}, err
	}
	var payload partialMediaPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return partialMediaPayload{}, err
	}
	return payload, nil
}

func (c *Client) createRequestWithRecovery(ctx context.Context, body map[string]any, match func(Request) bool) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if err := c.postCreateRequest(ctx, data); err != nil {
		if !isRetryableSeerrError(err) {
			return err
		}
		if waitErr := c.waitForVisibleRequest(ctx, match); waitErr == nil {
			return nil
		}
		if err := c.postCreateRequest(ctx, data); err != nil {
			if !isRetryableSeerrError(err) {
				return err
			}
			if waitErr := c.waitForVisibleRequest(ctx, match); waitErr == nil {
				return nil
			}
			return err
		}
	}
	if err := c.waitForVisibleRequest(ctx, match); err != nil {
		if errors.Is(err, errRequestNotVisible) || isRetryableSeerrError(err) {
			return nil
		}
		return err
	}
	return nil
}

func (c *Client) postCreateRequest(ctx context.Context, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/request", strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusUnprocessableEntity {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return classifySeerrHTTPError("create request", resp.StatusCode, body)
	}
	if err := detectSeerrResponseError("create request", resp.StatusCode, body); err != nil {
		return err
	}
	return nil
}

func (c *Client) waitForVisibleRequest(ctx context.Context, match func(Request) bool) error {
	backoff := []time.Duration{0, 1 * time.Second, 2 * time.Second, 3 * time.Second}
	for _, delay := range backoff {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		requests, err := c.PendingRequests(ctx)
		if err != nil {
			if isRetryableSeerrError(err) {
				continue
			}
			return err
		}
		for _, request := range requests {
			if match(request) {
				return nil
			}
		}
	}
	return errRequestNotVisible
}

// NotifyAvailable marks a media item as available in Seerr/Overseerr.
// It first looks up the Seerr-internal media ID by TMDB ID, then POSTs to
// the available endpoint. A 404 means the item isn't tracked in Seerr — not an error.
func (c *Client) NotifyAvailable(ctx context.Context, tmdbID int64, mediaType string) error {
	if c.baseURL == "" || c.apiKey == "" {
		return nil
	}
	apiMediaType := "movie"
	if strings.EqualFold(mediaType, "tv") || strings.EqualFold(mediaType, "episode") {
		apiMediaType = "tv"
	}
	// Resolve TMDB ID to Seerr internal media ID.
	infoURL := fmt.Sprintf("%s/api/v1/%s/%d", c.baseURL, apiMediaType, tmdbID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Api-Key", c.apiKey)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil // item not tracked in Seerr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("seerr media lookup status %d", resp.StatusCode)
	}
	var info struct {
		MediaInfo *struct {
			ID int64 `json:"id"`
		} `json:"mediaInfo"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return err
	}
	if info.MediaInfo == nil || info.MediaInfo.ID == 0 {
		return nil // no mediaInfo yet — Seerr doesn't track it
	}
	// Mark as available.
	availURL := fmt.Sprintf("%s/api/v1/media/%d/available", c.baseURL, info.MediaInfo.ID)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, availURL, strings.NewReader("{}"))
	if err != nil {
		return err
	}
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("X-Api-Key", c.apiKey)
	postResp, err := c.httpClient.Do(postReq)
	if err != nil {
		return err
	}
	defer postResp.Body.Close()
	if postResp.StatusCode < 200 || postResp.StatusCode >= 300 {
		return fmt.Errorf("seerr notify available status %d", postResp.StatusCode)
	}
	return nil
}

func classifySeerrHTTPError(action string, statusCode int, body []byte) error {
	snippet := summarizeSeerrBody(body)
	switch statusCode {
	case 520, 521, 522, 523:
		if snippet != "" {
			return fmt.Errorf("seerr %s cloudflare unavailable status %d: %s", action, statusCode, snippet)
		}
		return fmt.Errorf("seerr %s cloudflare unavailable status %d", action, statusCode)
	case 524:
		if snippet != "" {
			return fmt.Errorf("seerr %s cloudflare timeout status %d: %s", action, statusCode, snippet)
		}
		return fmt.Errorf("seerr %s cloudflare timeout status %d", action, statusCode)
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		if snippet != "" {
			return fmt.Errorf("seerr %s status %d: %s", action, statusCode, snippet)
		}
	}
	if snippet != "" {
		return fmt.Errorf("seerr %s status %d: %s", action, statusCode, snippet)
	}
	return fmt.Errorf("seerr %s status %d", action, statusCode)
}

func detectSeerrResponseError(action string, statusCode int, body []byte) error {
	text := strings.ToLower(strings.TrimSpace(string(body)))
	switch {
	case strings.Contains(text, "cloudflare") && strings.Contains(text, "524"):
		return fmt.Errorf("seerr %s cloudflare timeout status %d", action, statusCode)
	case strings.Contains(text, "cloudflare") && strings.Contains(text, "522"):
		return fmt.Errorf("seerr %s cloudflare unavailable status %d", action, statusCode)
	case strings.Contains(text, "cloudflare") && strings.Contains(text, "timed out"):
		return fmt.Errorf("seerr %s cloudflare timeout status %d", action, statusCode)
	case strings.Contains(text, "<html") && strings.Contains(text, "cloudflare"):
		return fmt.Errorf("seerr %s cloudflare unavailable status %d", action, statusCode)
	case strings.Contains(text, "<html") && strings.Contains(text, "bad gateway"):
		return fmt.Errorf("seerr %s status %d: bad gateway", action, statusCode)
	case strings.Contains(text, "<html") && strings.Contains(text, "gateway timeout"):
		return fmt.Errorf("seerr %s status %d: gateway timeout", action, statusCode)
	default:
		return nil
	}
}

func isRetryableSeerrError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "timeout"),
		strings.Contains(text, "deadline exceeded"),
		strings.Contains(text, "status 500"),
		strings.Contains(text, "status 502"),
		strings.Contains(text, "status 503"),
		strings.Contains(text, "status 504"),
		strings.Contains(text, "status 520"),
		strings.Contains(text, "status 521"),
		strings.Contains(text, "status 522"),
		strings.Contains(text, "status 523"),
		strings.Contains(text, "status 524"),
		strings.Contains(text, "cloudflare"),
		strings.Contains(text, "bad gateway"),
		strings.Contains(text, "gateway timeout"),
		strings.Contains(text, "connection refused"),
		strings.Contains(text, "no such host"):
		return true
	default:
		return false
	}
}

func summarizeSeerrBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return ""
	}
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 160 {
		text = text[:160]
	}
	return text
}

func parseYear(value string) int {
	if len(value) < 4 {
		return 0
	}
	year, err := strconv.Atoi(value[:4])
	if err != nil {
		return 0
	}
	return year
}

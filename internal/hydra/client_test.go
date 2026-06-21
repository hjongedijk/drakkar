package hydra

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hjongedijk/drakkar/internal/config"
)

func TestSearch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("t") != "search" {
			t.Fatalf("unexpected query %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"title":"Dune.2021.1080p.WEB-DL.x265-GRP","link":"http://example/nzb","indexer":"hydra","size":12345,"epoch":1710000000}]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	got, err := client.Search(context.Background(), SearchRequest{Query: "Dune 2021"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d", len(got))
	}
	if got[0].Indexer != "hydra" || got[0].SizeBytes != 12345 {
		t.Fatalf("unexpected result %+v", got[0])
	}
}

func TestSearchXML(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/">
  <channel>
    <item>
      <title>Dune.2021.1080p.WEB-DL.x265-GRP</title>
      <link>http://example/nzb</link>
      <pubDate>Fri, 05 Jun 2026 12:00:00 +0000</pubDate>
      <newznab:attr name="indexer" value="hydra" />
      <newznab:attr name="size" value="12345" />
    </item>
  </channel>
</rss>`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	got, err := client.Search(context.Background(), SearchRequest{Query: "Dune 2021"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d", len(got))
	}
	if got[0].Indexer != "hydra" || got[0].SizeBytes != 12345 || got[0].Link != "http://example/nzb" {
		t.Fatalf("unexpected result %+v", got[0])
	}
}

func TestProbeUsesCaps(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("t"); got != "caps" {
			t.Fatalf("unexpected probe type %q", got)
		}
		if got := r.URL.Query().Get("q"); got != "" {
			t.Fatalf("probe should not search, got q=%q", got)
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><caps></caps>`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL + "/api", APIKey: "abc"})
	if err := client.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSearchUsesStructuredMovieParams(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("t"); got != "movie" {
			t.Fatalf("expected movie search, got %q", got)
		}
		if got := r.URL.Query().Get("cat"); got != "2000,2010,2020,2030,2040,2045,2050,2060" {
			t.Fatalf("expected movie categories, got %q", got)
		}
		if got := r.URL.Query().Get("imdbid"); got != "1160419" {
			t.Fatalf("expected imdbid 1160419, got %q", got)
		}
		if got := r.URL.Query().Get("q"); got != "Dune 2021" {
			t.Fatalf("expected q, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	if _, err := client.Search(context.Background(), SearchRequest{MediaType: "movie", Query: "Dune 2021", IMDbID: "tt1160419"}); err != nil {
		t.Fatal(err)
	}
}

func TestSearchOmitsFreeTextWhenOnlyIMDbIDIsUsed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("imdbid"); got != "1160419" {
			t.Fatalf("expected imdbid 1160419, got %q", got)
		}
		if got := r.URL.Query().Get("q"); got != "" {
			t.Fatalf("expected empty q, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	if _, err := client.Search(context.Background(), SearchRequest{MediaType: "movie", IMDbID: "tt1160419"}); err != nil {
		t.Fatal(err)
	}
}

func TestSearchUsesStructuredTVParams(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("t"); got != "tvsearch" {
			t.Fatalf("expected tvsearch, got %q", got)
		}
		if got := r.URL.Query().Get("cat"); got != "5000" {
			t.Fatalf("expected tv categories, got %q", got)
		}
		if got := r.URL.Query().Get("imdbid"); got != "9140554" {
			t.Fatalf("expected imdbid 9140554, got %q", got)
		}
		if got := r.URL.Query().Get("tvdbid"); got != "362472" {
			t.Fatalf("expected tvdbid 362472, got %q", got)
		}
		if got := r.URL.Query().Get("season"); got != "2" {
			t.Fatalf("expected season 2, got %q", got)
		}
		if got := r.URL.Query().Get("ep"); got != "3" {
			t.Fatalf("expected ep 3, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	if _, err := client.Search(context.Background(), SearchRequest{MediaType: "episode", Query: "Loki S02E03", IMDbID: "tt9140554", TVDBID: 362472, SeasonNumber: 2, EpisodeNumber: 3}); err != nil {
		t.Fatal(err)
	}
}

func TestSearchRecentUsesCategoryOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("t"); got != "search" {
			t.Fatalf("expected search, got %q", got)
		}
		if got := r.URL.Query().Get("cat"); got != "5000" {
			t.Fatalf("expected tv category, got %q", got)
		}
		if got := r.URL.Query().Get("q"); got != "" {
			t.Fatalf("expected empty q, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	if _, err := client.SearchRecent(context.Background(), "tv"); err != nil {
		t.Fatal(err)
	}
}

func TestSearchStartsCooldownOn429(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	if _, err := client.Search(context.Background(), SearchRequest{Query: "Dune 2021"}); err == nil {
		t.Fatal("expected rate limit error")
	}
	if _, err := client.SearchRecent(context.Background(), "movie"); err == nil {
		t.Fatal("expected cooldown error")
	}
}

func TestSearchUsesCacheWithinTTL(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"title":"Dune.2021.1080p.WEB-DL.x265-GRP","link":"http://example/nzb","indexer":"hydra","size":12345,"epoch":1710000000}]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc", SearchCacheTTLSeconds: 3600})
	if _, err := client.Search(context.Background(), SearchRequest{MediaType: "movie", Query: "Dune 2021"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Search(context.Background(), SearchRequest{MediaType: "movie", Query: "Dune 2021"}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("expected 1 upstream hit, got %d", got)
	}
}

func TestSearchRecentUsesFeedCacheWithinTTL(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc", FeedCacheTTLSeconds: 3600})
	if _, err := client.SearchRecent(context.Background(), "tv"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SearchRecent(context.Background(), "tv"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("expected 1 upstream hit, got %d", got)
	}
}

func TestSearchDefaultDoesNotCache(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	if _, err := client.Search(context.Background(), SearchRequest{MediaType: "movie", Query: "Dune 2021"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Search(context.Background(), SearchRequest{MediaType: "movie", Query: "Dune 2021"}); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("expected 2 upstream hits with default no-cache, got %d", got)
	}
}

func TestSearchRecentDefaultDoesNotCache(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	if _, err := client.SearchRecent(context.Background(), "tv"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SearchRecent(context.Background(), "tv"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("expected 2 upstream hits with default no-cache, got %d", got)
	}
}

// TestSearchPaginates verifies that Search fetches subsequent pages when the
// first page is full (100 results) and stops when a partial page is received.
func TestSearchPaginates(t *testing.T) {
	// Build a full page of exactly searchPageSize results.
	items := make([]string, searchPageSize)
	for i := range items {
		items[i] = fmt.Sprintf(`{"title":"Release%d","link":"http://example/%d","indexer":"hydra","size":100,"epoch":1710000000}`, i, i)
	}
	fullPage := `{"results":[` + strings.Join(items, ",") + `]}`
	partialPage := `{"results":[{"title":"Last","link":"http://example/last","indexer":"hydra","size":100,"epoch":1710000000}]}`

	var offsets []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset := r.URL.Query().Get("offset")
		offsets = append(offsets, offset)
		w.Header().Set("Content-Type", "application/json")
		if offset == "0" {
			_, _ = w.Write([]byte(fullPage))
		} else {
			_, _ = w.Write([]byte(partialPage))
		}
	}))
	defer server.Close()

	client := NewClient(config.ServiceConfig{URL: server.URL, APIKey: "abc"})
	results, err := client.Search(context.Background(), SearchRequest{Query: "Show"})
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 {
		t.Fatalf("expected 2 page requests (offsets 0 and 100), got %d: %v", len(offsets), offsets)
	}
	if offsets[0] != "0" || offsets[1] != "100" {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
	// 100 results from page 0 + 1 result from page 1
	if len(results) != searchPageSize+1 {
		t.Fatalf("expected %d results, got %d", searchPageSize+1, len(results))
	}
}

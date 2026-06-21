package workflow

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPNZBFetcherRedirect403MentionsIndexerHost(t *testing.T) {
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer indexer.Close()

	hydra := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, indexer.URL+"/download.nzb", http.StatusFound)
	}))
	defer hydra.Close()

	_, _, err := (HTTPNZBFetcher{}).Fetch(context.Background(), hydra.URL+"/getnzb")
	if err == nil {
		t.Fatal("expected fetch error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "redirect to indexer host") {
		t.Fatalf("expected redirect host detail, got %q", msg)
	}
	if !strings.Contains(msg, "direct indexer download was forbidden") {
		t.Fatalf("expected direct-download detail, got %q", msg)
	}
}

func TestHTTPNZBFetcherFollowsRedirectCookieChallenge(t *testing.T) {
	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := r.Cookie("cf_clearance"); err != nil {
			http.SetCookie(w, &http.Cookie{Name: "cf_clearance", Value: "ok", Path: "/"})
			http.Redirect(w, r, "/download.nzb", http.StatusFound)
			return
		}
		if got := r.Header.Get("User-Agent"); !strings.Contains(got, "Drakkar") {
			http.Error(w, "missing user-agent", http.StatusForbidden)
			return
		}
		if got := r.Header.Get("Referer"); got == "" {
			http.Error(w, "missing referer", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/x-nzb")
		_, _ = io.WriteString(w, "<nzb></nzb>")
	}))
	defer indexer.Close()

	hydra := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, indexer.URL+"/gate", http.StatusFound)
	}))
	defer hydra.Close()

	name, raw, err := (HTTPNZBFetcher{}).Fetch(context.Background(), hydra.URL+"/getnzb")
	if err != nil {
		t.Fatalf("expected fetch success, got %v", err)
	}
	if name != "getnzb" {
		t.Fatalf("expected original filename fallback from request path, got %q", name)
	}
	if string(raw) != "<nzb></nzb>" {
		t.Fatalf("unexpected nzb body %q", string(raw))
	}
}

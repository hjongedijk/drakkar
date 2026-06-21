package plex

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMatchingLibrariesForPath(t *testing.T) {
	libs := []Library{
		{Key: "1", Type: "movie", Locations: []string{"/mnt/drakkar/media/movies"}},
		{Key: "2", Type: "show", Locations: []string{"/mnt/drakkar/media/tv"}},
	}
	got := matchingLibrariesForPath(libs, "/mnt/drakkar/media/tv/Yellowstone/Season 01")
	if len(got) != 1 || got[0].Key != "2" {
		t.Fatalf("expected tv library match, got %+v", got)
	}
}

func TestRefreshPathAutoUsesMatchingLibrary(t *testing.T) {
	var hits []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		switch r.URL.Path {
		case "/library/sections":
			if r.Method != http.MethodGet {
				t.Fatalf("expected GET libraries request, got %s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"MediaContainer":{"Directory":[
				{"key":"1","title":"Movies","type":"movie","agent":"x","Location":[{"path":"/mnt/drakkar/media/movies"}]},
				{"key":"2","title":"TV","type":"show","agent":"x","Location":[{"path":"/mnt/drakkar/media/tv"}]}
			]}}`))
		case "/library/sections/2/refresh":
			if r.Method != http.MethodGet {
				t.Fatalf("expected GET refresh request, got %s", r.Method)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected path %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "token")
	if err := client.RefreshPathAuto(context.Background(), "", "/mnt/drakkar/media/tv/Yellowstone"); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected libraries + one refresh call, got %v", hits)
	}
	if hits[1] != "GET /library/sections/2/refresh?path=%2Fmnt%2Fdrakkar%2Fmedia%2Ftv%2FYellowstone" {
		t.Fatalf("unexpected refresh hit %q", hits[1])
	}
}

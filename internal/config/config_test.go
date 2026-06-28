package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte(`{
  "database":{"host":"postgres","port":5432,"name":"drakkar","username":"drakkar","password":"secret"},
  "valkey":{"host":"valkey","port":6379,"password":""},
  "nzbhydra2":{"url":"http://nzbhydra2:5076","apiKey":""},
  "seerr":{"url":"http://seerr:5055","apiKey":""},
  "usenet":{"providers":[{"name":"primary","host":"news","port":563,"tls":true,"username":"","password":"","maxConnections":20,"enabled":true}]},
  "metadata":{"tmdb":{"apiKey":""},"tvdb":{"apiKey":""}},
  "subtitles":{"enabled":true,"languages":["nl","en"],"providers":{"subdl":{"enabled":true,"apiKey":""}}},
  "oops":true
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestRedactedSettings(t *testing.T) {
	out := RedactedSettings(Settings{
		Database:  DatabaseConfig{Host: "postgres", Port: 5432, Name: "drakkar", Username: "drakkar", Password: "secret"},
		Valkey:    ValkeyConfig{Host: "valkey", Port: 6379, Password: "secret"},
		NZBHydra2: ServiceConfig{URL: "http://nzbhydra2:5076", APIKey: "abc"},
		Seerr:     ServiceConfig{URL: "http://seerr:5055", APIKey: "def"},
		Usenet: UsenetConfig{
			MaxDownloadConnections: 15,
			StreamingPriorityPct:   80,
			ArticleBufferSize:      40,
			Providers:              []UsenetProvider{{Name: "primary", Host: "news", Port: 563, TLS: true, Username: "u", Password: "p", MaxConnections: 20, Enabled: true}},
		},
		Metadata: MetadataConfig{TMDB: APIKeyConfig{APIKey: "ghi"}, TVDB: APIKeyConfig{APIKey: "jkl"}},
		Subtitles: SubtitlesConfig{Enabled: true, Languages: []string{"en"}, Providers: map[string]SubtitleAuth{
			"subdl": {Enabled: true, APIKey: "xyz", Username: "u", Password: "p"},
		}},
	})
	if out["database"].(map[string]any)["password"] != "***" {
		t.Fatal("database password not redacted")
	}
	if out["metadata"].(map[string]any)["tmdb"].(map[string]any)["apiKey"] != "***" {
		t.Fatal("tmdb api key not redacted")
	}
	if out["subtitles"].(map[string]any)["providers"].(map[string]any)["subdl"].(map[string]any)["password"] != "***" {
		t.Fatal("subtitle password not redacted")
	}
	if out["usenet"].(map[string]any)["providers"].([]map[string]any)[0]["password"] != "***" {
		t.Fatal("usenet password not redacted")
	}
}

func TestValidatePathsRejectsNestedLibrary(t *testing.T) {
	rt := DefaultRuntime()
	rt.MovieLibraryPath = "/mnt/drakkar/vfs/media/movies"
	err := ValidatePaths(rt)
	if err == nil || !strings.Contains(err.Error(), "outside fuse mount") {
		t.Fatalf("expected fuse separation error, got %v", err)
	}
}

func TestLoadAppliesIndexerWorkerDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte(`{
  "database":{"host":"postgres","port":5432,"name":"drakkar","username":"drakkar","password":"secret"},
  "valkey":{"host":"valkey","port":6379,"password":""},
  "nzbhydra2":{"url":"http://nzbhydra2:5076","apiKey":""},
  "seerr":{"url":"http://seerr:5055","apiKey":""},
  "usenet":{"providers":[{"name":"primary","host":"news","port":563,"tls":true,"username":"","password":"","maxConnections":20,"enabled":true}]},
  "metadata":{"tmdb":{"apiKey":""},"tvdb":{"apiKey":""}},
  "subtitles":{"enabled":true,"languages":["en"],"providers":{"subdl":{"enabled":true,"apiKey":""}}},
  "indexer":{"searchDelayMs":0}
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Indexer.BackgroundSearchWorkers != 10 {
		t.Fatalf("expected default backgroundSearchWorkers=10, got %d", cfg.Indexer.BackgroundSearchWorkers)
	}
}

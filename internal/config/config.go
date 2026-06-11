package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultSettingsPath         = "/app/data/settings.json"
	DefaultFuseMountPath        = "/mnt/drakkar/vfs"
	DefaultMovieLibraryPath     = "/mnt/drakkar/media/movies"
	DefaultTVLibraryPath        = "/mnt/drakkar/media/tv"
	DefaultBlockCachePath       = "/mnt/drakkar/cache/blocks"
	DefaultHeaderCachePath      = "/mnt/drakkar/cache/headers"
	DefaultRepairWorkspacePath  = "/mnt/drakkar/cache/repair-workspace"
	DefaultStagingNZBPath       = "/mnt/drakkar/staging/nzbs"
	DefaultFailedDiagnostics    = "/mnt/drakkar/failed"
	DefaultLogsPath             = "/app/data/logs"
	DefaultHTTPAddress          = ":8080"
	DefaultWebDAVAddress        = ":8888"
	DefaultDiskCacheLimitBytes  = int64(20 << 30)
	DefaultReadAheadLimitBytes  = int64(512 << 20)
	DefaultMemoryHotCacheBytes  = int64(512 << 20)
	DefaultRepairWorkspaceBytes = int64(20 << 30)
	DefaultNZBUploadLimitBytes  = int64(64 << 20)
	DefaultMaxDownloadConns     = 15
	DefaultStreamingPriorityPct = 80
	DefaultArticleBufferSize    = 40
)

type Settings struct {
	Database  DatabaseConfig  `json:"database"`
	Valkey    ValkeyConfig    `json:"valkey"`
	NZBHydra2 ServiceConfig   `json:"nzbhydra2"`
	Seerr     ServiceConfig   `json:"seerr"`
	Usenet    UsenetConfig    `json:"usenet"`
	Metadata  MetadataConfig  `json:"metadata"`
	Subtitles SubtitlesConfig `json:"subtitles"`
	Plex      PlexConfig      `json:"plex"`
}

// PlexConfig holds the Plex Media Server connection settings.
type PlexConfig struct {
	URL        string `json:"url"`        // e.g. http://192.168.1.10:32400
	Token      string `json:"token"`      // X-Plex-Token
	SectionKey string `json:"sectionKey"` // library section key (empty = all)
}

type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type ValkeyConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Password string `json:"password"`
}

type ServiceConfig struct {
	URL                   string `json:"url"`
	APIKey                string `json:"apiKey"`
	SearchCacheTTLSeconds int    `json:"searchCacheTtlSeconds"`
	FeedCacheTTLSeconds   int    `json:"feedCacheTtlSeconds"`
	FeedMaxResults        int    `json:"feedMaxResults"`
}

type UsenetConfig struct {
	MaxDownloadConnections int              `json:"maxDownloadConnections"`
	StreamingPriorityPct   int              `json:"streamingPriorityPercent"`
	ArticleBufferSize      int              `json:"articleBufferSize"`
	Providers              []UsenetProvider `json:"providers"`
}

type UsenetProvider struct {
	Name           string `json:"name"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	TLS            bool   `json:"tls"`
	Username       string `json:"username"`
	Password       string `json:"password"`
	MaxConnections int    `json:"maxConnections"`
	Enabled        bool   `json:"enabled"`
}

type MetadataConfig struct {
	TMDB APIKeyConfig `json:"tmdb"`
	TVDB APIKeyConfig `json:"tvdb"`
}

type APIKeyConfig struct {
	APIKey string `json:"apiKey"`
}

type SubtitlesConfig struct {
	Enabled   bool                    `json:"enabled"`
	Languages []string                `json:"languages"`
	Providers map[string]SubtitleAuth `json:"providers"`
}

type SubtitleAuth struct {
	Enabled  bool   `json:"enabled"`
	APIKey   string `json:"apiKey"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type Runtime struct {
	SettingsPath           string
	HTTPAddress            string
	WebDAVAddress          string
	FuseMountPath          string
	MovieLibraryPath       string
	TVLibraryPath          string
	BlockCachePath         string
	HeaderCachePath        string
	RepairWorkspacePath    string
	StagingNZBPath         string
	FailedDiagnosticsPath  string
	LogsPath               string
	DiskCacheLimitBytes    int64
	ReadAheadLimitBytes    int64
	MemoryHotCacheMaxBytes int64
	RepairWorkspaceMax     int64
	NZBUploadLimitBytes    int64
}

func DefaultRuntime() Runtime {
	return Runtime{
		SettingsPath:           DefaultSettingsPath,
		HTTPAddress:            DefaultHTTPAddress,
		WebDAVAddress:          DefaultWebDAVAddress,
		FuseMountPath:          DefaultFuseMountPath,
		MovieLibraryPath:       DefaultMovieLibraryPath,
		TVLibraryPath:          DefaultTVLibraryPath,
		BlockCachePath:         DefaultBlockCachePath,
		HeaderCachePath:        DefaultHeaderCachePath,
		RepairWorkspacePath:    DefaultRepairWorkspacePath,
		StagingNZBPath:         DefaultStagingNZBPath,
		FailedDiagnosticsPath:  DefaultFailedDiagnostics,
		LogsPath:               DefaultLogsPath,
		DiskCacheLimitBytes:    DefaultDiskCacheLimitBytes,
		ReadAheadLimitBytes:    DefaultReadAheadLimitBytes,
		MemoryHotCacheMaxBytes: DefaultMemoryHotCacheBytes,
		RepairWorkspaceMax:     DefaultRepairWorkspaceBytes,
		NZBUploadLimitBytes:    DefaultNZBUploadLimitBytes,
	}
}

func Load(path string) (Settings, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Settings{}, fmt.Errorf("read settings: %w", err)
	}
	var cfg Settings
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Settings{}, fmt.Errorf("parse settings: %w", err)
	}
	applyDefaults(&cfg)
	if err := validate(cfg); err != nil {
		return Settings{}, err
	}
	return cfg, nil
}

func applyDefaults(cfg *Settings) {
	if cfg == nil {
		return
	}
	if cfg.Usenet.MaxDownloadConnections <= 0 {
		cfg.Usenet.MaxDownloadConnections = DefaultMaxDownloadConns
	}
	if cfg.Usenet.StreamingPriorityPct <= 0 {
		cfg.Usenet.StreamingPriorityPct = DefaultStreamingPriorityPct
	}
	if cfg.Usenet.ArticleBufferSize <= 0 {
		cfg.Usenet.ArticleBufferSize = DefaultArticleBufferSize
	}
}

func validate(cfg Settings) error {
	var problems []string
	if cfg.Database.Host == "" {
		problems = append(problems, "database.host required")
	}
	if cfg.Database.Port <= 0 {
		problems = append(problems, "database.port must be positive")
	}
	if cfg.Database.Name == "" {
		problems = append(problems, "database.name required")
	}
	if cfg.Database.Username == "" {
		problems = append(problems, "database.username required")
	}
	if cfg.Valkey.Host == "" {
		problems = append(problems, "valkey.host required")
	}
	if cfg.Valkey.Port <= 0 {
		problems = append(problems, "valkey.port must be positive")
	}
	if err := validateURL("nzbhydra2.url", cfg.NZBHydra2.URL); err != nil {
		problems = append(problems, err.Error())
	}
	if err := validateURL("seerr.url", cfg.Seerr.URL); err != nil {
		problems = append(problems, err.Error())
	}
	if len(cfg.Usenet.Providers) == 0 {
		problems = append(problems, "usenet.providers required")
	}
	if cfg.Usenet.MaxDownloadConnections <= 0 {
		problems = append(problems, "usenet.maxDownloadConnections must be positive")
	}
	if cfg.Usenet.StreamingPriorityPct < 0 || cfg.Usenet.StreamingPriorityPct > 100 {
		problems = append(problems, "usenet.streamingPriorityPercent must be 0..100")
	}
	if cfg.Usenet.ArticleBufferSize <= 0 {
		problems = append(problems, "usenet.articleBufferSize must be positive")
	}
	for i, provider := range cfg.Usenet.Providers {
		prefix := fmt.Sprintf("usenet.providers[%d]", i)
		if provider.Name == "" {
			problems = append(problems, prefix+".name required")
		}
		if provider.Port <= 0 {
			problems = append(problems, prefix+".port must be positive")
		}
		if provider.MaxConnections <= 0 {
			problems = append(problems, prefix+".maxConnections must be positive")
		}
	}
	if len(cfg.Subtitles.Languages) == 0 {
		problems = append(problems, "subtitles.languages required")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func validateURL(name, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s required", name)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%s invalid", name)
	}
	return nil
}

func ValidatePaths(rt Runtime) error {
	abs := func(path string) string {
		out, err := filepath.Abs(path)
		if err != nil {
			return path
		}
		return filepath.Clean(out)
	}

	fuseRoot := abs(rt.FuseMountPath)
	checks := []string{
		rt.MovieLibraryPath,
		rt.TVLibraryPath,
		rt.BlockCachePath,
		rt.HeaderCachePath,
		rt.RepairWorkspacePath,
		rt.StagingNZBPath,
		rt.FailedDiagnosticsPath,
	}

	for _, target := range checks {
		clean := abs(target)
		if clean == fuseRoot || strings.HasPrefix(clean, fuseRoot+string(os.PathSeparator)) {
			return fmt.Errorf("path %s must remain outside fuse mount %s", clean, fuseRoot)
		}
	}

	if !strings.HasPrefix(abs(rt.MovieLibraryPath), filepath.Dir(filepath.Dir(fuseRoot))+string(os.PathSeparator)) {
		return fmt.Errorf("movie library path %s must live under /mnt/drakkar", rt.MovieLibraryPath)
	}
	return nil
}

func RedactedSettings(cfg Settings) map[string]any {
	return map[string]any{
		"database": map[string]any{
			"host":     cfg.Database.Host,
			"port":     cfg.Database.Port,
			"name":     cfg.Database.Name,
			"username": cfg.Database.Username,
			"password": "***",
		},
		"valkey": map[string]any{
			"host":     cfg.Valkey.Host,
			"port":     cfg.Valkey.Port,
			"password": "***",
		},
		"nzbhydra2": map[string]any{
			"url":                   cfg.NZBHydra2.URL,
			"apiKey":                "***",
			"searchCacheTtlSeconds": cfg.NZBHydra2.SearchCacheTTLSeconds,
			"feedCacheTtlSeconds":   cfg.NZBHydra2.FeedCacheTTLSeconds,
			"feedMaxResults":        cfg.NZBHydra2.FeedMaxResults,
		},
		"seerr": map[string]any{
			"url":    cfg.Seerr.URL,
			"apiKey": "***",
		},
		"usenet": map[string]any{
			"maxDownloadConnections":   cfg.Usenet.MaxDownloadConnections,
			"streamingPriorityPercent": cfg.Usenet.StreamingPriorityPct,
			"articleBufferSize":        cfg.Usenet.ArticleBufferSize,
			"providers":                redactUsenetProviders(cfg.Usenet.Providers),
		},
		"metadata": map[string]any{
			"tmdb": map[string]any{
				"apiKey": "***",
			},
			"tvdb": map[string]any{
				"apiKey": "***",
			},
		},
		"subtitles": map[string]any{
			"enabled":   cfg.Subtitles.Enabled,
			"languages": append([]string(nil), cfg.Subtitles.Languages...),
			"providers": redactSubtitleProviders(cfg.Subtitles.Providers),
		},
	}
}

func redactUsenetProviders(providers []UsenetProvider) []map[string]any {
	out := make([]map[string]any, 0, len(providers))
	for _, provider := range providers {
		out = append(out, map[string]any{
			"name":           provider.Name,
			"host":           provider.Host,
			"port":           provider.Port,
			"tls":            provider.TLS,
			"username":       provider.Username,
			"password":       "***",
			"maxConnections": provider.MaxConnections,
			"enabled":        provider.Enabled,
		})
	}
	return out
}

func redactSubtitleProviders(providers map[string]SubtitleAuth) map[string]any {
	out := make(map[string]any, len(providers))
	for name, provider := range providers {
		out[name] = map[string]any{
			"enabled":  provider.Enabled,
			"apiKey":   "***",
			"username": provider.Username,
			"password": "***",
		}
	}
	return out
}

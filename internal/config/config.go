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
	Database      DatabaseConfig      `json:"database"`
	Valkey        ValkeyConfig        `json:"valkey"`
	NZBHydra2     ServiceConfig       `json:"nzbhydra2"`
	Seerr         ServiceConfig       `json:"seerr"`
	Usenet        UsenetConfig        `json:"usenet"`
	Metadata      MetadataConfig      `json:"metadata"`
	Subtitles     SubtitlesConfig     `json:"subtitles"`
	Plex          PlexConfig          `json:"plex"`
	Jellyfin      JellyfinConfig      `json:"jellyfin"`
	Library       LibraryConfig       `json:"library"`
	Indexer       IndexerConfig       `json:"indexer"`
	Notifications NotificationsConfig `json:"notifications"`
	Rclone        RcloneConfig        `json:"rclone"`
}

// RcloneConfig holds optional rclone remote control settings.
// When RCAddr is set Drakkar calls vfs/refresh after publishing new content
// so rclone's directory cache is invalidated immediately — matching nzbdav behaviour.
type RcloneConfig struct {
	// RCAddr: rclone remote control address (e.g. "http://drakkar_rclone:5572").
	// Leave empty to disable VFS refresh (rclone dir-cache-time handles staleness).
	RCAddr string `json:"rcAddr"`
}

// NotificationsConfig holds settings for outgoing event notifications.
// Mirrors Sonarr/Radarr Settings → Connect.
type NotificationsConfig struct {
	// DiscordWebhookURL: if set, sends Discord embeds on selected events.
	DiscordWebhookURL string `json:"discordWebhookUrl"`
	// GenericWebhookURL: if set, sends a JSON POST for every selected event.
	GenericWebhookURL string `json:"genericWebhookUrl"`
	// OnGrab: fire when a release is selected for download.
	OnGrab bool `json:"onGrab"`
	// OnAvailable: fire when an item finishes importing.
	OnAvailable bool `json:"onAvailable"`
	// OnFailed: fire when an item permanently fails.
	OnFailed bool `json:"onFailed"`
}

// IndexerConfig mirrors Sonarr/Radarr Settings → Indexers.
// Defaults are applied in DefaultIndexerConfig().
type IndexerConfig struct {
	// TvRssSyncIntervalMinutes: how often to poll TV/episode RSS feeds.
	// Valid range: 10–120, or 0 to disable. Sonarr default: 15. Minimum enforced: 15.
	TvRssSyncIntervalMinutes int `json:"tvRssSyncIntervalMinutes"`

	// MovieRssSyncIntervalMinutes: how often to poll movie RSS feeds.
	// Valid range: 10–120, or 0 to disable. Radarr default: 30. Minimum enforced: 30.
	MovieRssSyncIntervalMinutes int `json:"movieRssSyncIntervalMinutes"`

	// MinimumAgeMinutes: don't grab a release younger than this.
	// Gives time for the NZB to propagate across Usenet servers. Default: 0.
	MinimumAgeMinutes int `json:"minimumAgeMinutes"`

	// RetentionDays: skip releases older than this many days (0 = unlimited).
	// Matches your Usenet provider's actual retention window. Default: 0.
	RetentionDays int `json:"retentionDays"`

	// MaximumSizeMB: reject releases larger than this (0 = unlimited). Default: 0.
	MaximumSizeMB int `json:"maximumSizeMB"`

	// SearchDelayMs: minimum milliseconds between consecutive NZBHydra2 search
	// requests. 0 means no delay (Sonarr/Radarr behaviour). Default: 0.
	SearchDelayMs int `json:"searchDelayMs"`

	// BackgroundSearchWorkers: BullMQ worker concurrency for missing-item search
	// jobs. Higher values improve backlog throughput when Hydra throttling is low.
	// Default: 12.
	BackgroundSearchWorkers int `json:"backgroundSearchWorkers"`
}

func DefaultIndexerConfig() IndexerConfig {
	return IndexerConfig{
		TvRssSyncIntervalMinutes:    15,
		MovieRssSyncIntervalMinutes: 30,
		MinimumAgeMinutes:           0,
		RetentionDays:               0,
		MaximumSizeMB:               0,
		// 0 matches Radarr/Sonarr behaviour — NZBHydra2 handles its own rate limiting.
		SearchDelayMs:           0,
		BackgroundSearchWorkers: 10,
	}
}

// PlexConfig holds the Plex Media Server connection settings.
type PlexConfig struct {
	URL        string `json:"url"`        // e.g. http://192.168.1.10:32400
	Token      string `json:"token"`      // X-Plex-Token
	SectionKey string `json:"sectionKey"` // library section key (empty = all)
}

// JellyfinConfig holds the Jellyfin Media Server connection settings.
type JellyfinConfig struct {
	URL    string `json:"url"`    // e.g. http://192.168.1.10:8096
	APIKey string `json:"apiKey"` // Jellyfin API key
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
	Priority       int    `json:"priority"`
	RetentionDays  int    `json:"retentionDays"`
	Backup         bool   `json:"backup"`
	Enabled        bool   `json:"enabled"`
}

type MetadataConfig struct {
	TMDB          APIKeyConfig `json:"tmdb"`
	TVDB          APIKeyConfig `json:"tvdb"`
	Language      string       `json:"language"`
	CacheTTLHours int          `json:"cacheTtlHours"`
}

type LibraryConfig struct {
	DefaultMovieProfile string `json:"defaultMovieProfile"`
	DefaultTvProfile    string `json:"defaultTvProfile"`
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

// LoadOrCreate loads settings from path. If the file does not exist a minimal
// settings.json is created with Docker-Compose-compatible defaults and then
// loaded. Other errors (parse, validation) are returned as-is.
func LoadOrCreate(path string) (Settings, error) {
	cfg, err := Load(path)
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "no such file") {
		return Settings{}, err
	}
	if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
		return Settings{}, fmt.Errorf("create settings dir: %w", mkErr)
	}
	blank := defaultSettings()
	data, _ := json.MarshalIndent(blank, "", "  ")
	if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
		return Settings{}, fmt.Errorf("write default settings: %w", writeErr)
	}
	return Load(path)
}

func defaultSettings() Settings {
	s := Settings{}
	s.Database = DatabaseConfig{Host: "postgres", Port: 5432, Name: "drakkar", Username: "drakkar", Password: "change-me"}
	s.Valkey = ValkeyConfig{Host: "valkey", Port: 6379}
	applyDefaults(&s)
	return s
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
	if cfg.Indexer.TvRssSyncIntervalMinutes == 0 {
		cfg.Indexer.TvRssSyncIntervalMinutes = DefaultIndexerConfig().TvRssSyncIntervalMinutes
	}
	if cfg.Indexer.MovieRssSyncIntervalMinutes == 0 {
		cfg.Indexer.MovieRssSyncIntervalMinutes = DefaultIndexerConfig().MovieRssSyncIntervalMinutes
	}
	if cfg.Indexer.BackgroundSearchWorkers <= 0 {
		cfg.Indexer.BackgroundSearchWorkers = DefaultIndexerConfig().BackgroundSearchWorkers
	}
	if len(cfg.Subtitles.Languages) == 0 {
		cfg.Subtitles.Languages = []string{"en"}
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
	// External service URLs are optional — the app starts without them.
	if cfg.NZBHydra2.URL != "" {
		if err := validateURL("nzbhydra2.url", cfg.NZBHydra2.URL); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if cfg.Seerr.URL != "" {
		if err := validateURL("seerr.url", cfg.Seerr.URL); err != nil {
			problems = append(problems, err.Error())
		}
	}
	// Validate individual providers only when present.
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

// Save validates cfg and atomically writes it to path as indented JSON.
func Save(path string, cfg Settings) error {
	applyDefaults(&cfg)
	if err := validate(cfg); err != nil {
		return fmt.Errorf("invalid settings: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write settings: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("save settings: %w", err)
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

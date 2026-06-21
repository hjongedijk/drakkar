// Package plex provides a minimal Plex Media Server client for triggering
// library section refreshes after Drakkar publishes a new media file.
package plex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

// Client calls the Plex HTTP API.
type Client struct {
	serverURL  string
	token      string
	httpClient *http.Client
}

// Library is a Plex library section.
type Library struct {
	Key       string   `json:"key"`
	Title     string   `json:"title"`
	Type      string   `json:"type"` // "movie" or "show"
	Agent     string   `json:"agent"`
	Locations []string `json:"locations,omitempty"`
}

// TestResult is returned from a connection test.
type TestResult struct {
	OK         bool      `json:"ok"`
	ServerName string    `json:"serverName"`
	Libraries  []Library `json:"libraries"`
	Error      string    `json:"error,omitempty"`
}

func NewClient(serverURL, token string) *Client {
	return &Client{
		serverURL:  strings.TrimRight(serverURL, "/"),
		token:      token,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *Client) Enabled() bool {
	return c != nil && strings.TrimSpace(c.serverURL) != "" && strings.TrimSpace(c.token) != ""
}

// Test verifies connectivity and returns the server name + library list.
func (c *Client) Test(ctx context.Context) TestResult {
	if !c.Enabled() {
		return TestResult{Error: "plex not configured"}
	}
	// Fetch server info
	type serverInfo struct {
		MediaContainer struct {
			FriendlyName string `json:"friendlyName"`
		} `json:"MediaContainer"`
	}
	var info serverInfo
	if err := c.get(ctx, "/", &info); err != nil {
		return TestResult{Error: err.Error()}
	}
	libs, err := c.Libraries(ctx)
	if err != nil {
		return TestResult{OK: true, ServerName: info.MediaContainer.FriendlyName, Error: err.Error()}
	}
	return TestResult{
		OK:         true,
		ServerName: info.MediaContainer.FriendlyName,
		Libraries:  libs,
	}
}

// Libraries returns all library sections from the Plex server.
func (c *Client) Libraries(ctx context.Context) ([]Library, error) {
	type response struct {
		MediaContainer struct {
			Directory []struct {
				Key      string `json:"key"`
				Title    string `json:"title"`
				Type     string `json:"type"`
				Agent    string `json:"agent"`
				Location []struct {
					Path string `json:"path"`
				} `json:"Location"`
			} `json:"Directory"`
		} `json:"MediaContainer"`
	}
	var resp response
	if err := c.get(ctx, "/library/sections", &resp); err != nil {
		return nil, err
	}
	out := make([]Library, 0, len(resp.MediaContainer.Directory))
	for _, d := range resp.MediaContainer.Directory {
		lib := Library{Key: d.Key, Title: d.Title, Type: d.Type, Agent: d.Agent}
		for _, location := range d.Location {
			if strings.TrimSpace(location.Path) != "" {
				lib.Locations = append(lib.Locations, location.Path)
			}
		}
		out = append(out, lib)
	}
	return out, nil
}

// RefreshPathAuto triggers a path refresh using either the configured section
// key or automatic section detection from Plex library root locations.
func (c *Client) RefreshPathAuto(ctx context.Context, preferredSectionKey, filePath string) error {
	if !c.Enabled() {
		return nil
	}
	filePath = filepath.Clean(strings.TrimSpace(filePath))
	if filePath == "" {
		if preferredSectionKey != "" {
			return c.RefreshSection(ctx, preferredSectionKey)
		}
		return nil
	}
	if preferredSectionKey != "" {
		return c.RefreshPath(ctx, preferredSectionKey, filePath)
	}
	libs, err := c.Libraries(ctx)
	if err != nil {
		return err
	}
	candidates := matchingLibrariesForPath(libs, filePath)
	if len(candidates) == 0 {
		candidates = libs
	}
	var firstErr error
	for _, lib := range candidates {
		if err := c.RefreshPath(ctx, lib.Key, filePath); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func matchingLibrariesForPath(libs []Library, filePath string) []Library {
	filePath = filepath.Clean(filePath)
	var out []Library
	for _, lib := range libs {
		for _, root := range lib.Locations {
			root = filepath.Clean(strings.TrimSpace(root))
			if root == "" {
				continue
			}
			if filePath == root || strings.HasPrefix(filePath, root+string(filepath.Separator)) {
				out = append(out, lib)
				break
			}
		}
	}
	return out
}

// RefreshSection triggers a full scan of a library section by key.
// If key is empty, refreshes all sections.
func (c *Client) RefreshSection(ctx context.Context, sectionKey string) error {
	if !c.Enabled() {
		return nil
	}
	if sectionKey == "" {
		libs, err := c.Libraries(ctx)
		if err != nil {
			return err
		}
		for _, lib := range libs {
			if err := c.refreshSection(ctx, lib.Key); err != nil {
				return err
			}
		}
		return nil
	}
	return c.refreshSection(ctx, sectionKey)
}

// RefreshPath triggers a targeted scan of a specific file path within Plex.
// This is faster than a full section scan.
func (c *Client) RefreshPath(ctx context.Context, sectionKey, filePath string) error {
	if !c.Enabled() {
		return nil
	}
	endpoint := fmt.Sprintf("/library/sections/%s/refresh?path=%s", sectionKey, url.QueryEscape(filePath))
	return c.get(ctx, endpoint, nil)
}

func (c *Client) refreshSection(ctx context.Context, sectionKey string) error {
	return c.get(ctx, fmt.Sprintf("/library/sections/%s/refresh", sectionKey), nil)
}

func (c *Client) get(ctx context.Context, path string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.serverURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Plex-Token", c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("plex HTTP %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

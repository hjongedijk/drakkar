// Package rclone provides a minimal client for rclone's remote control (RC) API.
// Used after publishing new symlinks so rclone clears its VFS directory cache
// and Plex sees new content immediately — same as nzbdav's RcloneVfsForget.
package rclone

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client calls rclone's RC API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *Client) Enabled() bool {
	return c != nil && c.baseURL != ""
}

// ForgetVfsPaths calls vfs/forget to clear rclone's directory cache for the
// given paths. After this, rclone re-reads those directories from the WebDAV
// server and Plex sees newly added content immediately.
// Matches nzbdav's DavDatabaseContext.RcloneVfsForget() behaviour.
func (c *Client) ForgetVfsPaths(ctx context.Context, paths []string) error {
	if !c.Enabled() || len(paths) == 0 {
		return nil
	}
	// rclone RC vfs/forget accepts: {"dir": "/path1", "dir2": "/path2", ...}
	body := make(map[string]string, len(paths))
	for i, p := range paths {
		key := "dir"
		if i > 0 {
			key = fmt.Sprintf("dir%d", i+1)
		}
		body[key] = p
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/vfs/forget", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

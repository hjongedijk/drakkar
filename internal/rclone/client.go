package rclone

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client calls the rclone Remote Control (RC) API.
// It is used to refresh rclone's VFS directory cache after new content is
// published so Plex sees new files immediately — matching nzbdav's
// RcloneClient.RefreshVfsPaths() behaviour.
type Client struct {
	rcAddr     string
	httpClient *http.Client
}

func NewClient(rcAddr string) *Client {
	return &Client{
		rcAddr: strings.TrimRight(rcAddr, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// RefreshPath posts vfs/refresh for a single path. Errors are non-fatal
// (rclone dir-cache-time handles staleness when RC is unavailable).
func (c *Client) RefreshPath(ctx context.Context, path string) error {
	if c == nil || c.rcAddr == "" {
		return nil
	}
	endpoint := c.rcAddr + "/vfs/refresh"
	form := url.Values{}
	form.Set("dir", path)
	form.Set("recursive", "false")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("vfs/refresh: status %d", resp.StatusCode)
	}
	return nil
}

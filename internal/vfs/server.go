// Package vfs provides an HTTP server that streams virtual NZB files on demand,
// replacing the FUSE filesystem. Clients (Plex, Jellyfin, rclone) access content
// via HTTP Range requests — the same protocol nzbdav uses.
package vfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/hjongedijk/drakkar/internal/stream"
	"golang.org/x/net/webdav"
)

// FileOpener opens a virtual media file by ID.
type FileOpener interface {
	OpenVirtualMediaFile(ctx context.Context, virtualFileID int64) (stream.VirtualMediaFile, error)
}

// DB is the combined interface needed by the VFS server.
type DB interface {
	FileOpener
	VirtualFileLister
}

// Server is the HTTP content server. Mount it under /content on the main router.
// Files are served at /content/{id}/{filename} with full Range support.
// A WebDAV endpoint at /dav/ allows rclone to mount the content as a filesystem.
type Server struct {
	opener FileOpener
	dav    *webdav.Handler
}

func NewServer(db DB) *Server {
	davHandler := &webdav.Handler{
		FileSystem: newDAVFS(db, db),
		LockSystem: webdav.NewMemLS(),
	}
	return &Server{opener: db, dav: davHandler}
}

// Register mounts content + WebDAV routes on r.
func (s *Server) Register(r chi.Router) {
	r.Get("/content/{id}/{filename}", s.serveFile)
	r.Head("/content/{id}/{filename}", s.serveFile)
	// WebDAV mount — PROPFIND, GET, HEAD, OPTIONS and other WebDAV methods.
	// chi.Handle only covers known methods; use a catch-all middleware for /dav/*.
	davStripped := http.StripPrefix("/dav", s.dav)
	davHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		davStripped.ServeHTTP(w, req)
	})
	r.Mount("/dav", davHandler)
}

func (s *Server) serveFile(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	filename := chi.URLParam(r, "filename")

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	f, err := s.opener.OpenVirtualMediaFile(r.Context(), id)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	size := f.Size()
	if filename == "" {
		filename = f.Name()
	}

	// Content-Type from extension.
	ext := strings.ToLower(path.Ext(filename))
	ct := "application/octet-stream"
	switch ext {
	case ".mkv":
		ct = "video/x-matroska"
	case ".mp4":
		ct = "video/mp4"
	case ".avi":
		ct = "video/x-msvideo"
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, filename))

	// Parse Range header.
	rangeHdr := r.Header.Get("Range")
	start, end := int64(0), size-1
	isRange := false

	if rangeHdr != "" {
		parsed, parseErr := parseByteRange(rangeHdr, size)
		if parseErr != nil {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start, end = parsed[0], parsed[1]
		isRange = true
	}

	length := end - start + 1

	if r.Method == http.MethodHead {
		if isRange {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
			w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusOK)
		}
		return
	}

	if isRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
	}

	const chunkSize = 256 << 10 // 256 KB per read
	buf := make([]byte, chunkSize)
	pos := start
	for pos <= end {
		toRead := int64(len(buf))
		if pos+toRead-1 > end {
			toRead = end - pos + 1
		}
		n, readErr := f.ReadAt(r.Context(), buf[:toRead], pos)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return
			}
			return
		}
		pos += int64(n)
	}
}

// parseByteRange parses a "bytes=start-end" Range header.
func parseByteRange(hdr string, size int64) ([2]int64, error) {
	hdr = strings.TrimPrefix(hdr, "bytes=")
	parts := strings.SplitN(hdr, "-", 2)
	if len(parts) != 2 {
		return [2]int64{}, fmt.Errorf("invalid range")
	}
	var start, end int64
	var err error
	if parts[0] == "" {
		// Suffix range: bytes=-500 → last 500 bytes
		suffixLen, e := strconv.ParseInt(parts[1], 10, 64)
		if e != nil || suffixLen <= 0 {
			return [2]int64{}, fmt.Errorf("invalid suffix range")
		}
		start = size - suffixLen
		if start < 0 {
			start = 0
		}
		end = size - 1
	} else {
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return [2]int64{}, err
		}
		if parts[1] == "" {
			end = size - 1
		} else {
			end, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return [2]int64{}, err
			}
		}
	}
	if start < 0 || end >= size || start > end {
		return [2]int64{}, fmt.Errorf("range out of bounds")
	}
	return [2]int64{start, end}, nil
}

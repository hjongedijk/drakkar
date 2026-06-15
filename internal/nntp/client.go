package nntp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/hjongedijk/drakkar/internal/config"
)

type ArticleClient struct {
	provider config.UsenetProvider
	timeout  time.Duration
}

func NewArticleClient(provider config.UsenetProvider) *ArticleClient {
	return &ArticleClient{
		provider: provider,
		timeout:  30 * time.Second,
	}
}

func (c *ArticleClient) Name() string {
	return "usenet:" + c.provider.Name
}

func (c *ArticleClient) Probe(ctx context.Context) error {
	session, err := c.NewSession(ctx)
	if err != nil {
		return err
	}
	return session.Close()
}

func (c *ArticleClient) Body(ctx context.Context, messageID string) ([]byte, error) {
	session, err := c.NewSession(ctx)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	return session.Body(ctx, messageID)
}

func (c *ArticleClient) NewSession(ctx context.Context) (BodySession, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	// Set a deadline covering the entire greeting + auth handshake.
	// tls.DialWithDialer does not accept a context, so without this the
	// greeting/auth reads can block indefinitely if the server is slow.
	if err := conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		conn.Close()
		return nil, err
	}
	session := &clientSession{
		conn:    conn,
		reader:  bufio.NewReader(conn),
		writer:  bufio.NewWriter(conn),
		timeout: c.timeout,
	}
	if _, _, err := readStatusLine(session.reader); err != nil {
		conn.Close()
		return nil, err
	}
	if c.provider.Username != "" {
		if err := writeCommand(session.writer, "AUTHINFO USER "+c.provider.Username); err != nil {
			conn.Close()
			return nil, err
		}
		code, _, err := readStatusLine(session.reader)
		if err != nil {
			conn.Close()
			return nil, err
		}
		if code == 381 {
			if err := writeCommand(session.writer, "AUTHINFO PASS "+c.provider.Password); err != nil {
				conn.Close()
				return nil, err
			}
			if _, _, err := readStatusLine(session.reader); err != nil {
				conn.Close()
				return nil, err
			}
		}
	}
	// Clear the handshake deadline — per-command deadlines are set in Body/Stat.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, err
	}
	return session, nil
}

type clientSession struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	timeout time.Duration
}

func (s *clientSession) Body(ctx context.Context, messageID string) ([]byte, error) {
	deadline := time.Now().Add(s.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := s.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if err := writeCommand(s.writer, "BODY "+normalizeMessageID(messageID)); err != nil {
		return nil, err
	}
	code, _, err := readStatusLine(s.reader)
	if err != nil {
		return nil, err
	}
	if code != 222 {
		return nil, fmt.Errorf("unexpected BODY status %d", code)
	}
	return readMultilineBody(s.reader)
}

func (s *clientSession) Stat(ctx context.Context, messageID string) error {
	deadline := time.Now().Add(s.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := s.conn.SetDeadline(deadline); err != nil {
		return err
	}
	if err := writeCommand(s.writer, "STAT "+normalizeMessageID(messageID)); err != nil {
		return err
	}
	code, _, err := readStatusLine(s.reader)
	if err != nil {
		return err
	}
	if code == 430 {
		return ErrArticleMissing
	}
	if code != 223 {
		return fmt.Errorf("unexpected STAT status %d", code)
	}
	return nil
}

func (s *clientSession) Close() error {
	return s.conn.Close()
}

func (c *ArticleClient) dial(ctx context.Context) (net.Conn, error) {
	address := net.JoinHostPort(c.provider.Host, strconv.Itoa(c.provider.Port))
	dialer := &net.Dialer{Timeout: c.timeout}
	if c.provider.TLS {
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName: c.provider.Host,
			MinVersion: tls.VersionTLS12,
		})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	return dialer.DialContext(ctx, "tcp", address)
}

func writeCommand(writer *bufio.Writer, command string) error {
	if _, err := writer.WriteString(command + "\r\n"); err != nil {
		return err
	}
	return writer.Flush()
}

func readStatusLine(reader *bufio.Reader) (int, string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return 0, "", err
	}
	line = strings.TrimSpace(line)
	if len(line) < 3 {
		return 0, "", errors.New("short nntp status line")
	}
	code, err := strconv.Atoi(line[:3])
	if err != nil {
		return 0, "", err
	}
	return code, strings.TrimSpace(line[3:]), nil
}

func readMultilineBody(reader *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == ".\r\n" || line == ".\n" {
			break
		}
		if strings.HasPrefix(line, "..") {
			line = line[1:]
		}
		out = append(out, []byte(line)...)
	}
	return out, nil
}

func normalizeMessageID(messageID string) string {
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		return messageID
	}
	if strings.HasPrefix(messageID, "<") && strings.HasSuffix(messageID, ">") {
		return messageID
	}
	return "<" + strings.Trim(messageID, "<>") + ">"
}

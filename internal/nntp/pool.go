package nntp

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrArticleMissing is returned by Stat when the NNTP server confirms the
// article does not exist (430 No Such Article).
var ErrArticleMissing = errors.New("article missing")

type BodySession interface {
	Body(ctx context.Context, messageID string) ([]byte, error)
	// Stat checks article existence without downloading the body.
	// Returns ErrArticleMissing when the server responds 430.
	Stat(ctx context.Context, messageID string) error
	Close() error
}

type SessionFactory func(ctx context.Context) (BodySession, error)

// idleTimeout matches nzbdav's ConnectionPool default: close NNTP connections
// that have been idle for 30 seconds. This frees server-side resources when
// no playback is active and the background queue is quiet.
const idleTimeout = 30 * time.Second

type pooledSession struct {
	session   BodySession
	idleSince time.Time
}

type PooledSource struct {
	factory SessionFactory
	maxOpen int

	mu   sync.Mutex
	open int
	idle chan pooledSession
}

func NewPooledSource(factory SessionFactory, maxOpen int) *PooledSource {
	if maxOpen <= 0 {
		maxOpen = 1
	}
	p := &PooledSource{
		factory: factory,
		maxOpen: maxOpen,
		idle:    make(chan pooledSession, maxOpen),
	}
	go p.sweepLoop()
	return p
}

// sweepLoop closes connections idle longer than idleTimeout.
// Period = idleTimeout/2, matching nzbdav's SweepLoop.
func (p *PooledSource) sweepLoop() {
	ticker := time.NewTicker(idleTimeout / 2)
	defer ticker.Stop()
	for range ticker.C {
		p.sweepOnce()
	}
}

func (p *PooledSource) sweepOnce() {
	cutoff := time.Now().Add(-idleTimeout)
	var keep []pooledSession
	for {
		select {
		case s := <-p.idle:
			if s.idleSince.Before(cutoff) {
				_ = s.session.Close()
				p.mu.Lock()
				p.open--
				p.mu.Unlock()
			} else {
				keep = append(keep, s)
			}
		default:
			goto done
		}
	}
done:
	for _, s := range keep {
		select {
		case p.idle <- s:
		default:
			_ = s.session.Close()
			p.mu.Lock()
			p.open--
			p.mu.Unlock()
		}
	}
}

func (p *PooledSource) Body(ctx context.Context, messageID string) ([]byte, error) {
	if p == nil || p.factory == nil {
		return nil, errors.New("pooled source unavailable")
	}
	session, err := p.acquire(ctx)
	if err != nil {
		return nil, err
	}
	body, err := session.Body(ctx, messageID)
	if err != nil {
		p.discard(session)
		return nil, err
	}
	p.release(session)
	return body, nil
}

func (p *PooledSource) Stat(ctx context.Context, messageID string) error {
	if p == nil || p.factory == nil {
		return errors.New("pooled source unavailable")
	}
	session, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	err = session.Stat(ctx, messageID)
	if err != nil {
		// ErrArticleMissing (430) means the article doesn't exist but the
		// connection itself is still valid — release rather than discard so we
		// don't create a new TCP+TLS handshake for every missing segment.
		if errors.Is(err, ErrArticleMissing) {
			p.release(session)
		} else {
			p.discard(session)
		}
		return err
	}
	p.release(session)
	return nil
}

func (p *PooledSource) acquire(ctx context.Context) (BodySession, error) {
	// Check ctx before borrowing — cancelled read-ahead must not steal
	// a pooled session from an interactive reader.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// Drain stale idle sessions, take first fresh one.
	for {
		select {
		case s := <-p.idle:
			if time.Since(s.idleSince) > idleTimeout {
				_ = s.session.Close()
				p.mu.Lock()
				p.open--
				p.mu.Unlock()
				continue
			}
			return s.session, nil
		default:
			goto noIdle
		}
	}
noIdle:
	p.mu.Lock()
	if p.open < p.maxOpen {
		p.open++
		p.mu.Unlock()
		session, err := p.factory(ctx)
		if err != nil {
			p.mu.Lock()
			p.open--
			p.mu.Unlock()
			return nil, err
		}
		return session, nil
	}
	p.mu.Unlock()

	// All connections in use — wait.
	select {
	case s := <-p.idle:
		if time.Since(s.idleSince) > idleTimeout {
			_ = s.session.Close()
			p.mu.Lock()
			p.open--
			p.mu.Unlock()
			return p.acquire(ctx) // retry with decremented open count
		}
		return s.session, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *PooledSource) release(session BodySession) {
	select {
	case p.idle <- pooledSession{session: session, idleSince: time.Now()}:
	default:
		p.discard(session)
	}
}

func (p *PooledSource) discard(session BodySession) {
	_ = session.Close()
	p.mu.Lock()
	p.open--
	p.mu.Unlock()
}

// Stats returns current active and idle connection counts.
func (p *PooledSource) Stats() (active, idle int) {
	p.mu.Lock()
	active = p.open
	p.mu.Unlock()
	idle = len(p.idle)
	return
}

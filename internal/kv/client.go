package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is a retrying HTTP client that follows X-Raft-Leader and uses a
// stable ClientID with monotonic sequence numbers.
type Client struct {
	mu       sync.Mutex
	addrs    []string
	leader   string
	skip     map[string]struct{}
	clientID uint64
	seq      uint64
	http     *http.Client
}

// NewClient talks to the given base URLs (e.g. http://127.0.0.1:8001).
func NewClient(addrs []string) *Client {
	cp := append([]string(nil), addrs...)
	return &Client{
		addrs:    cp,
		leader:   cp[0],
		skip:     make(map[string]struct{}),
		clientID: newClientID(),
		http:     &http.Client{Timeout: 3 * time.Second},
	}
}

func newClientID() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	id := binary.LittleEndian.Uint64(b[:])
	if id == 0 {
		id = 1
	}
	return id
}

// Get returns the value or ErrNotFound.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	resp, err := c.do(ctx, http.MethodGet, "/kv/"+url.PathEscape(key), "")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get %s: status %d: %s", key, resp.StatusCode, body)
	}
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

// Put stores value at key.
func (c *Client) Put(ctx context.Context, key, value string) error {
	resp, err := c.do(ctx, http.MethodPut, "/kv/"+url.PathEscape(key), value)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put %s: status %d: %s", key, resp.StatusCode, body)
	}
	return nil
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/kv/"+url.PathEscape(key), "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete %s: status %d: %s", key, resp.StatusCode, body)
	}
	return nil
}

// Append concatenates value onto key and returns the new contents.
func (c *Client) Append(ctx context.Context, key, value string) (string, error) {
	resp, err := c.do(ctx, http.MethodPost, "/kv/"+url.PathEscape(key), value)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("append %s: status %d: %s", key, resp.StatusCode, body)
	}
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

var ErrNotFound = fmt.Errorf("key not found")

func (c *Client) do(ctx context.Context, method, path, body string) (*http.Response, error) {
	c.mu.Lock()
	c.seq++
	seq := c.seq
	clientID := c.clientID
	c.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < 40; attempt++ {
		c.mu.Lock()
		target := c.leader
		if target == "" {
			target = c.addrs[attempt%len(c.addrs)]
		}
		c.mu.Unlock()

		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(target, "/")+path, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set(headerClientID, strconv.FormatUint(clientID, 10))
		req.Header.Set(headerSeq, strconv.FormatUint(seq, 10))
		if method == http.MethodPut || method == http.MethodPost {
			req.Header.Set("Content-Type", "text/plain")
		}

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			c.noteUnreachable(target)
			c.rotate(target)
			time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
			loc := resp.Header.Get(headerLeader)
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			c.follow(loc, target)
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
			continue
		}
		return resp, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("retries exhausted")
	}
	return nil, lastErr
}

func (c *Client) noteUnreachable(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range c.addrs {
		if a == addr {
			return
		}
	}
	if addr != "" {
		c.skip[addr] = struct{}{}
	}
}

func (c *Client) follow(advertised, failed string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if advertised != "" {
		if _, bad := c.skip[advertised]; !bad {
			c.leader = advertised
			return
		}
	}
	c.rotateLocked(failed)
}

func (c *Client) rotate(failed string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotateLocked(failed)
}

func (c *Client) rotateLocked(failed string) {
	for _, a := range c.addrs {
		if a == failed {
			continue
		}
		if _, bad := c.skip[a]; bad {
			continue
		}
		c.leader = a
		return
	}
	for _, a := range c.addrs {
		if a != failed {
			c.leader = a
			return
		}
	}
	if len(c.addrs) > 0 {
		c.leader = c.addrs[0]
	}
}

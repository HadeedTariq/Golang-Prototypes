package proxysql

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ConnectionPool struct {
	mu          sync.Mutex
	connections chan *http.Client
	maxSize     int
}

func NewConnectionPool(size int) *ConnectionPool {
	// ~ so here in case of the databases we are giving connections to the databses and like the databases takes the connection
	pool := &ConnectionPool{
		connections: make(chan *http.Client, size),
		maxSize:     size,
	}
	for i := 0; i < size; i++ {
		client := &http.Client{}
		pool.connections <- client
	}
	return pool
}

func (cp *ConnectionPool) Get() (*http.Client, error) {
	select {
	case conn := <-cp.connections:
		return conn, nil
	default:
		return nil, errors.New("no available connections in pool")
	}
}

func (cp *ConnectionPool) Put(conn *http.Client) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if len(cp.connections) < cp.maxSize {
		cp.connections <- conn
	}
}

// ~ ok ok so how the things are working over there is proxy is basically define because here with in the proxy we define the master and the replicas

type CacheEntry struct {
	Data      any
	ExpiresAt time.Time
}

type CachedResponse struct {
	results map[string]CacheEntry
	mu      sync.Mutex
	ttl     time.Duration
}

func NewCache(ttl time.Duration) *CachedResponse {
	return &CachedResponse{
		results: make(map[string]CacheEntry),
		ttl:     ttl,
	}
}

func (c *CachedResponse) Push(query string, data any) {
	c.mu.Lock()

	defer c.mu.Unlock()

	c.results[query] = CacheEntry{
		Data:      data,
		ExpiresAt: time.Now().Add(c.ttl),
	}
}

func (c *CachedResponse) Get(query string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.results[query]
	if !exists {
		return nil, false
	}

	if time.Now().After(entry.ExpiresAt) {
		delete(c.results, query)
		return nil, false
	}

	return entry.Data, true
}

type Proxy struct {
	readDb   []string
	writeDb  string
	pool     *ConnectionPool
	mu       sync.Mutex
	lastUsed int
}

func NewProxy(
	readDb []string,
	writeDb string,
	pool *ConnectionPool,
) *Proxy {
	return &Proxy{
		readDb:   readDb,
		writeDb:  writeDb,
		pool:     pool,
		lastUsed: -1,
	}
}

func (px *Proxy) HandleQuery(query string) ([]byte, error) {
	conn, err := px.pool.Get()

	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer px.pool.Put(conn)
	targetUrl := px.RouteQuery(query)

	resp, err := conn.Post(targetUrl, "", bytes.NewBuffer([]byte(query)))

	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	return body, nil
}

func (px *Proxy) RouteQuery(query string) string {
	q := strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(q, "SELECT") {
		return px.pickReadReplica() // here put a random or based on load mechanism
	}

	return px.writeDb
}

func (px *Proxy) pickReadReplica() string {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.lastUsed = (px.lastUsed + 1) % len(px.readDb)
	return px.readDb[px.lastUsed]
}

func main() {
	pool := NewConnectionPool(10)
	readReplicas := []string{"http://replica1:109", "http://replica1:120"}
	master := "http://master:120"
	proxy := NewProxy(readReplicas, master, pool)

	proxy.HandleQuery("SELECT * from users")
}

// ~ so the thing that it performs is like it makes the request to the database servers
// ~ it manages and handle the connection polling with in that
// ~ it handle the cache related stuff with in that like cache the query results

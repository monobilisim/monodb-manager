package api

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"html/template"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

// globalConfig is read by query helpers; it is assigned once during startup
// before any request is served.
var globalConfig *Config

// PostgreSQLServer represents a Patroni-managed PostgreSQL cluster configuration
type PostgreSQLServer struct {
	Name     string `yaml:"name" json:"name"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"-"`
	DBName   string `yaml:"dbname" json:"dbname"`
	SSLMode  string `yaml:"sslmode" json:"sslmode"`

	// Patroni configuration
	PatroniNodes    []string `yaml:"patroni_nodes" json:"patroni_nodes"`                   // List of Patroni REST API endpoints
	PatroniPort     int      `yaml:"patroni_port,omitempty" json:"patroni_port,omitempty"` // Patroni REST API port (default 8008)
	ConnectToLeader bool     `yaml:"connect_to_leader,omitempty" json:"connect_to_leader"` // Only connect to leader
}

// AuthConfig configures panel authentication. Legacy username/password are
// kept for seeding the first admin and for Basic Auth fallback.
type AuthConfig struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	Server       string `yaml:"server"`
	SessionTTL   string `yaml:"session_ttl"`
	CookieSecure *bool  `yaml:"cookie_secure"`
	Disabled     bool   `yaml:"disabled"`
}

func (c AuthConfig) sessionTTL() time.Duration {
	if c.SessionTTL == "" {
		return 12 * time.Hour
	}
	ttl, err := time.ParseDuration(c.SessionTTL)
	if err != nil || ttl <= 0 {
		log.Printf("Warning: invalid auth.session_ttl %q, using 12h", c.SessionTTL)
		return 12 * time.Hour
	}
	return ttl
}

func (c AuthConfig) secureCookies() bool {
	return c.CookieSecure == nil || *c.CookieSecure
}

// Config holds database connection parameters and application settings
type Config struct {
	Servers []PostgreSQLServer `yaml:"servers"`
	Auth    AuthConfig         `yaml:"auth"`

	LogFile string `yaml:"log_file"`

	// PMM status iframe URL for the status page
	PMMStatusURL string `yaml:"pmm_status_url"`

	// Query Analytics iframe URL
	PMMQanURL string `yaml:"pmm_qan_url"`

	// HAProxy port badges
	Ports []HAProxyPort `yaml:"ports"`

	// Services configuration
	Services []Service `yaml:"services"`

	// Refresh intervals (in seconds)
	BadgeRefreshInterval int `yaml:"badge_refresh_interval"`

	// Users to ignore in active queries view
	IgnoreUsers []string `yaml:"ignore_users"`
}

// PatroniMember represents a member in the Patroni cluster
type PatroniMember struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Role     string `json:"role"`
	State    string `json:"state"`
	Timeline int    `json:"timeline"`
	Lag      int    `json:"lag"`
}

// PatroniCluster represents the Patroni cluster information
type PatroniCluster struct {
	Members []PatroniMember `json:"members"`
	Leader  string          `json:"leader"`
	Sync    []string        `json:"sync_standby"`
}

// BackendConn represents a connection to a specific backend with its PID and Patroni member info
type BackendConn struct {
	DB         *sql.DB
	PID        int
	MemberName string // Patroni member name
	Host       string // Member host
	Port       int    // Member port
	Role       string // Member role (leader, replica, etc.)
}

// ConnectionManager manages database connections to multiple PostgreSQL servers.
// Backends are kept leader-first so callers that need a writable connection
// (CREATE/DROP ROLE, grants) always land on the primary.
type ConnectionManager struct {
	backends  map[string][]BackendConn
	configs   map[string]PostgreSQLServer
	mutex     sync.RWMutex
	connectMu sync.Mutex // serialises topology discovery + reconnection
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(servers []PostgreSQLServer) *ConnectionManager {
	cm := &ConnectionManager{
		backends: make(map[string][]BackendConn),
		configs:  make(map[string]PostgreSQLServer),
	}

	for _, server := range servers {
		cm.configs[server.Name] = server
	}

	return cm
}

// GetConnection returns a connection for the specified server, preferring the
// leader. Writes issued through it therefore reach the primary.
func (cm *ConnectionManager) GetConnection(serverName string) (*sql.DB, error) {
	if backends := cm.GetBackendConnections(serverName); len(backends) > 0 {
		return backends[0].DB, nil
	}
	return cm.establishConnection(serverName)
}

// GetBackendConnections returns the live backend connections for a server.
// Health checks run outside the lock; stale connections are dropped by identity
// so a concurrent reconnect is never clobbered.
func (cm *ConnectionManager) GetBackendConnections(serverName string) []BackendConn {
	cm.mutex.RLock()
	backends := append([]BackendConn(nil), cm.backends[serverName]...)
	cm.mutex.RUnlock()
	if len(backends) == 0 {
		return nil
	}

	valid := make([]BackendConn, 0, len(backends))
	var stale []*sql.DB
	for _, backend := range backends {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := backend.DB.PingContext(ctx)
		cancel()
		if err == nil {
			valid = append(valid, backend)
			continue
		}
		log.Printf("Backend connection to %s (PID %d) is stale, removing: %v", serverName, backend.PID, err)
		stale = append(stale, backend.DB)
	}

	if len(stale) > 0 {
		cm.mutex.Lock()
		remaining := cm.backends[serverName][:0]
		for _, backend := range cm.backends[serverName] {
			if !containsDB(stale, backend.DB) {
				remaining = append(remaining, backend)
			}
		}
		cm.backends[serverName] = remaining
		cm.mutex.Unlock()

		for _, db := range stale {
			_ = db.Close()
		}
	}

	return valid
}

func containsDB(dbs []*sql.DB, target *sql.DB) bool {
	for _, db := range dbs {
		if db == target {
			return true
		}
	}
	return false
}

// discoverPatroniCluster queries Patroni REST API to discover cluster members
func (cm *ConnectionManager) discoverPatroniCluster(config PostgreSQLServer) (*PatroniCluster, error) {
	patroniPort := config.PatroniPort
	if patroniPort == 0 {
		patroniPort = 8008 // Default Patroni REST API port
	}

	client := &http.Client{Timeout: 5 * time.Second}

	// Try each Patroni node to get cluster information
	for _, node := range config.PatroniNodes {
		clusterURL := fmt.Sprintf("http://%s/cluster", net.JoinHostPort(node, strconv.Itoa(patroniPort)))
		resp, err := client.Get(clusterURL)
		if err != nil {
			log.Printf("Failed to connect to Patroni node %s: %v", node, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			log.Printf("Patroni node %s returned status %d", node, resp.StatusCode)
			continue
		}

		var cluster PatroniCluster
		decodeErr := json.NewDecoder(resp.Body).Decode(&cluster)
		_ = resp.Body.Close()
		if decodeErr != nil {
			log.Printf("Failed to decode Patroni response from %s: %v", node, decodeErr)
			continue
		}

		log.Printf("Successfully discovered Patroni cluster from node %s: %d members, leader: %s",
			node, len(cluster.Members), cluster.Leader)
		// Some Patroni versions omit the top-level leader field; derive it
		// from the member roles so callers always get a name.
		if cluster.Leader == "" {
			for _, member := range cluster.Members {
				if member.Role == "leader" {
					cluster.Leader = member.Name
					break
				}
			}
		}
		return &cluster, nil
	}

	return nil, fmt.Errorf("failed to discover Patroni cluster from any node")
}

// establishPatroniConnection creates connections to Patroni cluster members
func (cm *ConnectionManager) establishPatroniConnection(serverName string) (*sql.DB, error) {
	config, exists := cm.configs[serverName]
	if !exists {
		return nil, fmt.Errorf("server %s not found in configuration", serverName)
	}

	cm.connectMu.Lock()
	defer cm.connectMu.Unlock()

	// Another goroutine may have connected while we waited for the lock.
	cm.mutex.RLock()
	existing := cm.backends[serverName]
	cm.mutex.RUnlock()
	if len(existing) > 0 {
		return existing[0].DB, nil
	}

	cluster, err := cm.discoverPatroniCluster(config)
	if err != nil {
		return nil, fmt.Errorf("failed to discover Patroni cluster for %s: %w", serverName, err)
	}

	var leaders, replicas []BackendConn

	for _, member := range cluster.Members {
		if config.ConnectToLeader && member.Role != "leader" {
			continue
		}

		db, err := cm.createSingleConnection(config, member.Host, member.Port)
		if err != nil {
			log.Printf("Failed to connect to Patroni member %s (%s:%d): %v",
				member.Name, member.Host, member.Port, err)
			continue
		}

		var pid int
		if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&pid); err != nil {
			log.Printf("Failed to get backend PID for Patroni member %s: %v", member.Name, err)
			_ = db.Close()
			continue
		}

		backend := BackendConn{DB: db, PID: pid, MemberName: member.Name, Host: member.Host, Port: member.Port, Role: member.Role}
		if member.Role == "leader" {
			leaders = append(leaders, backend)
		} else {
			replicas = append(replicas, backend)
		}

		log.Printf("Connected to Patroni member %s (%s:%d) - Role: %s, State: %s, PID: %d",
			member.Name, member.Host, member.Port, member.Role, member.State, pid)
	}

	backends := append(leaders, replicas...)
	if len(backends) == 0 {
		return nil, fmt.Errorf("failed to connect to any Patroni cluster members for %s", serverName)
	}

	cm.mutex.Lock()
	cm.backends[serverName] = backends
	cm.mutex.Unlock()

	return backends[0].DB, nil
}

// getLeaderConnectionDetails returns the host and port of the leader node from Patroni cluster
func (cm *ConnectionManager) getLeaderConnectionDetails(serverName string) (string, int, error) {
	config, exists := cm.configs[serverName]
	if !exists {
		return "", 0, fmt.Errorf("server %s not found in configuration", serverName)
	}

	cluster, err := cm.discoverPatroniCluster(config)
	if err != nil {
		return "", 0, fmt.Errorf("failed to discover Patroni cluster for %s: %w", serverName, err)
	}

	// Find the leader node
	for _, member := range cluster.Members {
		if member.Role == "leader" {
			return member.Host, member.Port, nil
		}
	}

	// If no leader found, use the first available node
	if len(cluster.Members) > 0 {
		return cluster.Members[0].Host, cluster.Members[0].Port, nil
	}

	return "", 0, fmt.Errorf("no cluster members found for %s", serverName)
}

// establishConnection creates new database connection(s) - always uses Patroni mode
func (cm *ConnectionManager) establishConnection(serverName string) (*sql.DB, error) {
	return cm.establishPatroniConnection(serverName)
}

// connectionString builds a URL-style DSN. URL encoding keeps passwords with
// spaces, quotes or slashes intact (key=value DSNs would silently truncate).
func connectionString(config PostgreSQLServer, host string, port int, dbName string) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(config.User, config.Password),
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   "/" + dbName,
	}
	if config.SSLMode != "" {
		q := u.Query()
		q.Set("sslmode", config.SSLMode)
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// createSingleConnection creates a single database connection to a specific host:port
func (cm *ConnectionManager) createSingleConnection(config PostgreSQLServer, host string, port int) (*sql.DB, error) {
	db, err := sql.Open("postgres", connectionString(config, host, port, config.DBName))
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to %s: %w", config.Name, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping %s: %w", config.Name, err)
	}

	// One persistent connection per backend; queries must not pile up on a
	// replica while the page polls every few seconds.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)

	return db, nil
}

// GetServerConfig returns the configuration for a specific server
func (cm *ConnectionManager) GetServerConfig(serverName string) (PostgreSQLServer, bool) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	config, exists := cm.configs[serverName]
	return config, exists
}

// Close closes all database connections
func (cm *ConnectionManager) Close() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	for name, backends := range cm.backends {
		for _, backend := range backends {
			if err := backend.DB.Close(); err != nil {
				log.Printf("Error closing backend connection to %s (PID %d): %v", name, backend.PID, err)
			}
		}
		log.Printf("Closed %d backend connections to PostgreSQL server: %s", len(backends), name)
	}
	cm.backends = make(map[string][]BackendConn)
}

type User struct {
	Username   string
	Superuser  bool
	CreateDB   bool
	CreateRole bool
	ValidUntil *time.Time
	Databases  []string
}

// HAProxyPort is a routing endpoint advertised by HAProxy.
type HAProxyPort struct {
	Port   int    `yaml:"port"`
	Type   string `yaml:"type"`
	Status string `yaml:"status"`
}

// ServiceNode is one Uptime Kuma badge endpoint for a service.
type ServiceNode struct {
	URL string `yaml:"url"`
}

type Service struct {
	Name  string        `yaml:"name"`
	Nodes []ServiceNode `yaml:"nodes"`
}

// NodeHeader labels a service-table column with its Patroni member name and host.
type NodeHeader struct {
	Name   string
	Detail string
}

// PageData is the template payload for the users page.
type PageData struct {
	Users     []User   `json:"users"`
	Databases []string `json:"databases"`
	Auth      UserView `json:"-"`
}

// CreateUserRequest is the JSON body of POST /api/v1/users.
type CreateUserRequest struct {
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	Databases []string `json:"databases"`
}

type Query struct {
	PID      int    `json:"pid"`
	Username string `json:"username"`
	Database string `json:"database"`
	Duration string `json:"duration"`
	Query    string `json:"query"`
}

// QueryWithServer is an active query annotated with the member it runs on.
type QueryWithServer struct {
	Query
	Server     string `json:"server"`
	BackendPID int    `json:"backend_pid"`
	MemberName string `json:"member_name"`
	MemberHost string `json:"member_host"`
	MemberPort int    `json:"member_port"`
	MemberRole string `json:"member_role"`
}

// TopologyMember describes a node in the Patroni cluster with replication detail.
type TopologyMember struct {
	Name            string `json:"name"`
	Host            string `json:"host"`
	Port            int    `json:"port"`
	Role            string `json:"role"`  // leader | replica | sync_standby | standby_leader
	State           string `json:"state"` // running | streaming | starting | stopped | etc.
	Timeline        int    `json:"timeline"`
	LagBytes        int64  `json:"lag_bytes"`        // 0 for leader
	LagSeconds      int    `json:"lag_seconds"`      // 0 for leader
	SyncState       string `json:"sync_state"`       // sync | async | quorum | "" (from pg_stat_replication)
	ApplicationName string `json:"application_name"` // from pg_stat_replication
	Reachable       bool   `json:"reachable"`
}

// TopologyServer describes one logical cluster (one `servers:` entry in config.yaml).
type TopologyServer struct {
	Name    string           `json:"name"`
	Leader  string           `json:"leader"`
	Members []TopologyMember `json:"members"`
	Error   string           `json:"error,omitempty"` // set if discovery failed
}

// TopologyResponse is the full payload served at /api/v1/topology.
type TopologyResponse struct {
	Servers     []TopologyServer `json:"servers"`
	GeneratedAt int64            `json:"generated_at"`
}

func getTemplatesDir(configuredPath string) string {
	if configuredPath != "" {
		if _, err := os.Stat(configuredPath); err == nil {
			return filepath.Join(configuredPath, "*")
		}
		log.Printf("Warning: Configured templates directory %s not found", configuredPath)
	}

	// Working directory (development) then next to the binary (packaged).
	if _, err := os.Stat("templates"); err == nil {
		return "templates/*"
	}
	if exePath, err := os.Executable(); err == nil {
		templatesDir := filepath.Join(filepath.Dir(exePath), "templates")
		if _, err := os.Stat(templatesDir); err == nil {
			return filepath.Join(templatesDir, "*")
		}
	}
	// No on-disk templates found; signal the caller to use the embedded copies.
	return ""
}

// ignoredUsers returns the configured ignore list as a non-nil slice so it can
// be bound as a text[] parameter (nil would bind as NULL and match no rows).
func ignoredUsers() []string {
	if globalConfig == nil || len(globalConfig.IgnoreUsers) == 0 {
		return []string{}
	}
	return globalConfig.IgnoreUsers
}

// loadConfig loads the main configuration file including servers and other settings
func loadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath) // #nosec G304 -- path comes from the -config flag
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// replaceBadgeWithDashboard rewrites an Uptime Kuma badge URL into the
// corresponding dashboard page URL.
//
//	input : https://host/api/badge/14/status?style=for-the-badge
//	output: https://host/dashboard/14
//
// Both the "/status" path segment and the query string must be dropped — the
// dashboard route doesn't accept them. This mirrors the JS helper in
// status.html so server-rendered links and client-rendered links agree.
func replaceBadgeWithDashboard(url string) string {
	url = strings.Replace(url, "/api/badge/", "/dashboard/", 1)
	// Strip query string first so we can cleanly match a trailing "/status".
	if idx := strings.Index(url, "?"); idx != -1 {
		url = url[:idx]
	}
	url = strings.TrimSuffix(url, "/status")
	return url
}

// pmmEmbedURL returns a Grafana URL rewritten for clean iframe embedding.
// It forces kiosk=1 so Grafana's breadcrumb, search bar and Edit/Share/Export
// buttons are hidden, while variable filters and the time picker remain visible.
// Note: older Grafana versions accepted "tv"/"full"/bare "kiosk", but Grafana
// v11.3+ (PMM 3.x) removed those values from the switch; only "1" (or "true")
// is universally supported, so we always normalise to "1" - overriding any
// legacy/invalid value that a caller may have left in the URL.
// Existing query params (e.g. refresh=2s) are preserved. An empty input returns
// an empty string so callers/templates can skip rendering.
func pmmEmbedURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Fallback: best-effort string concatenation if parsing fails.
		if strings.Contains(raw, "?") {
			return raw + "&kiosk=1"
		}
		return raw + "?kiosk=1"
	}
	q := u.Query()
	// Always force kiosk=1 - overrides any stale "tv" / "full" / empty value.
	q.Set("kiosk", "1")
	u.RawQuery = q.Encode()
	return u.String()
}

// pmmOpenURL returns the Grafana URL with any kiosk parameter stripped, so
// "Open in Grafana" launches the full interactive view in a new tab.
func pmmOpenURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Fallback: leave the URL untouched rather than mangling it.
		return raw
	}
	q := u.Query()
	q.Del("kiosk")
	u.RawQuery = q.Encode()
	return u.String()
}

// aggregateUsers queries all configured servers and returns users with their
// verified database access list plus the union of all databases (both in
// "db@server" compound format). Per-database table-privilege verification is
// done in parallel with a bounded worker pool (one connection per unique DB
// per server instead of one per (user, db) pair).
func aggregateUsers(ctx context.Context, cm *ConnectionManager, servers []PostgreSQLServer) ([]User, []string) {
	var allUsers []User
	databaseSet := make(map[string]bool)

	for _, server := range servers {
		db, err := cm.GetConnection(server.Name)
		if err != nil {
			log.Printf("Error connecting to server %s: %v", server.Name, err)
			continue
		}

		rows, err := db.QueryContext(ctx, `
			SELECT
				usename,
				usesuper,
				usecreatedb,
				r.rolcreaterole,
				valuntil
			FROM pg_user u
			JOIN pg_roles r ON u.usename = r.rolname
			ORDER BY usename
		`)
		if err != nil {
			log.Printf("Error querying users from server %s: %v", server.Name, err)
			continue
		}

		type pendingUser struct {
			user       User
			origName   string
			candidates []string // only populated for non-superusers
		}
		var pending []pendingUser

		for rows.Next() {
			var user User
			if err := rows.Scan(
				&user.Username,
				&user.Superuser,
				&user.CreateDB,
				&user.CreateRole,
				&user.ValidUntil,
			); err != nil {
				log.Printf("Error scanning user row from server %s: %v", server.Name, err)
				continue
			}

			originalUsername := user.Username
			user.Username = fmt.Sprintf("%s@%s", user.Username, server.Name)

			if user.Superuser || originalUsername == "postgres" {
				// Superusers / postgres see all non-template databases.
				dbRows, err := db.QueryContext(ctx, `
					SELECT datname
					FROM pg_database
					WHERE datistemplate = false
					ORDER BY datname
				`)
				if err == nil {
					for dbRows.Next() {
						var dbName string
						if err := dbRows.Scan(&dbName); err == nil {
							qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
							user.Databases = append(user.Databases, qualifiedDBName)
							databaseSet[qualifiedDBName] = true
						}
					}
					_ = dbRows.Close()
				}
				pending = append(pending, pendingUser{user: user, origName: originalUsername})
			} else {
				// Regular user: candidate DBs are those with CONNECT privilege.
				dbRows, err := db.QueryContext(ctx, `
					SELECT d.datname
					FROM pg_database d
					WHERE d.datistemplate = false
					AND has_database_privilege($1, d.datname, 'CONNECT')
					ORDER BY d.datname
				`, originalUsername)
				var candidates []string
				if err == nil {
					for dbRows.Next() {
						var dbName string
						if err := dbRows.Scan(&dbName); err == nil {
							candidates = append(candidates, dbName)
						}
					}
					_ = dbRows.Close()
				}
				pending = append(pending, pendingUser{
					user:       user,
					origName:   originalUsername,
					candidates: candidates,
				})
			}
		}
		_ = rows.Close()

		// Collect the unique DB names we need to probe (public-schema table grantees).
		uniqueDBs := make(map[string]struct{})
		for _, p := range pending {
			if p.user.Superuser || p.origName == "postgres" {
				continue
			}
			for _, dbName := range p.candidates {
				uniqueDBs[dbName] = struct{}{}
			}
		}

		// Resolve leader connection details ONCE per server (was: per user-db pair).
		var leaderHost string
		var leaderPort int
		if len(uniqueDBs) > 0 {
			var leaderErr error
			leaderHost, leaderPort, leaderErr = cm.getLeaderConnectionDetails(server.Name)
			if leaderErr != nil {
				log.Printf("Error getting leader connection details for %s: %v", server.Name, leaderErr)
			}
		}

		// Probe each unique DB in parallel with a bounded worker pool.
		granteesByDB := make(map[string]map[string]bool)
		var granteesMu sync.Mutex

		if leaderHost != "" {
			var wg sync.WaitGroup
			sem := make(chan struct{}, 8)

			for dbName := range uniqueDBs {
				wg.Add(1)
				sem <- struct{}{}
				go func(dbName string) {
					defer wg.Done()
					defer func() { <-sem }()

					probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
					defer cancel()

					dbConn, err := sql.Open("postgres", connectionString(server, leaderHost, leaderPort, dbName))
					if err != nil {
						log.Printf("Error opening connection to %s/%s: %v", server.Name, dbName, err)
						return
					}
					defer func() { _ = dbConn.Close() }()

					granteeRows, err := dbConn.QueryContext(probeCtx, `
						SELECT DISTINCT grantee
						FROM information_schema.table_privileges
						WHERE table_schema = 'public'
					`)
					if err != nil {
						log.Printf("Error querying grantees for %s/%s: %v", server.Name, dbName, err)
						return
					}
					defer func() { _ = granteeRows.Close() }()

					set := make(map[string]bool)
					for granteeRows.Next() {
						var grantee string
						if err := granteeRows.Scan(&grantee); err == nil {
							set[grantee] = true
						}
					}

					granteesMu.Lock()
					granteesByDB[dbName] = set
					granteesMu.Unlock()
				}(dbName)
			}

			wg.Wait()
		}

		// Assemble final user entries using the grantees map.
		for _, p := range pending {
			if p.user.Superuser || p.origName == "postgres" {
				allUsers = append(allUsers, p.user)
				continue
			}
			for _, dbName := range p.candidates {
				set, ok := granteesByDB[dbName]
				if !ok || !set[p.origName] {
					continue
				}
				qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
				p.user.Databases = append(p.user.Databases, qualifiedDBName)
				databaseSet[qualifiedDBName] = true
			}
			allUsers = append(allUsers, p.user)
		}

		// Populate the create-user form's database list with all non-template,
		// non-'postgres' databases for this server.
		dbRows, err := db.QueryContext(ctx, `
			SELECT datname
			FROM pg_database
			WHERE datistemplate = false
			AND datname != 'postgres'
			ORDER BY datname
		`)
		if err == nil {
			for dbRows.Next() {
				var dbName string
				if err := dbRows.Scan(&dbName); err == nil {
					databaseSet[fmt.Sprintf("%s@%s", dbName, server.Name)] = true
				}
			}
			_ = dbRows.Close()
		}
	}

	allDatabases := make([]string, 0, len(databaseSet))
	for dbName := range databaseSet {
		allDatabases = append(allDatabases, dbName)
	}
	sort.Strings(allDatabases)

	return allUsers, allDatabases
}

// splitCompound splits "name@server" on the last '@', so role names that
// contain '@' keep working while the server suffix stays unambiguous.
func splitCompound(value string) (name, server string, ok bool) {
	idx := strings.LastIndex(value, "@")
	if idx <= 0 || idx == len(value)-1 {
		return "", "", false
	}
	return value[:idx], value[idx+1:], true
}

// selectedDatabases groups "dbname@server" selections by server.
func selectedDatabases(keys []string) (map[string][]string, error) {
	selected := make(map[string][]string)
	for _, key := range keys {
		dbName, serverName, ok := splitCompound(key)
		if !ok {
			return nil, fmt.Errorf("invalid database reference %q", key)
		}
		if dbName == "postgres" {
			return nil, errors.New("cannot grant access to postgres database")
		}
		selected[serverName] = append(selected[serverName], dbName)
	}
	if len(selected) == 0 {
		return nil, errors.New("no valid databases selected")
	}
	for _, databases := range selected {
		sort.Strings(databases)
	}
	return selected, nil
}

// grantUserAccess grants CONNECT plus public-schema privileges on each database.
// Failures are collected: the role already exists, so reporting them beats
// pretending the grant applied.
func grantUserAccess(ctx context.Context, cm *ConnectionManager, db *sql.DB, cfg PostgreSQLServer, username string, databases []string) []string {
	host, port, err := cm.getLeaderConnectionDetails(cfg.Name)
	if err != nil {
		return []string{fmt.Sprintf("%s: leader discovery failed: %v", cfg.Name, err)}
	}

	ident := pq.QuoteIdentifier(username)
	var warnings []string

	for _, dbName := range databases {
		if _, err := db.ExecContext(ctx, "GRANT CONNECT ON DATABASE "+pq.QuoteIdentifier(dbName)+" TO "+ident); err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: GRANT CONNECT failed: %v", dbName, err))
			continue
		}

		dbConn, err := sql.Open("postgres", connectionString(cfg, host, port, dbName))
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: connect failed: %v", dbName, err))
			continue
		}

		schemaQueries := []string{
			"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO " + ident,
			"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO " + ident,
			"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO " + ident,
			"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO " + ident,
		}
		for _, query := range schemaQueries {
			if _, err := dbConn.ExecContext(ctx, query); err != nil {
				warnings = append(warnings, fmt.Sprintf("%s: %v", dbName, err))
			}
		}
		_ = dbConn.Close()
	}

	return warnings
}

// revokeUserAccess drops the role's grants and owned objects in every database.
// The role is dropped by the caller afterwards.
func revokeUserAccess(ctx context.Context, cm *ConnectionManager, db *sql.DB, cfg PostgreSQLServer, username string) []string {
	host, port, err := cm.getLeaderConnectionDetails(cfg.Name)
	if err != nil {
		return []string{fmt.Sprintf("%s: leader discovery failed: %v", cfg.Name, err)}
	}

	rows, err := db.QueryContext(ctx, "SELECT datname FROM pg_database WHERE datistemplate = false")
	if err != nil {
		return []string{fmt.Sprintf("list databases failed: %v", err)}
	}
	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err == nil {
			databases = append(databases, dbName)
		}
	}
	_ = rows.Close()

	ident := pq.QuoteIdentifier(username)
	owner := pq.QuoteIdentifier(cfg.User)
	var warnings []string

	for _, dbName := range databases {
		dbConn, err := sql.Open("postgres", connectionString(cfg, host, port, dbName))
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: connect failed: %v", dbName, err))
			continue
		}

		statements := []string{
			"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM " + ident,
			"REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM " + ident,
			"REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM " + ident,
			"REVOKE ALL PRIVILEGES ON DATABASE " + pq.QuoteIdentifier(dbName) + " FROM " + ident,
			"REASSIGN OWNED BY " + ident + " TO " + owner,
			"DROP OWNED BY " + ident,
		}
		for _, query := range statements {
			if _, err := dbConn.ExecContext(ctx, query); err != nil {
				warnings = append(warnings, fmt.Sprintf("%s: %v", dbName, err))
			}
		}
		_ = dbConn.Close()
	}

	return warnings
}

// collectActiveQueries gathers pgbouncer-agnostic activity from every member,
// collapsing parallel workers onto their leader PID.
func collectActiveQueries(ctx context.Context, cm *ConnectionManager, servers []PostgreSQLServer) []QueryWithServer {
	const querySQL = `
		SELECT DISTINCT ON (COALESCE(leader_pid, pid))
			COALESCE(leader_pid, pid) AS pid,
			usename,
			COALESCE(datname, 'system') AS datname,
			ROUND(EXTRACT(EPOCH FROM now() - query_start))::text || 's' AS duration,
			query
		FROM pg_stat_activity
		WHERE state = 'active'
			AND query NOT ILIKE '%pg_stat_activity%'
			AND pid != pg_backend_pid()
			AND usename::text <> ALL($1)
		ORDER BY COALESCE(leader_pid, pid), query_start ASC`

	allQueries := make([]QueryWithServer, 0)
	for _, server := range servers {
		backends := cm.GetBackendConnections(server.Name)
		if len(backends) == 0 {
			log.Printf("No backend connections available for server %s", server.Name)
			continue
		}

		for _, backend := range backends {
			rows, err := backend.DB.QueryContext(ctx, querySQL, pq.Array(ignoredUsers()))
			if err != nil {
				log.Printf("Error querying active queries from server %s backend PID %d: %v", server.Name, backend.PID, err)
				continue
			}

			for rows.Next() {
				var q Query
				if err := rows.Scan(&q.PID, &q.Username, &q.Database, &q.Duration, &q.Query); err != nil {
					log.Printf("Error scanning query row from server %s backend PID %d: %v", server.Name, backend.PID, err)
					continue
				}
				allQueries = append(allQueries, QueryWithServer{
					Query:      q,
					Server:     server.Name,
					BackendPID: backend.PID,
					MemberName: backend.MemberName,
					MemberHost: backend.Host,
					MemberPort: backend.Port,
					MemberRole: backend.Role,
				})
			}
			_ = rows.Close()
		}
	}

	return allQueries
}

func securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "SAMEORIGIN")
		c.Header("Referrer-Policy", "same-origin")
		c.Next()
	}
}

// sameOriginOnly rejects cross-site state changes. Browsers attach Origin to
// CORS/fetch/form POSTs; non-browser API clients normally send none. Basic auth
// is otherwise CSRF-able because browsers replay credentials automatically.
func sameOriginOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		switch c.Request.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			c.Next()
			return
		}
		if origin := c.Request.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host != c.Request.Host {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "cross-origin request rejected"})
				return
			}
		}
		c.Next()
	}
}

// templateFuncs holds helpers shared by the server and the template parse test.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"replaceBadgeWithDashboard": replaceBadgeWithDashboard,
		"pmmEmbedURL":               pmmEmbedURL,
		"pmmOpenURL":                pmmOpenURL,
		"roleLabel":                 RoleLabel,
		"dict": func(values ...any) map[string]any {
			m := make(map[string]any, len(values)/2)
			for i := 0; i+1 < len(values); i += 2 {
				key, _ := values[i].(string)
				m[key] = values[i+1]
			}
			return m
		},
		"until": func(n int) []int {
			numbers := make([]int, n)
			for i := range numbers {
				numbers[i] = i + 1
			}
			return numbers
		},
		"formatDate": func(t *time.Time) string {
			if t == nil {
				return ""
			}
			return t.Format("02.01.2006")
		},
		// stripServer drops a trailing "@server" suffix from compound keys
		// (e.g. "alice@calik-yepas-patroni" -> "alice") for display only.
		// The original value must still be used for CRUD calls that depend
		// on the username@server / database@server compound format.
		"stripServer": func(s string) string {
			if idx := strings.LastIndex(s, "@"); idx != -1 {
				return s[:idx]
			}
			return s
		},
	}
}

func InitServer(assets fs.FS, version string) {
	var templatesDir string
	var configPath string
	var serverPort string
	var addUser string
	var addRole string

	flag.StringVar(&configPath, "config", "", "Path to configuration file (required)")
	flag.StringVar(&templatesDir, "templates", "", "Path to templates directory")
	flag.StringVar(&serverPort, "server-port", "8080", "Server port to listen on")
	flag.StringVar(&addUser, "add-user", "", "Create or update a panel user, then exit (password read from stdin)")
	flag.StringVar(&addRole, "role", RoleAdmin, "Role for -add-user: viewer, operator or admin")
	flag.Parse()

	log.Println("Initializing server...")

	if configPath == "" {
		log.Fatal("Configuration file is required - single server mode is not supported. Please use -config flag with a Patroni cluster configuration.")
	}

	log.Printf("Loading configuration from: %s", configPath)
	loadedConfig, err := loadConfig(configPath)
	if err != nil {
		log.Fatal("Failed to load configuration:", err)
	}

	config := *loadedConfig
	globalConfig = &config
	if len(config.Servers) == 0 {
		log.Fatal("No servers configured in config file")
	}

	if config.LogFile != "" {
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", config.LogFile, err)
		}
		defer logFile.Close()
		log.SetOutput(io.MultiWriter(os.Stderr, logFile))
	}

	servers := config.Servers
	log.Printf("Configured %d PostgreSQL servers", len(servers))
	if len(config.IgnoreUsers) > 0 {
		log.Printf("Configured to ignore %d users in active queries: %v", len(config.IgnoreUsers), config.IgnoreUsers)
	}

	connectionManager := NewConnectionManager(servers)
	defer connectionManager.Close()

	for _, server := range servers {
		if _, err := connectionManager.GetConnection(server.Name); err != nil {
			log.Printf("Warning: Failed to connect to server %s: %v", server.Name, err)
		}
	}

	legacyPassword := config.Auth.Password
	authOptions := AuthOptions{
		LegacyUsername: config.Auth.Username,
		Disabled:       config.Auth.Disabled,
		CookieSecure:   config.Auth.secureCookies(),
		SessionTTL:     config.Auth.sessionTTL(),
	}
	authOptions.LegacyConfigValue = legacyPassword

	var panelStore UserStore
	if !config.Auth.Disabled {
		authServer := config.Auth.Server
		if authServer == "" {
			authServer = servers[0].Name
		}
		if _, ok := connectionManager.GetServerConfig(authServer); !ok {
			log.Fatalf("auth.server %q is not a configured server", authServer)
		}

		store := NewPGUserStore(func() (*sql.DB, error) {
			return connectionManager.GetConnection(authServer)
		})
		initCtx, initCancel := context.WithTimeout(context.Background(), 20*time.Second)
		initErr := store.Init(initCtx)
		if initErr == nil {
			seedUser, seedPass := config.Auth.Username, config.Auth.Password
			if envPass := os.Getenv("MONODB_ADMIN_PASSWORD"); envPass != "" && seedPass == "" {
				seedUser = "admin"
				seedPass = envPass
			}
			if err := SeedPanelAdmin(initCtx, store, seedUser, seedPass); err != nil {
				log.Printf("Warning: failed to seed panel admin: %v", err)
			}
			panelStore = store
		}
		initCancel()

		if initErr != nil {
			if authOptions.LegacyUsername != "" && authOptions.LegacyConfigValue != "" {
				log.Printf("WARNING: panel user store unavailable (%v); continuing with legacy Basic Auth only", initErr)
			} else {
				log.Fatalf("Panel auth database unavailable and no legacy auth configured: %v", initErr)
			}
		}
	}

	if addUser != "" {
		if panelStore == nil {
			log.Fatal("Cannot add panel user: auth store unavailable")
		}
		if !validUsername(addUser) {
			log.Fatalf("Invalid panel username %q", addUser)
		}
		if !ValidRole(addRole) {
			log.Fatalf("Invalid role %q (viewer|operator|admin)", addRole)
		}
		password, err := readPasswordLine()
		if err != nil {
			log.Fatal("Failed to read password from stdin:", err)
		}
		cliCtx, cliCancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := UpsertPanelUser(cliCtx, panelStore, addUser, addRole, password); err != nil {
			cliCancel()
			log.Fatal("Failed to write panel user:", err)
		}
		cliCancel()
		log.Printf("panel user %q is ready with role %s", addUser, addRole)
		return
	}

	authenticator := NewAuthenticator(authOptions, panelStore)

	if os.Getenv("GIN_MODE") == "" {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.Default()
	if err := router.SetTrustedProxies(nil); err != nil {
		log.Fatal("Failed to configure trusted proxies:", err)
	}

	router.SetFuncMap(templateFuncs())

	// Prefer on-disk templates (override), fall back to the embedded copies so
	// releases are self-contained.
	if diskGlob := getTemplatesDir(templatesDir); diskGlob != "" {
		log.Printf("Loading templates from disk: %s", diskGlob)
		router.LoadHTMLGlob(diskGlob)
	} else {
		log.Printf("Loading embedded templates")
		tmpl := template.Must(template.New("").Funcs(router.FuncMap).ParseFS(assets, "templates/*.html"))
		router.SetHTMLTemplate(tmpl)
	}

	staticFS, err := fs.Sub(assets, "static")
	if err != nil {
		log.Fatal("Failed to load embedded static assets:", err)
	}
	router.StaticFS("/static", http.FS(staticFS))

	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	router.Use(securityHeaders())
	router.GET("/login", authenticator.LoginPage())
	router.POST("/login", sameOriginOnly(), authenticator.Login())

	app := router.Group("")
	app.Use(authenticator.Middleware(), sameOriginOnly())
	app.POST("/logout", authenticator.Logout())
	app.GET("/settings/users", authenticator.Require(PermManagePanelUsers), authenticator.UsersPage())
	app.GET("/settings/password", authenticator.PasswordPage())

	haPorts := config.Ports
	services := config.Services
	pmmURL := config.PMMStatusURL
	queryIframeURL := config.PMMQanURL
	badgeRefreshInterval := config.BadgeRefreshInterval
	if badgeRefreshInterval == 0 {
		badgeRefreshInterval = 3000
	}
	log.Printf("Loaded %d HAProxy ports, %d services", len(haPorts), len(services))

	// Number of service columns the status table needs: the widest service
	// node list, not a hardcoded cluster size.
	nodeCount := 0
	for _, service := range services {
		if len(service.Nodes) > nodeCount {
			nodeCount = len(service.Nodes)
		}
	}

	app.GET("/", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
		defer cancel()

		var serverStatuses []map[string]interface{}
		var clusterNodes []NodeHeader

		for _, server := range servers {
			status := map[string]interface{}{
				"name":      server.Name,
				"host":      "patroni-cluster",
				"port":      "dynamic",
				"status":    "disconnected",
				"users":     0,
				"databases": 0,
			}

			cluster, err := connectionManager.discoverPatroniCluster(server)
			if err != nil {
				log.Printf("Error discovering Patroni cluster %s: %v", server.Name, err)
				status["error"] = err.Error()
			} else {
				status["use_patroni"] = true
				status["patroni_cluster"] = cluster
				status["status"] = "patroni"
				if len(clusterNodes) == 0 {
					for _, member := range cluster.Members {
						clusterNodes = append(clusterNodes, NodeHeader{Name: member.Name, Detail: member.Host})
					}
				}
			}

			db, err := connectionManager.GetConnection(server.Name)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", server.Name, err)
			} else {
				status["status"] = "connected"

				var userCount int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_user").Scan(&userCount); err == nil {
					status["users"] = userCount
				}

				var dbCount int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_database WHERE datistemplate = false").Scan(&dbCount); err == nil {
					status["databases"] = dbCount
				}
			}

			serverStatuses = append(serverStatuses, status)
		}

		nodeHeaders := make([]NodeHeader, 0, nodeCount)
		for i := 0; i < nodeCount; i++ {
			if i < len(clusterNodes) {
				nodeHeaders = append(nodeHeaders, clusterNodes[i])
			} else {
				nodeHeaders = append(nodeHeaders, NodeHeader{Name: fmt.Sprintf("Node %d", i+1)})
			}
		}

		c.HTML(200, "status.html", gin.H{
			"Auth":                 authenticator.View(c),
			"Servers":              servers,
			"ServerStatuses":       serverStatuses,
			"HAPorts":              haPorts,
			"Services":             services,
			"NodeCount":            nodeCount,
			"NodeHeaders":          nodeHeaders,
			"PMMStatusURL":         pmmURL,
			"PMMQanURL":            queryIframeURL,
			"BadgeRefreshInterval": badgeRefreshInterval,
			"Version":              version,
		})
	})

	app.GET("/users", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
		defer cancel()

		allUsers, allDatabases := aggregateUsers(ctx, connectionManager, servers)
		c.HTML(200, "users.html", PageData{Users: allUsers, Databases: allDatabases, Auth: authenticator.View(c)})
	})

	// Group API routes under /api/v1
	v1 := app.Group("/api/v1")
	v1.Use(authenticator.Require(PermView))
	{
		v1.GET("/auth/me", authenticator.Me())
		v1.POST("/auth/password", authenticator.ChangePassword())
		v1.GET("/auth/users", authenticator.Require(PermManagePanelUsers), authenticator.ListUsers())
		v1.POST("/auth/users", authenticator.Require(PermManagePanelUsers), authenticator.CreateUser())
		v1.PUT("/auth/users/:username", authenticator.Require(PermManagePanelUsers), authenticator.UpdateUser())
		v1.DELETE("/auth/users/:username", authenticator.Require(PermManagePanelUsers), authenticator.DeleteUser())

		v1.GET("/servers", func(c *gin.Context) {
			c.JSON(200, gin.H{"servers": servers})
		})

		v1.GET("/status", func(c *gin.Context) {
			c.Header("Cache-Control", "no-cache, must-revalidate")

			start := time.Now()
			defer func() {
				log.Printf("Status endpoint served in %v: %d ports, %d services",
					time.Since(start), len(haPorts), len(services))
			}()

			c.JSON(200, gin.H{
				"HAPorts":              haPorts,
				"Services":             services,
				"PMMStatusURL":         pmmURL,
				"PMMQanURL":            queryIframeURL,
				"BadgeRefreshInterval": badgeRefreshInterval,
				"ServerTime":           time.Now().Unix(),
				"Version":              version,
			})
		})

		v1.GET("/users", func(c *gin.Context) {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
			defer cancel()

			allUsers, allDatabases := aggregateUsers(ctx, connectionManager, servers)
			c.JSON(200, PageData{Users: allUsers, Databases: allDatabases})
		})

		v1.POST("/users", authenticator.Require(PermManageDBUsers), func(c *gin.Context) {
			var req CreateUserRequest
			if err := c.ShouldBindJSON(&req); err != nil {
				log.Printf("Bind error: %v", err)
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
				return
			}
			req.Username = strings.TrimSpace(req.Username)
			if req.Username == "" || req.Password == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Username and password are required"})
				return
			}

			serverDatabases, err := selectedDatabases(req.Databases)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
			defer cancel()

			quotedUser := pq.QuoteIdentifier(req.Username)
			var warnings []string

			serverNames := make([]string, 0, len(serverDatabases))
			for serverName := range serverDatabases {
				serverNames = append(serverNames, serverName)
			}
			sort.Strings(serverNames)

			for _, serverName := range serverNames {
				db, err := connectionManager.GetConnection(serverName)
				if err != nil {
					log.Printf("Error connecting to server %s: %v", serverName, err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to connect to server %s", serverName)})
					return
				}

				var exists bool
				if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", req.Username).Scan(&exists); err != nil {
					log.Printf("Error checking user existence on %s: %v", serverName, err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to check user on server %s", serverName)})
					return
				}
				if exists {
					c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("User already exists on server %s", serverName)})
					return
				}

				// Identifiers and literals cannot be bound as parameters in
				// CREATE ROLE, so quote both explicitly to keep them inert.
				createQuery := "CREATE USER " + quotedUser + " WITH PASSWORD " + pq.QuoteLiteral(req.Password) // #nosec G202 -- both fragments are pg-quoted
				if _, err := db.ExecContext(ctx, createQuery); err != nil {
					log.Printf("Error creating user on %s: %v", serverName, err)
					errorMsg := fmt.Sprintf("Failed to create user on server %s", serverName)
					switch {
					case strings.Contains(err.Error(), "permission denied"):
						errorMsg = fmt.Sprintf("Permission denied on server %s - insufficient privileges", serverName)
					case strings.Contains(err.Error(), "password authentication failed"):
						errorMsg = "Invalid password format"
					case strings.Contains(err.Error(), "role name contains invalid characters"):
						errorMsg = "Username contains invalid characters"
					}
					c.JSON(http.StatusBadRequest, gin.H{"error": errorMsg})
					return
				}

				cfg, _ := connectionManager.GetServerConfig(serverName)
				warnings = append(warnings, grantUserAccess(ctx, connectionManager, db, cfg, req.Username, serverDatabases[serverName])...)
			}

			audit(c, "db_user_create", req.Username, "ok")
			response := gin.H{"message": "User created successfully on all selected servers"}
			if len(warnings) > 0 {
				response["warnings"] = warnings
			}
			c.JSON(http.StatusOK, response)
		})

		v1.DELETE("/users/:username", authenticator.Require(PermManageDBUsers), func(c *gin.Context) {
			usernameWithServer := c.Param("username")
			username, serverName, ok := splitCompound(usernameWithServer)
			if !ok {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid username format. Expected username@server"})
				return
			}

			db, err := connectionManager.GetConnection(serverName)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", serverName, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to connect to server %s", serverName)})
				return
			}

			serverConfig, exists := connectionManager.GetServerConfig(serverName)
			if !exists {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Unknown server %s", serverName)})
				return
			}
			if username == serverConfig.User {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Cannot delete the current user"})
				return
			}

			ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
			defer cancel()

			warnings := revokeUserAccess(ctx, connectionManager, db, serverConfig, username)
			// Drop the role only after every database has been cleaned up.
			if _, err := db.ExecContext(ctx, "DROP ROLE IF EXISTS "+pq.QuoteIdentifier(username)); err != nil {
				log.Printf("Error deleting user %s on %s: %v", username, serverName, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to delete user on server %s", serverName)})
				return
			}

			audit(c, "db_user_delete", username, "ok")
			response := gin.H{"message": fmt.Sprintf("User deleted successfully from server %s", serverName)}
			if len(warnings) > 0 {
				response["warnings"] = warnings
			}
			c.JSON(http.StatusOK, response)
		})

		// Active queries across every cluster member.
		v1.GET("/queries", func(c *gin.Context) {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
			defer cancel()

			c.JSON(200, gin.H{"queries": collectActiveQueries(ctx, connectionManager, servers)})
		})

		// Terminate a backend on a specific server.
		v1.DELETE("/queries/:pid", authenticator.Require(PermKillQuery), func(c *gin.Context) {
			pid, err := strconv.Atoi(c.Param("pid"))
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid PID"})
				return
			}
			serverName := c.Query("server")
			if serverName == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Server parameter is required"})
				return
			}

			backends := connectionManager.GetBackendConnections(serverName)
			if len(backends) == 0 {
				log.Printf("No backend connections available for server %s", serverName)
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("No connections available for server %s", serverName)})
				return
			}

			ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
			defer cancel()

			var queryFound bool
			for _, backend := range backends {
				var exists bool
				if err := backend.DB.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&exists); err != nil {
					log.Printf("Error checking query existence on %s backend PID %d: %v", serverName, backend.PID, err)
					continue
				}
				if !exists {
					continue
				}

				queryFound = true
				var ok bool
				if err := backend.DB.QueryRowContext(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&ok); err != nil {
					log.Printf("Terminate backend %d failed on %s backend PID %d: %v", pid, serverName, backend.PID, err)
					continue
				}
				if ok {
					log.Printf("Successfully killed query %d on server %s backend PID %d", pid, serverName, backend.PID)
					audit(c, "query_kill", fmt.Sprintf("%d@%s", pid, serverName), "ok")
					c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Query killed successfully on server %s", serverName)})
					return
				}
				log.Printf("Terminate backend %d returned false on %s backend PID %d", pid, serverName, backend.PID)
			}

			if !queryFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "Query not found on any backend"})
				return
			}
			c.JSON(http.StatusConflict, gin.H{"error": "Query could not be terminated"})
		})

		// Cluster topology endpoint - returns Patroni cluster members enriched
		// with per-replica lag detail pulled from pg_stat_replication on the leader.
		v1.GET("/topology", func(c *gin.Context) {
			c.Header("Cache-Control", "no-cache")

			ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
			defer cancel()

			resp := TopologyResponse{
				Servers:     make([]TopologyServer, 0, len(servers)),
				GeneratedAt: time.Now().Unix(),
			}

			for _, server := range servers {
				ts := TopologyServer{Name: server.Name}

				cluster, err := connectionManager.discoverPatroniCluster(server)
				if err != nil {
					log.Printf("Topology: Patroni discovery failed for %s: %v", server.Name, err)
					ts.Error = err.Error()
					resp.Servers = append(resp.Servers, ts)
					continue
				}

				ts.Leader = cluster.Leader
				ts.Members = make([]TopologyMember, 0, len(cluster.Members))

				// Copy Patroni member data. Patroni wouldn't have reported them if
				// they weren't reachable, so default Reachable=true.
				for _, m := range cluster.Members {
					ts.Members = append(ts.Members, TopologyMember{
						Name:      m.Name,
						Host:      m.Host,
						Port:      m.Port,
						Role:      m.Role,
						State:     m.State,
						Timeline:  m.Timeline,
						LagBytes:  int64(m.Lag),
						Reachable: true,
					})
				}

				// Best-effort enrichment from pg_stat_replication on the leader.
				// Failures here are non-fatal: we still return Patroni-only data.
				db, dbErr := connectionManager.GetConnection(server.Name)
				if dbErr != nil {
					log.Printf("Topology: failed to get leader connection for %s: %v", server.Name, dbErr)
					resp.Servers = append(resp.Servers, ts)
					continue
				}

				rows, qErr := db.QueryContext(ctx, `
					SELECT
					  COALESCE(application_name, ''),
					  COALESCE(state, ''),
					  COALESCE(sync_state, ''),
					  COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), 0)::bigint AS lag_bytes,
					  COALESCE(EXTRACT(epoch FROM (now() - reply_time))::int, 0) AS lag_seconds
					FROM pg_stat_replication
				`)
				if qErr != nil {
					log.Printf("Topology: pg_stat_replication query failed for %s: %v", server.Name, qErr)
					resp.Servers = append(resp.Servers, ts)
					continue
				}

				type replRow struct {
					appName    string
					state      string
					syncState  string
					lagBytes   int64
					lagSeconds int
				}
				var replRows []replRow
				for rows.Next() {
					var r replRow
					if scanErr := rows.Scan(&r.appName, &r.state, &r.syncState, &r.lagBytes, &r.lagSeconds); scanErr != nil {
						log.Printf("Topology: scan error for %s: %v", server.Name, scanErr)
						continue
					}
					replRows = append(replRows, r)
				}
				_ = rows.Close()

				// Match rows to members by application_name == member.Name.
				for i := range ts.Members {
					if ts.Members[i].Role == "leader" {
						continue
					}
					for _, r := range replRows {
						if r.appName == ts.Members[i].Name {
							ts.Members[i].ApplicationName = r.appName
							ts.Members[i].SyncState = r.syncState
							ts.Members[i].LagBytes = r.lagBytes
							ts.Members[i].LagSeconds = r.lagSeconds
							// Prefer the precise streaming state from pg_stat_replication
							// over Patroni's coarser state label if present.
							if r.state != "" {
								ts.Members[i].State = r.state
							}
							break
						}
					}
				}

				resp.Servers = append(resp.Servers, ts)
			}

			c.JSON(200, resp)
		})
	}

	app.GET("/query", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()

		c.HTML(200, "query.html", gin.H{
			"Auth":    authenticator.View(c),
			"Queries": collectActiveQueries(ctx, connectionManager, servers),
		})
	})

	app.GET("/query-analytics", func(c *gin.Context) {
		c.HTML(200, "query_analytics.html", gin.H{
			"Auth":      authenticator.View(c),
			"PMMQanURL": queryIframeURL,
		})
	})

	// Cluster topology page. Actual data is fetched client-side from
	// /api/v1/topology and refreshed every 5 seconds.
	app.GET("/topology", func(c *gin.Context) {
		c.HTML(200, "topology.html", gin.H{
			"Auth":    authenticator.View(c),
			"HAPorts": haPorts,
		})
	})

	// Start the server with timeouts: a management panel must not be held open
	// by a slow client.
	log.Printf("Starting server on :%s", serverPort)
	server := &http.Server{
		Addr:              ":" + serverPort,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("Failed to start server:", err)
	}
}

func readPasswordLine() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	password := strings.TrimSpace(line)
	if password == "" {
		return "", errors.New("empty password")
	}
	return password, nil
}

package api

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"text/template"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

// Global config variable to access ignore users setting
var globalConfig *Config

// PostgreSQLServer represents a Patroni-managed PostgreSQL cluster configuration
type PostgreSQLServer struct {
	Name     string `yaml:"name"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`

	// Patroni configuration
	PatroniNodes    []string `yaml:"patroni_nodes"`               // List of Patroni REST API endpoints
	PatroniPort     int      `yaml:"patroni_port,omitempty"`      // Patroni REST API port (default 8008)
	PreferLeader    bool     `yaml:"prefer_leader,omitempty"`     // Connect to leader by default
	ConnectToLeader bool     `yaml:"connect_to_leader,omitempty"` // Only connect to leader
}

// Config holds database connection parameters and application settings
type Config struct {
	// Multi-server configuration
	Servers []PostgreSQLServer `yaml:"servers"`

	// Legacy single-server fields (for backward compatibility)
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string

	Databases []string `yaml:"databases"`
	LogFile   string   `yaml:"log_file"`

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

// ConnectionManager manages database connections to multiple PostgreSQL servers
type ConnectionManager struct {
	backends map[string][]BackendConn
	configs  map[string]PostgreSQLServer
	mutex    sync.RWMutex
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

// GetConnection returns a database connection for the specified server (backward compatibility)
func (cm *ConnectionManager) GetConnection(serverName string) (*sql.DB, error) {
	backends := cm.GetBackendConnections(serverName)
	if len(backends) == 0 {
		return cm.establishConnection(serverName)
	}
	return backends[0].DB, nil
}

// GetBackendConnections returns all backend connections for the specified server
func (cm *ConnectionManager) GetBackendConnections(serverName string) []BackendConn {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	backends, exists := cm.backends[serverName]
	if !exists {
		return nil
	}

	// Test all connections and remove stale ones
	validBackends := make([]BackendConn, 0, len(backends))
	for _, backend := range backends {
		if err := backend.DB.Ping(); err == nil {
			validBackends = append(validBackends, backend)
		} else {
			log.Printf("Backend connection to %s (PID %d) is stale, removing: %v", serverName, backend.PID, err)
			backend.DB.Close()
		}
	}

	// Update the stored backends if any were removed
	if len(validBackends) != len(backends) {
		cm.mutex.RUnlock()
		cm.mutex.Lock()
		cm.backends[serverName] = validBackends
		cm.mutex.Unlock()
		cm.mutex.RLock()
	}

	return validBackends
}

// removeConnection safely removes a connection from the manager
func (cm *ConnectionManager) removeConnection(serverName string) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if backends, exists := cm.backends[serverName]; exists {
		for _, backend := range backends {
			backend.DB.Close()
		}
		delete(cm.backends, serverName)
		log.Printf("Removed stale connections for server: %s", serverName)
	}
}

// discoverPatroniCluster queries Patroni REST API to discover cluster members
func (cm *ConnectionManager) discoverPatroniCluster(config PostgreSQLServer) (*PatroniCluster, error) {
	patroniPort := config.PatroniPort
	if patroniPort == 0 {
		patroniPort = 8008 // Default Patroni REST API port
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Try each Patroni node to get cluster information
	for _, node := range config.PatroniNodes {
		url := fmt.Sprintf("http://%s:%d/cluster", node, patroniPort)
		resp, err := client.Get(url)
		if err != nil {
			log.Printf("Failed to connect to Patroni node %s: %v", node, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("Patroni node %s returned status %d", node, resp.StatusCode)
			continue
		}

		var cluster PatroniCluster
		if err := json.NewDecoder(resp.Body).Decode(&cluster); err != nil {
			log.Printf("Failed to decode Patroni response from %s: %v", node, err)
			continue
		}

		log.Printf("Successfully discovered Patroni cluster from node %s: %d members, leader: %s",
			node, len(cluster.Members), cluster.Leader)
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

	cluster, err := cm.discoverPatroniCluster(config)
	if err != nil {
		return nil, fmt.Errorf("failed to discover Patroni cluster for %s: %v", serverName, err)
	}

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Clear existing connections for this server
	if backends, exists := cm.backends[serverName]; exists {
		for _, backend := range backends {
			backend.DB.Close()
		}
		delete(cm.backends, serverName)
	}

	var backends []BackendConn
	var leaderConn *BackendConn

	// Connect to each cluster member
	for _, member := range cluster.Members {
		// Skip if we only want leader connections and this isn't the leader
		if config.ConnectToLeader && member.Role != "leader" {
			continue
		}

		// Connect to this specific member
		db, err := cm.createSingleConnection(config, member.Host, member.Port)
		if err != nil {
			log.Printf("Failed to connect to Patroni member %s (%s:%d): %v",
				member.Name, member.Host, member.Port, err)
			continue
		}

		// Get PID for this connection
		var pid int
		if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&pid); err != nil {
			log.Printf("Failed to get backend PID for Patroni member %s: %v", member.Name, err)
			db.Close()
			continue
		}

		backend := BackendConn{DB: db, PID: pid, MemberName: member.Name, Host: member.Host, Port: member.Port, Role: member.Role}
		backends = append(backends, backend)

		if member.Role == "leader" {
			leaderConn = &backend
		}

		log.Printf("Connected to Patroni member %s (%s:%d) - Role: %s, State: %s, PID: %d",
			member.Name, member.Host, member.Port, member.Role, member.State, pid)
	}

	if len(backends) == 0 {
		return nil, fmt.Errorf("failed to connect to any Patroni cluster members for %s", serverName)
	}

	cm.backends[serverName] = backends

	// Return leader connection if preferred, otherwise return first available
	if config.PreferLeader && leaderConn != nil {
		return leaderConn.DB, nil
	}
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
		return "", 0, fmt.Errorf("failed to discover Patroni cluster for %s: %v", serverName, err)
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

// createSingleConnection creates a single database connection to a specific host:port
func (cm *ConnectionManager) createSingleConnection(config PostgreSQLServer, host string, port int) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		host,
		port,
		config.User,
		config.Password,
		config.DBName,
		config.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to %s: %v", config.Name, err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping %s: %v", config.Name, err)
	}

	// Configure connection pooling for persistent connections
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0) // Keep connections open indefinitely

	return db, nil
}

// GetServerNames returns all configured server names
func (cm *ConnectionManager) GetServerNames() []string {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	names := make([]string, 0, len(cm.configs))
	for name := range cm.configs {
		names = append(names, name)
	}
	return names
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
	Username   string     `db:"usename"`
	Superuser  bool       `db:"usesuper"`
	CreateDB   bool       `db:"usecreatedb"`
	CreateRole bool       `db:"rolcreaterole"`
	ValidUntil *time.Time `db:"valuntil"`
	Databases  []string   // Add this field
}

// Add near the top with other structs
type HAProxyPort struct {
	Port   int    `yaml:"port"`
	Type   string `yaml:"type"`
	Status string `yaml:"status"`
}

// Add these new structs
type ServiceNode struct {
	URL string `yaml:"url"`
}

type Service struct {
	Name  string        `yaml:"name"`
	Nodes []ServiceNode `yaml:"nodes"`
}

type PageData struct {
	Servers              []PostgreSQLServer
	SelectedServer       string
	Users                []User
	Databases            []string
	HAPorts              []HAProxyPort
	Services             []Service
	PMMStatusURL         string
	PMMQanURL            string
	BadgeRefreshInterval int
}

// Add this after the User struct
type CreateUserRequest struct {
	Username  string   `form:"username"`
	Password  string   `form:"password"`
	Databases []string `form:"databases"`
}

// Add this struct after the existing ones
type Query struct {
	PID      int    `json:"pid"`
	Username string `json:"username"`
	Database string `json:"database"`
	Duration string `json:"duration"`
	Query    string `json:"query"`
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
	// If templates dir is configured, use that
	if configuredPath != "" {
		// Check if the directory exists
		if _, err := os.Stat(configuredPath); err == nil {
			return filepath.Join(configuredPath, "*")
		}
		log.Printf("Warning: Configured templates directory %s not found", configuredPath)
	}

	// Try relative to current working directory
	if _, err := os.Stat("templates"); err == nil {
		return "templates/*"
	}

	// Try relative to binary location
	exePath, err := os.Executable()
	if err == nil {
		binDir := filepath.Dir(exePath)
		templatesPath := filepath.Join(binDir, "templates", "*")
		if _, err := os.Stat(filepath.Dir(templatesPath)); err == nil {
			return templatesPath
		}
	}

	// Fallback to source code location
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)
	return filepath.Join(filepath.Dir(basepath), "templates", "*")
}

// buildUserFilterClause creates a SQL WHERE clause to filter out ignored users
func buildUserFilterClause() string {
	if globalConfig == nil || len(globalConfig.IgnoreUsers) == 0 {
		return ""
	}

	var filters []string
	for _, user := range globalConfig.IgnoreUsers {
		filters = append(filters, fmt.Sprintf("usename != '%s'", strings.ReplaceAll(user, "'", "''")))
	}

	return "AND " + strings.Join(filters, " AND ")
}

// loadConfig loads the main configuration file including servers and other settings
func loadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// Update the Config struct in loadHAProxyConfig
func loadHAProxyConfig(configPath string) ([]HAProxyPort, []Service, string, int, string, error) {
	type Config struct {
		Ports                []HAProxyPort `yaml:"ports"`
		Services             []Service     `yaml:"services"`
		PMMStatusURL         string        `yaml:"pmm_status_url"`
		BadgeRefreshInterval int           `yaml:"badge_refresh_interval"`
		PMMQanURL            string        `yaml:"pmm_qan_url"`
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, nil, "", 0, "", err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, nil, "", 0, "", err
	}

	return config.Ports, config.Services, config.PMMStatusURL, config.BadgeRefreshInterval, config.PMMQanURL, nil
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
func aggregateUsers(cm *ConnectionManager, servers []PostgreSQLServer) ([]User, []string) {
	var allUsers []User
	databaseSet := make(map[string]bool)
	var dbSetMu sync.Mutex

	for _, server := range servers {
		db, err := cm.GetConnection(server.Name)
		if err != nil {
			log.Printf("Error connecting to server %s: %v", server.Name, err)
			continue
		}

		// Fetch user list
		rows, err := db.Query(`
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
				dbRows, err := db.Query(`
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
					dbRows.Close()
				}
				pending = append(pending, pendingUser{user: user, origName: originalUsername})
			} else {
				// Regular user: candidate DBs are those with CONNECT privilege.
				dbRows, err := db.Query(`
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
					dbRows.Close()
				}
				pending = append(pending, pendingUser{
					user:       user,
					origName:   originalUsername,
					candidates: candidates,
				})
			}
		}
		rows.Close()

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
		var (
			leaderHost    string
			leaderPort    int
			leaderErr     error
			leaderFetched bool
		)
		if len(uniqueDBs) > 0 {
			leaderHost, leaderPort, leaderErr = cm.getLeaderConnectionDetails(server.Name)
			leaderFetched = true
			if leaderErr != nil {
				log.Printf("Error getting leader connection details for %s: %v", server.Name, leaderErr)
			}
		}

		// Probe each unique DB in parallel with a bounded worker pool.
		granteesByDB := make(map[string]map[string]bool)
		var granteesMu sync.Mutex

		if leaderFetched && leaderErr == nil {
			var wg sync.WaitGroup
			sem := make(chan struct{}, 8)

			for dbName := range uniqueDBs {
				wg.Add(1)
				sem <- struct{}{}
				go func(dbName string) {
					defer wg.Done()
					defer func() { <-sem }()

					dbConnStr := fmt.Sprintf(
						"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
						leaderHost,
						leaderPort,
						server.User,
						server.Password,
						dbName,
						server.SSLMode,
					)

					dbConn, err := sql.Open("postgres", dbConnStr)
					if err != nil {
						log.Printf("Error opening connection to %s/%s: %v", server.Name, dbName, err)
						return
					}
					defer dbConn.Close()

					granteeRows, err := dbConn.Query(`
						SELECT DISTINCT grantee
						FROM information_schema.table_privileges
						WHERE table_schema = 'public'
					`)
					if err != nil {
						log.Printf("Error querying grantees for %s/%s: %v", server.Name, dbName, err)
						return
					}
					defer granteeRows.Close()

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
				if !ok {
					continue
				}
				if set[p.origName] {
					qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
					p.user.Databases = append(p.user.Databases, qualifiedDBName)
					dbSetMu.Lock()
					databaseSet[qualifiedDBName] = true
					dbSetMu.Unlock()
				}
			}
			allUsers = append(allUsers, p.user)
		}

		// Populate the create-user form's database list with all non-template,
		// non-'postgres' databases for this server.
		dbRows, err := db.Query(`
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
					qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
					databaseSet[qualifiedDBName] = true
				}
			}
			dbRows.Close()
		}
	}

	allDatabases := make([]string, 0, len(databaseSet))
	for dbName := range databaseSet {
		allDatabases = append(allDatabases, dbName)
	}

	return allUsers, allDatabases
}

func InitServer() {
	// Define command line flags
	var config Config
	var templatesDir string
	var configPath string
	var serverPort string

	// Multi-server mode flags
	flag.StringVar(&configPath, "config", "", "Path to configuration file (enables multi-server mode)")

	// Legacy single-server mode flags (for backward compatibility)
	flag.StringVar(&config.Host, "host", "localhost", "PostgreSQL host")
	flag.IntVar(&config.Port, "port", 5432, "PostgreSQL port")
	flag.StringVar(&config.User, "user", "postgres", "PostgreSQL user")
	flag.StringVar(&config.Password, "password", "", "PostgreSQL password")
	flag.StringVar(&config.DBName, "dbname", "postgres", "PostgreSQL database name")
	flag.StringVar(&config.SSLMode, "sslmode", "disable", "PostgreSQL SSL mode")

	// Other flags
	flag.StringVar(&templatesDir, "templates", "", "Path to templates directory")
	flag.StringVar(&serverPort, "server-port", "8080", "Server port to listen on")
	flag.Parse()

	log.Println("Initializing server...")

	var connectionManager *ConnectionManager
	var servers []PostgreSQLServer

	// Determine if we're in multi-server or single-server mode
	if configPath != "" {
		// Multi-server mode: load from config file
		log.Printf("Loading configuration from: %s", configPath)
		loadedConfig, err := loadConfig(configPath)
		if err != nil {
			log.Fatal("Failed to load configuration:", err)
		}

		config = *loadedConfig
		globalConfig = &config // Set global config for use in helper functions
		if len(config.Servers) == 0 {
			log.Fatal("No servers configured in config file")
		}

		servers = config.Servers
		log.Printf("Configured %d PostgreSQL servers", len(servers))
		if len(config.IgnoreUsers) > 0 {
			log.Printf("Configured to ignore %d users in active queries: %v", len(config.IgnoreUsers), config.IgnoreUsers)
		}
	} else {
		log.Fatal("Configuration file is required - single server mode is not supported. Please use -config flag with a Patroni cluster configuration.")
	}

	// Initialize connection manager
	connectionManager = NewConnectionManager(servers)
	defer connectionManager.Close()

	// Test connections to all servers
	for _, server := range servers {
		_, err := connectionManager.GetConnection(server.Name)
		if err != nil {
			log.Printf("Warning: Failed to connect to server %s: %v", server.Name, err)
		}
	}

	router := gin.Default()

	// Set the function map first
	router.SetFuncMap(template.FuncMap{
		"replaceBadgeWithDashboard": replaceBadgeWithDashboard,
		"pmmEmbedURL":               pmmEmbedURL,
		"pmmOpenURL":                pmmOpenURL,
		// stripServer drops a trailing "@server" suffix from compound keys
		// (e.g. "alice@calik-yepas-patroni" -> "alice") for display only.
		// The original value must still be used for CRUD calls that depend
		// on the username@server / database@server compound format.
		"stripServer": func(s string) string {
			if idx := strings.Index(s, "@"); idx != -1 {
				return s[:idx]
			}
			return s
		},
	})

	// Then load the templates
	templatesPath := getTemplatesDir(templatesDir)
	log.Printf("Loading templates from: %s", templatesPath)
	router.LoadHTMLGlob(templatesPath)

	// Load additional config if not already loaded from main config file
	var haPorts []HAProxyPort
	var services []Service
	var pmmURL string
	var badgeRefreshInterval int
	var queryIframeURL string

	if configPath != "" {
		// Use values from loaded config
		pmmURL = config.PMMStatusURL
		queryIframeURL = config.PMMQanURL
		badgeRefreshInterval = config.BadgeRefreshInterval
		if badgeRefreshInterval == 0 {
			badgeRefreshInterval = 3000
		}

		// Convert HAProxyPorts map to slice
		haPorts = config.Ports
		log.Printf("Loaded %d HAProxy ports from config", len(haPorts))

		// Use Services directly from config
		services = config.Services
		log.Printf("Loaded %d services from config", len(services))
	} else {
		// Try to load legacy HAProxy config
		legacyConfigPath := "config/haproxy.yaml"
		var err error
		haPorts, services, pmmURL, badgeRefreshInterval, queryIframeURL, err = loadHAProxyConfig(legacyConfigPath)
		if err != nil {
			log.Printf("Warning: Failed to load legacy config: %v", err)
			haPorts = []HAProxyPort{}
			services = []Service{}
			pmmURL = ""
			queryIframeURL = ""
			badgeRefreshInterval = 3000
		}
		// Set the values in config struct
		config.PMMQanURL = queryIframeURL
		config.PMMStatusURL = pmmURL
	}

	// Add this route after the existing routes
	router.GET("/", func(c *gin.Context) {
		// Test connectivity to all servers and gather basic stats
		var serverStatuses []map[string]interface{}

		for _, server := range servers {
			status := map[string]interface{}{
				"name":      server.Name,
				"host":      "patroni-cluster",
				"port":      "dynamic",
				"status":    "disconnected",
				"users":     0,
				"databases": 0,
			}

			// Discover Patroni cluster (all servers are Patroni clusters now)
			cluster, err := connectionManager.discoverPatroniCluster(server)
			if err != nil {
				log.Printf("Error discovering Patroni cluster %s: %v", server.Name, err)
				status["error"] = err.Error()
			} else {
				status["use_patroni"] = true
				status["patroni_cluster"] = cluster
				status["status"] = "patroni"
			}

			db, err := connectionManager.GetConnection(server.Name)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", server.Name, err)
			} else {
				// Server is connected
				status["status"] = "connected"

				// Get user count
				var userCount int
				err = db.QueryRow("SELECT COUNT(*) FROM pg_user").Scan(&userCount)
				if err == nil {
					status["users"] = userCount
				}

				// Get database count
				var dbCount int
				err = db.QueryRow("SELECT COUNT(*) FROM pg_database WHERE datistemplate = false").Scan(&dbCount)
				if err == nil {
					status["databases"] = dbCount
				}
			}

			serverStatuses = append(serverStatuses, status)
		}

		log.Printf("Status page: Passing %d HAPorts and %d Services to template", len(haPorts), len(services))

		c.HTML(200, "status.html", gin.H{
			"Servers":              servers,
			"ServerStatuses":       serverStatuses,
			"HAPorts":              haPorts,
			"Services":             services,
			"PMMStatusURL":         pmmURL,
			"PMMQanURL":            queryIframeURL,
			"BadgeRefreshInterval": badgeRefreshInterval,
		})
	})

	router.GET("/users", func(c *gin.Context) {
		allUsers, allDatabases := aggregateUsers(connectionManager, servers)
		c.HTML(200, "users.html", PageData{
			Servers:              servers,
			SelectedServer:       "",
			Users:                allUsers,
			Databases:            allDatabases,
			HAPorts:              haPorts,
			Services:             services,
			PMMStatusURL:         pmmURL,
			PMMQanURL:            queryIframeURL,
			BadgeRefreshInterval: badgeRefreshInterval,
		})
	})

	// Group API routes under /api/v1
	v1 := router.Group("/api/v1")
	{
		// Add server list endpoint
		v1.GET("/servers", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"servers": servers,
			})
		})

		// Add dedicated status endpoint for lightweight status data refresh
		v1.GET("/status", func(c *gin.Context) {
			// Add cache headers to prevent unnecessary requests
			c.Header("Cache-Control", "no-cache, must-revalidate")
			c.Header("Last-Modified", time.Now().UTC().Format(time.RFC1123))

			statusData := gin.H{
				"HAPorts":              haPorts,
				"Services":             services,
				"PMMStatusURL":         config.PMMStatusURL,
				"PMMQanURL":            config.PMMQanURL,
				"BadgeRefreshInterval": config.BadgeRefreshInterval,
				"ServerTime":           time.Now().Unix(),
				"Version":              "1.0", // For API versioning
			}

			// Add timing for performance monitoring
			start := time.Now()
			defer func() {
				duration := time.Since(start)
				log.Printf("Status endpoint served in %v: %d ports, %d services",
					duration, len(haPorts), len(services))
			}()

			c.JSON(200, statusData)
		})

		v1.GET("/users", func(c *gin.Context) {
			allUsers, allDatabases := aggregateUsers(connectionManager, servers)
			c.JSON(200, PageData{
				Servers:              servers,
				SelectedServer:       "",
				Users:                allUsers,
				Databases:            allDatabases,
				HAPorts:              haPorts,
				Services:             services,
				PMMStatusURL:         pmmURL,
				PMMQanURL:            queryIframeURL,
				BadgeRefreshInterval: badgeRefreshInterval,
			})
		})

		v1.POST("/users", func(c *gin.Context) {
			var req CreateUserRequest
			if err := c.BindJSON(&req); err != nil {
				log.Printf("Bind error: %v", err)
				c.JSON(400, gin.H{"error": "Invalid form data"})
				return
			}

			// Parse server information from database names (format: "dbname@servername")
			serverDatabases := make(map[string][]string)
			for _, dbName := range req.Databases {
				if strings.Contains(dbName, "@") {
					parts := strings.Split(dbName, "@")
					if len(parts) == 2 {
						db, server := parts[0], parts[1]
						if db == "postgres" {
							c.JSON(400, gin.H{"error": "Cannot grant access to postgres database"})
							return
						}
						serverDatabases[server] = append(serverDatabases[server], db)
					}
				}
			}

			if len(serverDatabases) == 0 {
				c.JSON(400, gin.H{"error": "No valid databases selected"})
				return
			}

			// Create user on each server that has selected databases
			for serverName, databases := range serverDatabases {
				db, err := connectionManager.GetConnection(serverName)
				if err != nil {
					log.Printf("Error connecting to server %s: %v", serverName, err)
					c.JSON(500, gin.H{"error": fmt.Sprintf("Failed to connect to server %s", serverName)})
					return
				}

				// First check if user exists
				var exists bool
				err = db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", req.Username).Scan(&exists)
				if err != nil {
					log.Printf("Error checking user existence on %s: %v", serverName, err)
					c.JSON(500, gin.H{"error": fmt.Sprintf("Failed to check user on server %s", serverName)})
					return
				}
				if exists {
					c.JSON(400, gin.H{"error": fmt.Sprintf("User already exists on server %s", serverName)})
					return
				}

				// Create user with proper parameter handling
				createQuery := fmt.Sprintf(
					"CREATE USER %s WITH PASSWORD '%s'",
					req.Username,
					req.Password,
				)

				if _, err = db.Exec(createQuery); err != nil {
					log.Printf("Error creating user on %s: %v", serverName, err)
					errorMsg := fmt.Sprintf("Failed to create user on server %s", serverName)
					errStr := err.Error()

					switch {
					case strings.Contains(errStr, "permission denied"):
						errorMsg = fmt.Sprintf("Permission denied on server %s - insufficient privileges", serverName)
					case strings.Contains(errStr, "password authentication failed"):
						errorMsg = "Invalid password format"
					case strings.Contains(errStr, "role name contains invalid characters"):
						errorMsg = "Username contains invalid characters"
					}

					c.JSON(400, gin.H{"error": errorMsg})
					return
				}

				// Grant permissions to selected databases on this server
				for _, dbName := range databases {
					// First grant connect privilege
					grantQuery := fmt.Sprintf(`GRANT CONNECT ON DATABASE "%s" TO "%s"`,
						strings.Replace(dbName, `"`, `""`, -1),
						strings.Replace(req.Username, `"`, `""`, -1),
					)
					if _, err = db.Exec(grantQuery); err != nil {
						log.Printf("Error granting CONNECT on %s/%s: %v", serverName, dbName, err)
						continue
					}

					// Connect to the database to grant schema-level privileges
					serverConfig, exists := connectionManager.GetServerConfig(serverName)
					if !exists {
						continue
					}

					// Get leader connection details from Patroni cluster
					host, port, err := connectionManager.getLeaderConnectionDetails(serverName)
					if err != nil {
						log.Printf("Error getting leader connection details for %s: %v", serverName, err)
						continue
					}

					dbConnStr := fmt.Sprintf(
						"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
						host,
						port,
						serverConfig.User,
						serverConfig.Password,
						dbName,
						serverConfig.SSLMode,
					)

					dbConn, err := sql.Open("postgres", dbConnStr)
					if err != nil {
						log.Printf("Error connecting to database %s/%s: %v", serverName, dbName, err)
						continue
					}
					defer dbConn.Close()

					// Grant schema-level privileges
					schemaQueries := []string{
						fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "%s"`,
							strings.Replace(req.Username, `"`, `""`, -1)),
						fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "%s"`,
							strings.Replace(req.Username, `"`, `""`, -1)),
						fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "%s"`,
							strings.Replace(req.Username, `"`, `""`, -1)),
						fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO "%s"`,
							strings.Replace(req.Username, `"`, `""`, -1)),
					}

					for _, query := range schemaQueries {
						if _, err = dbConn.Exec(query); err != nil {
							log.Printf("Error executing schema grant query on %s/%s: %v", serverName, dbName, err)
							// Continue with other queries even if one fails
						}
					}
				}
			}

			c.JSON(200, gin.H{"message": "User created successfully on all selected servers"})
		})

		v1.DELETE("/users/:username", func(c *gin.Context) {
			usernameWithServer := c.Param("username")

			// Parse username and server (format: "username@servername")
			if !strings.Contains(usernameWithServer, "@") {
				c.JSON(400, gin.H{"error": "Invalid username format. Expected username@server"})
				return
			}

			parts := strings.Split(usernameWithServer, "@")
			if len(parts) != 2 {
				c.JSON(400, gin.H{"error": "Invalid username format. Expected username@server"})
				return
			}

			username, serverName := parts[0], parts[1]

			db, err := connectionManager.GetConnection(serverName)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", serverName, err)
				c.JSON(500, gin.H{"error": fmt.Sprintf("Failed to connect to server %s", serverName)})
				return
			}

			// Don't allow deletion of current user
			serverConfig, exists := connectionManager.GetServerConfig(serverName)
			if exists && username == serverConfig.User {
				c.JSON(400, gin.H{"error": "Cannot delete the current user"})
				return
			}

			// First get all databases
			rows, err := db.Query(`
				SELECT datname
				FROM pg_database
				WHERE datistemplate = false
			`)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var dbName string
					if err := rows.Scan(&dbName); err == nil {
						// Connect to each database to revoke schema-level privileges
						// Get leader connection details from Patroni cluster
						host, port, err := connectionManager.getLeaderConnectionDetails(serverName)
						if err != nil {
							log.Printf("Error getting leader connection details for %s: %v", serverName, err)
							continue
						}

						dbConnStr := fmt.Sprintf(
							"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
							host,
							port,
							serverConfig.User,
							serverConfig.Password,
							dbName,
							serverConfig.SSLMode,
						)

						dbConn, err := sql.Open("postgres", dbConnStr)
						if err != nil {
							log.Printf("Error connecting to database %s/%s: %v", serverName, dbName, err)
							continue
						}
						defer dbConn.Close()

						// Revoke all privileges and reassign owned objects
						revokeQueries := []string{
							fmt.Sprintf(`REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM "%s"`, username),
							fmt.Sprintf(`REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM "%s"`, username),
							fmt.Sprintf(`REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM "%s"`, username),
							fmt.Sprintf(`REVOKE ALL PRIVILEGES ON DATABASE "%s" FROM "%s"`, dbName, username),
							fmt.Sprintf(`REASSIGN OWNED BY "%s" TO postgres`, username),
							fmt.Sprintf(`DROP OWNED BY "%s"`, username),
						}

						for _, query := range revokeQueries {
							if _, err := dbConn.Exec(query); err != nil {
								log.Printf("Error executing revoke query on %s/%s: %v", serverName, dbName, err)
								// Continue with other queries even if one fails
							}
						}
					}
				}
			}

			// Finally drop the user
			dropQuery := fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username)
			_, err = db.Exec(dropQuery)
			if err != nil {
				log.Printf("Error deleting user %s on %s: %v", username, serverName, err)
				c.JSON(500, gin.H{"error": fmt.Sprintf("Failed to delete user on server %s", serverName)})
				return
			}

			c.JSON(200, gin.H{"message": fmt.Sprintf("User deleted successfully from server %s", serverName)})
		})

		// Add a new endpoint for queries in JSON format - aggregate from all servers
		v1.GET("/queries", func(c *gin.Context) {
			type QueryWithServer struct {
				Query
				Server     string `json:"server"`
				BackendPID int    `json:"backend_pid"`
				MemberName string `json:"member_name"`
				MemberHost string `json:"member_host"`
				MemberPort int    `json:"member_port"`
				MemberRole string `json:"member_role"`
			}

			var allQueries []QueryWithServer

			for _, server := range servers {
				backends := connectionManager.GetBackendConnections(server.Name)
				if len(backends) == 0 {
					log.Printf("No backend connections available for server %s", server.Name)
					continue
				}

				for _, backend := range backends {
					userFilter := buildUserFilterClause()
					querySQL := fmt.Sprintf(`
						SELECT DISTINCT ON (COALESCE(leader_pid, pid))
							COALESCE(leader_pid, pid) AS pid,
							usename,
						COALESCE(datname, 'system') as datname,
						ROUND(EXTRACT(EPOCH FROM now() - query_start))::text || 's' as duration,
						query
						FROM pg_stat_activity
						WHERE state = 'active'
						AND query NOT ILIKE '%%pg_stat_activity%%'
						AND pid != pg_backend_pid()
						%s
						ORDER BY pid, query_start ASC;
					`, userFilter)

					rows, err := backend.DB.Query(querySQL)

					if err != nil {
						log.Printf("Error querying active queries from server %s backend PID %d: %v", server.Name, backend.PID, err)
						continue
					}

					for rows.Next() {
						var q Query
						err := rows.Scan(&q.PID, &q.Username, &q.Database, &q.Duration, &q.Query)
						if err != nil {
							log.Printf("Error scanning query row from server %s backend PID %d: %v", server.Name, backend.PID, err)
							continue
						}
						// Server identity is carried in QueryWithServer.Server below;
						// don't pollute the displayed username/database with a suffix.

						qWithServer := QueryWithServer{
							Query:      q,
							Server:     server.Name,
							BackendPID: backend.PID,
							MemberName: backend.MemberName,
							MemberHost: backend.Host,
							MemberPort: backend.Port,
							MemberRole: backend.Role,
						}
						allQueries = append(allQueries, qWithServer)
					}
					rows.Close()
				}
			}

			c.JSON(200, gin.H{"queries": allQueries})
		})

		// Add this endpoint to the API group - handle server-specific query termination
		v1.DELETE("/queries/:pid", func(c *gin.Context) {
			pid := c.Param("pid")
			serverName := c.Query("server")

			if serverName == "" {
				c.JSON(400, gin.H{"error": "Server parameter is required"})
				return
			}

			backends := connectionManager.GetBackendConnections(serverName)
			if len(backends) == 0 {
				log.Printf("No backend connections available for server %s", serverName)
				c.JSON(500, gin.H{"error": fmt.Sprintf("No connections available for server %s", serverName)})
				return
			}

			// Try to find and kill the query on any backend of this server
			var queryFound bool
			var killSuccessful bool

			for _, backend := range backends {
				// First check if query exists on this backend
				var exists bool
				err := backend.DB.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&exists)
				if err != nil {
					log.Printf("Error checking query existence on %s backend PID %d: %v", serverName, backend.PID, err)
					continue
				}

				if exists {
					queryFound = true
					// Kill the query and check the result boolean
					var ok bool
					err = backend.DB.QueryRow("SELECT pg_terminate_backend($1)", pid).Scan(&ok)
					if err != nil {
						log.Printf("Terminate backend %s failed on %s backend PID %d: %v", pid, serverName, backend.PID, err)
						continue
					}

					if ok {
						killSuccessful = true
						log.Printf("Successfully killed query %s on server %s backend PID %d", pid, serverName, backend.PID)
						c.JSON(200, gin.H{"message": fmt.Sprintf("Query killed successfully on server %s", serverName)})
						return
					} else {
						log.Printf("Terminate backend %s returned false on %s backend PID %d", pid, serverName, backend.PID)
					}
				}
			}

			if !queryFound {
				c.JSON(404, gin.H{"error": "Query not found on any backend"})
				return
			}

			if !killSuccessful {
				c.JSON(409, gin.H{"error": "Query could not be terminated"})
				return
			}
		})

		// Cluster topology endpoint - returns Patroni cluster members enriched
		// with per-replica lag detail pulled from pg_stat_replication on the leader.
		v1.GET("/topology", func(c *gin.Context) {
			c.Header("Cache-Control", "no-cache")

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

				rows, qErr := db.Query(`
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
				rows.Close()

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

	// Add the query route handler for the query page - aggregate from all servers
	router.GET("/query", func(c *gin.Context) {
		type QueryWithServer struct {
			Query
			Server     string `json:"server"`
			BackendPID int    `json:"backend_pid"`
			MemberName string `json:"member_name"`
			MemberHost string `json:"member_host"`
			MemberPort int    `json:"member_port"`
			MemberRole string `json:"member_role"`
		}

		var allQueries []QueryWithServer

		for _, server := range servers {
			backends := connectionManager.GetBackendConnections(server.Name)
			if len(backends) == 0 {
				log.Printf("No backend connections available for server %s", server.Name)
				continue
			}

			for _, backend := range backends {
				userFilter := buildUserFilterClause()
				querySQL := fmt.Sprintf(`
						SELECT DISTINCT ON (COALESCE(leader_pid, pid))
							COALESCE(leader_pid, pid) AS pid,
							usename,
						COALESCE(datname, 'system') as datname,
						ROUND(EXTRACT(EPOCH FROM now() - query_start))::text || 's' as duration,
						query
						FROM pg_stat_activity
						WHERE state = 'active'
						AND query NOT ILIKE '%%pg_stat_activity%%'
						AND pid != pg_backend_pid()
						%s
						ORDER BY pid, query_start ASC;
				`, userFilter)

				rows, err := backend.DB.Query(querySQL)
				if err != nil {
					log.Printf("Error querying active queries from server %s backend PID %d: %v", server.Name, backend.PID, err)
					continue
				}

				for rows.Next() {
					var q Query
					err := rows.Scan(&q.PID, &q.Username, &q.Database, &q.Duration, &q.Query)
					if err != nil {
						log.Printf("Error scanning query row from server %s backend PID %d: %v", server.Name, backend.PID, err)
						continue
					}
					// Server identity is carried in QueryWithServer.Server below;
					// don't pollute the displayed username/database with a suffix.

					qWithServer := QueryWithServer{
						Query:      q,
						Server:     server.Name,
						BackendPID: backend.PID,
						MemberName: backend.MemberName,
						MemberHost: backend.Host,
						MemberPort: backend.Port,
						MemberRole: backend.Role,
					}
					allQueries = append(allQueries, qWithServer)
				}
				rows.Close()
			}
		}

		c.HTML(200, "query.html", gin.H{
			"Servers": servers,
			"Queries": allQueries,
		})
	})

	// Add route handler for query analytics page
	router.GET("/query-analytics", func(c *gin.Context) {
		c.HTML(200, "query_analytics.html", gin.H{
			"Servers":   servers,
			"PMMQanURL": config.PMMQanURL,
		})
	})

	// Cluster topology page. Actual data is fetched client-side from
	// /api/v1/topology and refreshed every 5 seconds.
	router.GET("/topology", func(c *gin.Context) {
		c.HTML(200, "topology.html", gin.H{
			"Servers": servers,
			"HAPorts": haPorts,
		})
	})

	// Start the server
	log.Printf("Starting server on :%s", serverPort)
	if err := router.Run(":" + serverPort); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

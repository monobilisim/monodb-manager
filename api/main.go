package api

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
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

// PostgreSQLServer represents a single PostgreSQL server configuration
type PostgreSQLServer struct {
	Name             string `yaml:"name"`
	Host             string `yaml:"host"`
	Port             int    `yaml:"port"`
	User             string `yaml:"user"`
	Password         string `yaml:"password"`
	DBName           string `yaml:"dbname"`
	SSLMode          string `yaml:"sslmode"`
	DiscoverBackends bool   `yaml:"discover_backends,omitempty"`
	MaxAttempts      int    `yaml:"attempts,omitempty"`
	StableRepeats    int    `yaml:"stable_repeats,omitempty"`
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

	// PMM iframe URL for the status page
	PMMIframeURL string `yaml:"pmm_iframe"`

	// Query Analytics iframe URL
	QueryIframeURL string `yaml:"query_iframe"`

	// HAProxy port badges
	Ports []HAProxyPort `yaml:"ports"`

	// Services configuration
	Services []Service `yaml:"services"`

	// Refresh intervals (in seconds)
	BadgeRefreshInterval int `yaml:"badge_refresh_interval"`
}

// BackendConn represents a connection to a specific backend with its PID
type BackendConn struct {
	DB  *sql.DB
	PID int
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

// establishConnection creates new database connection(s)
func (cm *ConnectionManager) establishConnection(serverName string) (*sql.DB, error) {
	config, exists := cm.configs[serverName]
	if !exists {
		return nil, fmt.Errorf("server %s not found in configuration", serverName)
	}

	if config.DiscoverBackends {
		if err := cm.discoverBackends(config); err != nil {
			return nil, err
		}
		backends := cm.GetBackendConnections(serverName)
		if len(backends) > 0 {
			return backends[0].DB, nil
		}
		return nil, fmt.Errorf("no backends discovered for server %s", serverName)
	}

	// Single connection mode (legacy behavior)
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Double-check if connection was created while we were waiting for the lock
	if backends, exists := cm.backends[serverName]; exists && len(backends) > 0 {
		if err := backends[0].DB.Ping(); err == nil {
			return backends[0].DB, nil
		}
		// Remove stale connection
		for _, backend := range backends {
			backend.DB.Close()
		}
		delete(cm.backends, serverName)
	}

	db, err := cm.createSingleConnection(config)
	if err != nil {
		return nil, err
	}

	// Get PID for this connection
	var pid int
	if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&pid); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to get backend PID for %s: %v", serverName, err)
	}

	cm.backends[serverName] = []BackendConn{{DB: db, PID: pid}}
	log.Printf("Established connection to PostgreSQL server: %s (%s:%d) PID: %d", serverName, config.Host, config.Port, pid)
	return db, nil
}

// createSingleConnection creates a single database connection
func (cm *ConnectionManager) createSingleConnection(config PostgreSQLServer) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host,
		config.Port,
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

// discoverBackends discovers all backend connections behind a load balancer
func (cm *ConnectionManager) discoverBackends(config PostgreSQLServer) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// Set defaults
	maxAttempts := config.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 12
	}
	stableRepeats := config.StableRepeats
	if stableRepeats == 0 {
		stableRepeats = 4
	}

	seenPIDs := make(map[int]bool)
	var backends []BackendConn
	dupStreak := 0

	log.Printf("Discovering backends for server %s (max attempts: %d, stable repeats: %d)", config.Name, maxAttempts, stableRepeats)

	for i := 0; i < maxAttempts; i++ {
		db, err := cm.createSingleConnection(config)
		if err != nil {
			log.Printf("Failed to create connection attempt %d for %s: %v", i+1, config.Name, err)
			continue
		}

		var pid int
		if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&pid); err != nil {
			log.Printf("Failed to get backend PID for %s attempt %d: %v", config.Name, i+1, err)
			db.Close()
			continue
		}

		if !seenPIDs[pid] {
			seenPIDs[pid] = true
			backends = append(backends, BackendConn{DB: db, PID: pid})
			dupStreak = 0
			log.Printf("Discovered new backend for %s: PID %d", config.Name, pid)
		} else {
			dupStreak++
			db.Close()
			log.Printf("Duplicate PID %d for %s (streak: %d/%d)", pid, config.Name, dupStreak, stableRepeats)
			if dupStreak >= stableRepeats {
				break
			}
		}
	}

	if len(backends) == 0 {
		return fmt.Errorf("no backends discovered for server %s", config.Name)
	}

	cm.backends[config.Name] = backends
	log.Printf("Successfully discovered %d backends for server %s", len(backends), config.Name)
	return nil
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
	PMMURL               string
	QueryIframeURL       string
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
		PMMURL               string        `yaml:"pmm_url"`
		BadgeRefreshInterval int           `yaml:"badge_refresh_interval"`
		QueryIframeURL       string        `yaml:"query_iframe"`
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, nil, "", 0, "", err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, nil, "", 0, "", err
	}

	return config.Ports, config.Services, config.PMMURL, config.BadgeRefreshInterval, config.QueryIframeURL, nil
}

// Update this function before InitServer
func replaceBadgeWithDashboard(url string) string {
	// First replace api/badge with dashboard
	url = strings.Replace(url, "/api/badge/", "/dashboard/", 1)
	// Then remove everything after ? if it exists
	if idx := strings.Index(url, "?"); idx != -1 {
		url = url[:idx]
	}
	return url
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
		if len(config.Servers) == 0 {
			log.Fatal("No servers configured in config file")
		}

		servers = config.Servers
		log.Printf("Configured %d PostgreSQL servers", len(servers))
	} else {
		// Single-server mode: use CLI flags (backward compatibility)
		log.Println("Using single-server mode (legacy)")
		if config.Password == "" {
			log.Fatal("Password is required in single-server mode")
		}

		servers = []PostgreSQLServer{
			{
				Name:     "default",
				Host:     config.Host,
				Port:     config.Port,
				User:     config.User,
				Password: config.Password,
				DBName:   config.DBName,
				SSLMode:  config.SSLMode,
			},
		}
		config.Servers = servers
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
		pmmURL = config.PMMIframeURL
		queryIframeURL = config.QueryIframeURL
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
		config.QueryIframeURL = queryIframeURL
		config.PMMIframeURL = pmmURL
	}

	// Add this route after the existing routes
	router.GET("/", func(c *gin.Context) {
		// Test connectivity to all servers and gather basic stats
		var serverStatuses []map[string]interface{}

		for _, server := range servers {
			status := map[string]interface{}{
				"name":      server.Name,
				"host":      server.Host,
				"port":      server.Port,
				"status":    "disconnected",
				"users":     0,
				"databases": 0,
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
			"PMMURL":               pmmURL,
			"QueryIframeURL":       queryIframeURL,
			"BadgeRefreshInterval": badgeRefreshInterval,
		})
	})

	router.GET("/users", func(c *gin.Context) {
		// Aggregate users from all servers
		var allUsers []User
		var allDatabases []string
		databaseSet := make(map[string]bool)

		for _, server := range servers {
			db, err := connectionManager.GetConnection(server.Name)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", server.Name, err)
				continue
			}

			// Get users from this server
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
			defer rows.Close()

			for rows.Next() {
				var user User
				err := rows.Scan(
					&user.Username,
					&user.Superuser,
					&user.CreateDB,
					&user.CreateRole,
					&user.ValidUntil,
				)
				if err != nil {
					log.Printf("Error scanning user row from server %s: %v", server.Name, err)
					continue
				}

				// Add server name to username for identification
				user.Username = fmt.Sprintf("%s@%s", user.Username, server.Name)

				// Get database access for this user
				originalUsername := strings.Split(user.Username, "@")[0]
				if user.Superuser || originalUsername == "postgres" {
					// Superusers and postgres user have access to all databases
					dbRows, err := db.Query(`
						SELECT datname
						FROM pg_database
						WHERE datistemplate = false
						ORDER BY datname
					`)
					if err == nil {
						defer dbRows.Close()
						for dbRows.Next() {
							var dbName string
							if err := dbRows.Scan(&dbName); err == nil {
								qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
								user.Databases = append(user.Databases, qualifiedDBName)
								databaseSet[qualifiedDBName] = true
							}
						}
					}
				} else {
					// For regular users, check specific grants
					dbRows, err := db.Query(`
						SELECT d.datname
						FROM pg_database d
						WHERE d.datistemplate = false
						AND has_database_privilege($1, d.datname, 'CONNECT')
						ORDER BY d.datname
					`, originalUsername)

					if err == nil {
						defer dbRows.Close()
						var databases []string
						for dbRows.Next() {
							var dbName string
							if err := dbRows.Scan(&dbName); err == nil {
								databases = append(databases, dbName)
							}
						}

						// Check each database for actual privileges
						for _, dbName := range databases {
							func() {
								dbConnStr := fmt.Sprintf(
									"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
									server.Host,
									server.Port,
									server.User,
									server.Password,
									dbName,
									server.SSLMode,
								)

								dbConn, err := sql.Open("postgres", dbConnStr)
								if err != nil {
									return
								}
								defer dbConn.Close()

								// Check for actual table access
								var hasAccess bool
								err = dbConn.QueryRow(`
									SELECT EXISTS (
										SELECT 1
										FROM information_schema.table_privileges
										WHERE grantee = $1
										AND table_schema = 'public'
									)
								`, originalUsername).Scan(&hasAccess)

								if err == nil && hasAccess {
									qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
									user.Databases = append(user.Databases, qualifiedDBName)
									databaseSet[qualifiedDBName] = true
								}
							}()
						}
					}
				}

				allUsers = append(allUsers, user)
			}

			// Get all databases from this server for the create form
			dbRows, err := db.Query(`
				SELECT datname
				FROM pg_database
				WHERE datistemplate = false
				AND datname != 'postgres'
				ORDER BY datname
			`)
			if err == nil {
				defer dbRows.Close()
				for dbRows.Next() {
					var dbName string
					if err := dbRows.Scan(&dbName); err == nil {
						qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
						databaseSet[qualifiedDBName] = true
					}
				}
			}
		}

		// Convert database set to slice
		for dbName := range databaseSet {
			allDatabases = append(allDatabases, dbName)
		}

		c.HTML(200, "users.html", PageData{
			Servers:              servers,
			SelectedServer:       "",
			Users:                allUsers,
			Databases:            allDatabases,
			HAPorts:              haPorts,
			Services:             services,
			PMMURL:               pmmURL,
			QueryIframeURL:       queryIframeURL,
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

		v1.GET("/users", func(c *gin.Context) {
			// Aggregate users from all servers
			var allUsers []User
			var allDatabases []string
			databaseSet := make(map[string]bool)

			for _, server := range servers {
				db, err := connectionManager.GetConnection(server.Name)
				if err != nil {
					log.Printf("Error connecting to server %s: %v", server.Name, err)
					continue
				}

				// Get users from this server
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
				defer rows.Close()

				for rows.Next() {
					var user User
					err := rows.Scan(
						&user.Username,
						&user.Superuser,
						&user.CreateDB,
						&user.CreateRole,
						&user.ValidUntil,
					)
					if err != nil {
						log.Printf("Error scanning user row from server %s: %v", server.Name, err)
						continue
					}

					// Add server name to username for identification
					user.Username = fmt.Sprintf("%s@%s", user.Username, server.Name)

					// Get database access for this user
					originalUsername := strings.Split(user.Username, "@")[0]
					if user.Superuser || originalUsername == "postgres" {
						// Superusers and postgres user have access to all databases
						dbRows, err := db.Query(`
							SELECT datname
							FROM pg_database
							WHERE datistemplate = false
							ORDER BY datname
						`)
						if err == nil {
							defer dbRows.Close()
							for dbRows.Next() {
								var dbName string
								if err := dbRows.Scan(&dbName); err == nil {
									qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
									user.Databases = append(user.Databases, qualifiedDBName)
									databaseSet[qualifiedDBName] = true
								}
							}
						}
					} else {
						// For regular users, check specific grants
						dbRows, err := db.Query(`
							SELECT d.datname
							FROM pg_database d
							WHERE d.datistemplate = false
							AND has_database_privilege($1, d.datname, 'CONNECT')
							ORDER BY d.datname
						`, originalUsername)

						if err == nil {
							defer dbRows.Close()
							var databases []string
							for dbRows.Next() {
								var dbName string
								if err := dbRows.Scan(&dbName); err == nil {
									databases = append(databases, dbName)
								}
							}

							// Check each database for actual privileges
							for _, dbName := range databases {
								func() {
									dbConnStr := fmt.Sprintf(
										"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
										server.Host,
										server.Port,
										server.User,
										server.Password,
										dbName,
										server.SSLMode,
									)

									dbConn, err := sql.Open("postgres", dbConnStr)
									if err != nil {
										return
									}
									defer dbConn.Close()

									// Check for actual table access
									var hasAccess bool
									err = dbConn.QueryRow(`
										SELECT EXISTS (
											SELECT 1
											FROM information_schema.table_privileges
											WHERE grantee = $1
											AND table_schema = 'public'
										)
									`, originalUsername).Scan(&hasAccess)

									if err == nil && hasAccess {
										qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
										user.Databases = append(user.Databases, qualifiedDBName)
										databaseSet[qualifiedDBName] = true
									}
								}()
							}
						}
					}

					allUsers = append(allUsers, user)
				}

				// Get all databases from this server for the create form
				dbRows, err := db.Query(`
					SELECT datname
					FROM pg_database
					WHERE datistemplate = false
					AND datname != 'postgres'
					ORDER BY datname
				`)
				if err == nil {
					defer dbRows.Close()
					for dbRows.Next() {
						var dbName string
						if err := dbRows.Scan(&dbName); err == nil {
							qualifiedDBName := fmt.Sprintf("%s@%s", dbName, server.Name)
							databaseSet[qualifiedDBName] = true
						}
					}
				}
			}

			// Convert database set to slice
			for dbName := range databaseSet {
				allDatabases = append(allDatabases, dbName)
			}

			c.JSON(200, PageData{
				Servers:              servers,
				SelectedServer:       "",
				Users:                allUsers,
				Databases:            allDatabases,
				HAPorts:              haPorts,
				Services:             services,
				PMMURL:               pmmURL,
				QueryIframeURL:       queryIframeURL,
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

					dbConnStr := fmt.Sprintf(
						"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
						serverConfig.Host,
						serverConfig.Port,
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
						dbConnStr := fmt.Sprintf(
							"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
							serverConfig.Host,
							serverConfig.Port,
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
			}

			var allQueries []QueryWithServer

			for _, server := range servers {
				backends := connectionManager.GetBackendConnections(server.Name)
				if len(backends) == 0 {
					log.Printf("No backend connections available for server %s", server.Name)
					continue
				}

				for _, backend := range backends {
					rows, err := backend.DB.Query(`
						SELECT
							pid,
							usename,
							COALESCE(datname, 'system') as datname,
							EXTRACT(EPOCH FROM now() - query_start)::text || 's' as duration,
							query
						FROM pg_stat_activity
						WHERE state = 'active'
						AND query NOT ILIKE '%pg_stat_activity%'
						AND pid != pg_backend_pid()
						ORDER BY query_start DESC
					`)

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
						// Add server information to identify where the query is running
						q.Username = fmt.Sprintf("%s@%s", q.Username, server.Name)
						q.Database = fmt.Sprintf("%s@%s", q.Database, server.Name)

						qWithServer := QueryWithServer{
							Query:      q,
							Server:     server.Name,
							BackendPID: backend.PID,
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
	}

	// Add the query route handler for the query page - aggregate from all servers
	router.GET("/query", func(c *gin.Context) {
		type QueryWithServer struct {
			Query
			Server     string `json:"server"`
			BackendPID int    `json:"backend_pid"`
		}

		var allQueries []QueryWithServer

		for _, server := range servers {
			backends := connectionManager.GetBackendConnections(server.Name)
			if len(backends) == 0 {
				log.Printf("No backend connections available for server %s", server.Name)
				continue
			}

			for _, backend := range backends {
				rows, err := backend.DB.Query(`
					SELECT
						pid,
						usename,
						COALESCE(datname, 'system') as datname,
						EXTRACT(EPOCH FROM now() - query_start)::text || 's' as duration,
						query
					FROM pg_stat_activity
					WHERE state = 'active'
					AND query NOT ILIKE '%pg_stat_activity%'
					AND pid != pg_backend_pid()
					ORDER BY query_start DESC
				`)
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
					// Add server information to identify where the query is running
					q.Username = fmt.Sprintf("%s@%s", q.Username, server.Name)
					q.Database = fmt.Sprintf("%s@%s", q.Database, server.Name)

					qWithServer := QueryWithServer{
						Query:      q,
						Server:     server.Name,
						BackendPID: backend.PID,
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
			"Servers":        servers,
			"QueryIframeURL": config.QueryIframeURL,
		})
	})

	// Start the server
	log.Printf("Starting server on :%s", serverPort)
	if err := router.Run(":" + serverPort); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

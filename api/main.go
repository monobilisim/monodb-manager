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
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

// Config holds database connection parameters
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
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
	Users     []User
	Databases []string
	HAPorts   []HAProxyPort
	Services  []Service
	PMMURL    string
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

// Update the Config struct in loadHAProxyConfig
func loadHAProxyConfig(configPath string) ([]HAProxyPort, []Service, string, error) {
	type Config struct {
		Ports    []HAProxyPort `yaml:"ports"`
		Services []Service     `yaml:"services"`
		PMMURL   string        `yaml:"pmm_url"`
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, nil, "", err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, nil, "", err
	}

	return config.Ports, config.Services, config.PMMURL, nil
}

func InitServer() {
	// Define command line flags
	config := Config{}
	var templatesDir string
	var haproxyConfig string
	var serverPort string

	flag.StringVar(&config.Host, "host", "localhost", "PostgreSQL host")
	flag.IntVar(&config.Port, "port", 5432, "PostgreSQL port")
	flag.StringVar(&config.User, "user", "postgres", "PostgreSQL user")
	flag.StringVar(&config.Password, "password", "", "PostgreSQL password")
	flag.StringVar(&config.DBName, "dbname", "postgres", "PostgreSQL database name")
	flag.StringVar(&config.SSLMode, "sslmode", "disable", "PostgreSQL SSL mode")
	flag.StringVar(&templatesDir, "templates", "", "Path to templates directory")
	flag.StringVar(&haproxyConfig, "config", "config/haproxy.yaml", "Path to config file")
	flag.StringVar(&serverPort, "server-port", "8080", "Server port to listen on")
	flag.Parse()

	log.Println("Initializing server...")

	// Construct connection string
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host,
		config.Port,
		config.User,
		config.Password,
		config.DBName,
		config.SSLMode,
	)

	// Initialize database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Test the connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}
	log.Printf("Connected to PostgreSQL at %s:%d", config.Host, config.Port)

	router := gin.Default()

	// Load HTML templates
	templatesPath := getTemplatesDir(templatesDir)
	log.Printf("Loading templates from: %s", templatesPath)
	router.LoadHTMLGlob(templatesPath)

	// Load HAProxy config
	haPorts, services, pmmURL, err := loadHAProxyConfig(haproxyConfig)
	if err != nil {
		log.Printf("Warning: Failed to load config: %v", err)
		haPorts = []HAProxyPort{}
		services = []Service{}
		pmmURL = ""
	}

	// Add a route for the root path to redirect to /users page
	router.GET("/", func(c *gin.Context) {
		// First get all users
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
			log.Printf("Error querying users: %v", err)
			c.JSON(500, gin.H{"error": "Failed to fetch users"})
			return
		}
		defer rows.Close()

		var users []User
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
				log.Printf("Error scanning user row: %v", err)
				continue
			}

			// Get database access for this user
			if user.Superuser || user.Username == "postgres" {
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
							user.Databases = append(user.Databases, dbName)
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
                `, user.Username)

				if err == nil {
					defer dbRows.Close()
					var databases []string
					for dbRows.Next() {
						var dbName string
						if err := dbRows.Scan(&dbName); err == nil {
							databases = append(databases, dbName)
						}
					}

					// Now check each database for actual privileges
					for _, dbName := range databases {
						func() {
							dbConnStr := fmt.Sprintf(
								"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
								config.Host,
								config.Port,
								config.User,
								config.Password,
								dbName,
								config.SSLMode,
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
                            `, user.Username).Scan(&hasAccess)

							if err == nil && hasAccess {
								user.Databases = append(user.Databases, dbName)
							}
						}()
					}
				}
			}

			users = append(users, user)
		}

		// Get all databases for the create form
		dbRows, err := db.Query(`
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
            AND datname != 'postgres'
            ORDER BY datname
        `)
		if err != nil {
			log.Printf("Error querying databases: %v", err)
			c.JSON(500, gin.H{"error": "Failed to fetch databases"})
			return
		}
		defer dbRows.Close()

		var databases []string
		for dbRows.Next() {
			var dbName string
			if err := dbRows.Scan(&dbName); err != nil {
				log.Printf("Error scanning database row: %v", err)
				continue
			}
			databases = append(databases, dbName)
		}

		c.HTML(200, "users.html", PageData{
			Users:     users,
			Databases: databases,
			HAPorts:   haPorts,
			Services:  services,
			PMMURL:    pmmURL,
		})
	})

	// Group API routes under /api/v1
	v1 := router.Group("/api/v1")
	{
		v1.GET("/users", func(c *gin.Context) {
			// First get all users
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
				log.Printf("Error querying users: %v", err)
				c.JSON(500, gin.H{"error": "Failed to fetch users"})
				return
			}
			defer rows.Close()

			var users []User
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
					log.Printf("Error scanning user row: %v", err)
					continue
				}

				// Get database access for this user
				if user.Superuser || user.Username == "postgres" {
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
								user.Databases = append(user.Databases, dbName)
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
                    `, user.Username)

					if err == nil {
						defer dbRows.Close()
						var databases []string
						for dbRows.Next() {
							var dbName string
							if err := dbRows.Scan(&dbName); err == nil {
								databases = append(databases, dbName)
							}
						}

						// Now check each database for actual privileges
						for _, dbName := range databases {
							func() {
								dbConnStr := fmt.Sprintf(
									"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
									config.Host,
									config.Port,
									config.User,
									config.Password,
									dbName,
									config.SSLMode,
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
                                `, user.Username).Scan(&hasAccess)

								if err == nil && hasAccess {
									user.Databases = append(user.Databases, dbName)
								}
							}()
						}
					}
				}

				users = append(users, user)
			}

			// Get all databases for the create form
			dbRows, err := db.Query(`
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false
                AND datname != 'postgres'
                ORDER BY datname
            `)
			if err != nil {
				log.Printf("Error querying databases: %v", err)
				c.JSON(500, gin.H{"error": "Failed to fetch databases"})
				return
			}
			defer dbRows.Close()

			var databases []string
			for dbRows.Next() {
				var dbName string
				if err := dbRows.Scan(&dbName); err != nil {
					log.Printf("Error scanning database row: %v", err)
					continue
				}
				databases = append(databases, dbName)
			}

			c.JSON(200, PageData{
				Users:     users,
				Databases: databases,
				HAPorts:   haPorts,
				Services:  services,
				PMMURL:    pmmURL,
			})
		})

		v1.POST("/users", func(c *gin.Context) {
			var req CreateUserRequest
			if err := c.BindJSON(&req); err != nil {
				log.Printf("Bind error: %v", err)
				c.JSON(400, gin.H{"error": "Invalid form data"})
				return
			}

			// Check postgres database access first
			for _, dbName := range req.Databases {
				if dbName == "postgres" {
					c.JSON(400, gin.H{"error": "Cannot grant access to postgres database"})
					return
				}
			}

			// First check if user exists
			var exists bool
			err = db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", req.Username).Scan(&exists)
			if err != nil {
				log.Printf("Error checking user existence: %v", err)
				c.JSON(500, gin.H{"error": "Failed to create user"})
				return
			}
			if exists {
				c.JSON(400, gin.H{"error": "User already exists"})
				return
			}

			// Create user with proper parameter handling
			createQuery := fmt.Sprintf(
				"CREATE USER %s WITH PASSWORD '%s'",
				req.Username,
				req.Password,
			)

			if _, err = db.Exec(createQuery); err != nil {
				log.Printf("Error creating user: %v", err)
				errorMsg := "Failed to create user"
				errStr := err.Error()

				switch {
				case strings.Contains(errStr, "permission denied"):
					errorMsg = "Permission denied - insufficient privileges"
				case strings.Contains(errStr, "password authentication failed"):
					errorMsg = "Invalid password format"
				case strings.Contains(errStr, "role name contains invalid characters"):
					errorMsg = "Username contains invalid characters"
				}

				log.Printf("Detailed error: %v", err)
				c.JSON(400, gin.H{"error": errorMsg})
				return
			}

			// Grant permissions to selected databases
			for _, dbName := range req.Databases {
				// First grant connect privilege
				grantQuery := fmt.Sprintf(`GRANT CONNECT ON DATABASE "%s" TO "%s"`,
					strings.Replace(dbName, `"`, `""`, -1),
					strings.Replace(req.Username, `"`, `""`, -1),
				)
				if _, err = db.Exec(grantQuery); err != nil {
					log.Printf("Error granting CONNECT on %s: %v", dbName, err)
					continue
				}

				// Connect to the database to grant schema-level privileges
				dbConnStr := fmt.Sprintf(
					"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
					config.Host,
					config.Port,
					config.User,
					config.Password,
					dbName,
					config.SSLMode,
				)

				dbConn, err := sql.Open("postgres", dbConnStr)
				if err != nil {
					log.Printf("Error connecting to database %s: %v", dbName, err)
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
						log.Printf("Error executing schema grant query on %s: %v", dbName, err)
						// Continue with other queries even if one fails
					}
				}
			}

			c.JSON(200, gin.H{"message": "User created successfully"})
		})

		v1.DELETE("/users/:username", func(c *gin.Context) {
			username := c.Param("username")

			// Don't allow deletion of current user
			if username == config.User {
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
							config.Host,
							config.Port,
							config.User,
							config.Password,
							dbName,
							config.SSLMode,
						)

						dbConn, err := sql.Open("postgres", dbConnStr)
						if err != nil {
							log.Printf("Error connecting to database %s: %v", dbName, err)
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
								log.Printf("Error executing revoke query on %s: %v", dbName, err)
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
				log.Printf("Error deleting user %s: %v", username, err)
				c.JSON(500, gin.H{"error": "Failed to delete user"})
				return
			}

			c.JSON(200, gin.H{"message": "User deleted successfully"})
		})

		// Add this route in the router setup after the existing routes
		router.GET("/query", func(c *gin.Context) {
			rows, err := db.Query(`
				SELECT
					pid,
					usename,
					datname,
					EXTRACT(EPOCH FROM now() - query_start)::text || 's' as duration,
					query
				FROM pg_stat_activity
				WHERE state != 'idle'
				AND pid != pg_backend_pid()
				ORDER BY query_start DESC
			`)
			if err != nil {
				log.Printf("Error querying active queries: %v", err)
				c.JSON(500, gin.H{"error": "Failed to fetch queries"})
				return
			}
			defer rows.Close()

			var queries []Query
			for rows.Next() {
				var q Query
				err := rows.Scan(&q.PID, &q.Username, &q.Database, &q.Duration, &q.Query)
				if err != nil {
					log.Printf("Error scanning query row: %v", err)
					continue
				}
				queries = append(queries, q)
			}

			c.HTML(200, "query.html", gin.H{
				"Queries": queries,
			})
		})

		// Add this endpoint to the API group
		v1.DELETE("/queries/:pid", func(c *gin.Context) {
			pid := c.Param("pid")

			// First check if query exists
			var exists bool
			err = db.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", pid).Scan(&exists)
			if err != nil {
				log.Printf("Error checking query existence: %v", err)
				c.JSON(500, gin.H{"error": "Failed to check query"})
				return
			}
			if !exists {
				c.JSON(404, gin.H{"error": "Query not found"})
				return
			}

			// Kill the query
			_, err = db.Exec("SELECT pg_terminate_backend($1)", pid)
			if err != nil {
				log.Printf("Error killing query %s: %v", pid, err)
				c.JSON(500, gin.H{"error": "Failed to kill query"})
				return
			}

			c.JSON(200, gin.H{"message": "Query killed successfully"})
		})
	}

	// Add this route after the existing routes
	router.GET("/status", func(c *gin.Context) {
		c.HTML(200, "status.html", PageData{
			HAPorts:  haPorts,
			Services: services,
			PMMURL:   pmmURL,
		})
	})

	// Start the server
	log.Printf("Starting server on :%s", serverPort)
	if err := router.Run(":" + serverPort); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

package api

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

// AppUser is a panel (dashboard) login account, persisted in SQLite.
// It is distinct from PostgreSQL roles managed on the clusters.
type AppUser struct {
	ID        int64     `json:"id"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
}

// AuditEntry is a single immutable record of an action performed through the
// panel: who did what, against which target/server, from where, and whether it
// succeeded.
type AuditEntry struct {
	ID        int64     `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Actor     string    `json:"actor"`  // panel username (or "-" before login)
	Action    string    `json:"action"` // e.g. login, pg_user.create, query.kill
	Target    string    `json:"target"` // pg user, pid, app user, etc.
	Server    string    `json:"server"` // cluster name when relevant
	IP        string    `json:"ip"`
	Result    string    `json:"result"` // success | failure
	Detail    string    `json:"detail"`
}

// Audit action constants keep call sites consistent and greppable.
const (
	ActionLogin        = "login"
	ActionLoginFailed  = "login_failed"
	ActionLogout       = "logout"
	ActionPGUserCreate = "pg_user.create"
	ActionPGUserDelete = "pg_user.delete"
	ActionQueryKill    = "query.kill"
	ActionAppUserAdd   = "app_user.create"
	ActionAppUserDel   = "app_user.delete"

	ResultSuccess = "success"
	ResultFailure = "failure"
)

// Store wraps the SQLite database that backs panel authentication and the
// audit log. A single connection is sufficient for the dashboard's load and
// keeps the pure-Go driver behaviour predictable.
type Store struct {
	db *sql.DB
}

// OpenStore opens (creating if needed) the SQLite database at path and applies
// the schema. The pure-Go modernc driver is registered under the name "sqlite".
func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite store at %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite store at %s: %w", path, err)
	}

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS app_users (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	username      TEXT NOT NULL UNIQUE,
	password_hash TEXT NOT NULL,
	created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_log (
	id        INTEGER PRIMARY KEY AUTOINCREMENT,
	timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	actor     TEXT NOT NULL DEFAULT '-',
	action    TEXT NOT NULL,
	target    TEXT NOT NULL DEFAULT '',
	server    TEXT NOT NULL DEFAULT '',
	ip        TEXT NOT NULL DEFAULT '',
	result    TEXT NOT NULL DEFAULT 'success',
	detail    TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_action    ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_actor     ON audit_log(actor);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("apply sqlite schema: %w", err)
	}
	return nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// CountAppUsers returns the number of panel accounts. Used at startup to decide
// whether to bootstrap the initial admin.
func (s *Store) CountAppUsers() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM app_users`).Scan(&n)
	return n, err
}

// CreateAppUser inserts a new panel account with a bcrypt-hashed password.
func (s *Store) CreateAppUser(username, password string) error {
	username = strings.TrimSpace(username)
	if username == "" {
		return fmt.Errorf("username must not be empty")
	}
	if len(password) < 8 {
		return fmt.Errorf("password must be at least 8 characters")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	_, err = s.db.Exec(
		`INSERT INTO app_users (username, password_hash) VALUES (?, ?)`,
		username, string(hash),
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return fmt.Errorf("user %q already exists", username)
		}
		return fmt.Errorf("create app user: %w", err)
	}
	return nil
}

// DeleteAppUser removes a panel account by username.
func (s *Store) DeleteAppUser(username string) error {
	res, err := s.db.Exec(`DELETE FROM app_users WHERE username = ?`, username)
	if err != nil {
		return fmt.Errorf("delete app user: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("user %q not found", username)
	}
	return nil
}

// VerifyAppUser checks a username/password pair against the stored bcrypt hash.
// It returns true only when the user exists and the password matches.
func (s *Store) VerifyAppUser(username, password string) (bool, error) {
	var hash string
	err := s.db.QueryRow(
		`SELECT password_hash FROM app_users WHERE username = ?`, username,
	).Scan(&hash)
	if err == sql.ErrNoRows {
		// Run a dummy compare to keep timing roughly constant and avoid
		// trivially leaking whether the username exists.
		_ = bcrypt.CompareHashAndPassword(
			[]byte("$2a$10$wbnaSDR3GZ2eLZ6hP3z5/.YCDpUk6Yt7fJ6vR9k4q1m9xqf0eMlS"),
			[]byte(password),
		)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup app user: %w", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return false, nil
	}
	return true, nil
}

// ListAppUsers returns all panel accounts ordered by username.
func (s *Store) ListAppUsers() ([]AppUser, error) {
	rows, err := s.db.Query(
		`SELECT id, username, created_at FROM app_users ORDER BY username`,
	)
	if err != nil {
		return nil, fmt.Errorf("list app users: %w", err)
	}
	defer rows.Close()

	var users []AppUser
	for rows.Next() {
		var u AppUser
		if err := rows.Scan(&u.ID, &u.Username, &u.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan app user: %w", err)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// WriteAudit persists a single audit entry. Audit failures must never break the
// primary action, so callers typically log (but do not surface) the returned
// error.
func (s *Store) WriteAudit(e AuditEntry) error {
	if e.Actor == "" {
		e.Actor = "-"
	}
	if e.Result == "" {
		e.Result = ResultSuccess
	}
	_, err := s.db.Exec(
		`INSERT INTO audit_log (actor, action, target, server, ip, result, detail)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		e.Actor, e.Action, e.Target, e.Server, e.IP, e.Result, e.Detail,
	)
	if err != nil {
		return fmt.Errorf("write audit: %w", err)
	}
	return nil
}

// AuditFilter narrows the audit log query. Empty fields are ignored.
type AuditFilter struct {
	Actor  string
	Action string
	Limit  int
}

// ListAudit returns audit entries newest-first, optionally filtered by actor
// and/or action, capped at filter.Limit (default 500).
func (s *Store) ListAudit(f AuditFilter) ([]AuditEntry, error) {
	limit := f.Limit
	if limit <= 0 || limit > 5000 {
		limit = 500
	}

	var (
		conds []string
		args  []any
	)
	if f.Actor != "" {
		conds = append(conds, "actor = ?")
		args = append(args, f.Actor)
	}
	if f.Action != "" {
		conds = append(conds, "action = ?")
		args = append(args, f.Action)
	}

	query := `SELECT id, timestamp, actor, action, target, server, ip, result, detail FROM audit_log`
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list audit: %w", err)
	}
	defer rows.Close()

	var entries []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(
			&e.ID, &e.Timestamp, &e.Actor, &e.Action,
			&e.Target, &e.Server, &e.IP, &e.Result, &e.Detail,
		); err != nil {
			return nil, fmt.Errorf("scan audit row: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// DistinctAuditActions returns the set of action values present in the log, for
// populating the audit page's filter dropdown.
func (s *Store) DistinctAuditActions() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT action FROM audit_log ORDER BY action`)
	if err != nil {
		return nil, fmt.Errorf("distinct audit actions: %w", err)
	}
	defer rows.Close()

	var actions []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, fmt.Errorf("scan audit action: %w", err)
		}
		actions = append(actions, a)
	}
	return actions, rows.Err()
}

// randomPassword returns a URL-safe random password of roughly n*1.3 chars.
func randomPassword(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

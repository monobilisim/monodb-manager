package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

// Panel roles and permissions. Roles are global across every configured
// cluster; panel users are stored in monodb.panel_users on one cluster.
const (
	RoleViewer   = "viewer"
	RoleOperator = "operator"
	RoleAdmin    = "admin"

	sessionCookieName = "monodb_session"
	loginPath         = "/login"
)

type Permission string

const (
	PermView             Permission = "view"
	PermKillQuery        Permission = "kill_query"
	PermManageDBUsers    Permission = "manage_db_users"
	PermManagePanelUsers Permission = "manage_panel_users"
)

var allPermissions = []Permission{PermView, PermKillQuery, PermManageDBUsers, PermManagePanelUsers}

var rolePermissions = map[string]map[Permission]bool{
	RoleViewer: {PermView: true},
	RoleOperator: {
		PermView:          true,
		PermKillQuery:     true,
		PermManageDBUsers: true,
	},
	RoleAdmin: {
		PermView:             true,
		PermKillQuery:        true,
		PermManageDBUsers:    true,
		PermManagePanelUsers: true,
	},
}

func ValidRole(role string) bool {
	_, ok := rolePermissions[role]
	return ok
}

func RoleHasPermission(role string, perm Permission) bool {
	return rolePermissions[role][perm]
}

func RoleLabel(role string) string {
	switch role {
	case RoleViewer:
		return "İzleyici"
	case RoleOperator:
		return "Operatör"
	case RoleAdmin:
		return "Yönetici"
	default:
		return role
	}
}

var roleOrder = []string{RoleViewer, RoleOperator, RoleAdmin}

var roleSummaries = map[string]string{
	RoleViewer:   "Salt okuma",
	RoleOperator: "İzleyici + sorgu sonlandırma, DB kullanıcı/yetki yönetimi",
	RoleAdmin:    "Operatör + panel kullanıcılarını yönetme",
}

var permissionLabels = map[Permission]string{
	PermView:             "Sayfaları ve API'yi görüntüleme",
	PermKillQuery:        "Aktif sorguyu sonlandırma",
	PermManageDBUsers:    "Veritabanı kullanıcı ve yetkilerini yönetme",
	PermManagePanelUsers: "Panel kullanıcılarını ve rollerini yönetme",
}

// RoleColumn is one role column of the matrix shown on the panel users page.
type RoleColumn struct {
	Key     string
	Label   string
	Summary string
}

// RoleMatrixRow is one permission row; Granted is keyed by role.
type RoleMatrixRow struct {
	Label   string
	Granted map[string]bool
}

func RoleColumns() []RoleColumn {
	columns := make([]RoleColumn, 0, len(roleOrder))
	for _, role := range roleOrder {
		columns = append(columns, RoleColumn{Key: role, Label: RoleLabel(role), Summary: roleSummaries[role]})
	}
	return columns
}

// RoleMatrix derives the table from rolePermissions so the page cannot drift
// from the permission checks that actually run.
func RoleMatrix() []RoleMatrixRow {
	rows := make([]RoleMatrixRow, 0, len(allPermissions))
	for _, perm := range allPermissions {
		row := RoleMatrixRow{Label: permissionLabels[perm], Granted: make(map[string]bool, len(roleOrder))}
		for _, role := range roleOrder {
			row.Granted[role] = RoleHasPermission(role, perm)
		}
		rows = append(rows, row)
	}
	return rows
}

var (
	ErrUserNotFound = errors.New("panel user not found")
	ErrUserExists   = errors.New("panel user already exists")
)

// PanelUser is one login for the web UI. It is unrelated to PostgreSQL roles.
type PanelUser struct {
	Username     string
	PasswordHash string
	Role         string
	Active       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	LastLoginAt  *time.Time
}

// UserStore abstracts panel user persistence so handlers can be tested with a
// fake, while production uses PostgreSQL.
type UserStore interface {
	Init(ctx context.Context) error
	Count(ctx context.Context) (int, error)
	CountAdmins(ctx context.Context) (int, error)
	List(ctx context.Context) ([]PanelUser, error)
	Get(ctx context.Context, username string) (PanelUser, error)
	Create(ctx context.Context, user PanelUser) error
	Update(ctx context.Context, username, role string, active bool) error
	SetPassword(ctx context.Context, username, hash string) error
	Delete(ctx context.Context, username string) error
	TouchLogin(ctx context.Context, username string) error
}

type dbProvider func() (*sql.DB, error)

// pgUserStore keeps panel users in monodb.panel_users on the cluster leader.
type pgUserStore struct {
	db dbProvider
}

func NewPGUserStore(db dbProvider) *pgUserStore {
	return &pgUserStore{db: db}
}

func (s *pgUserStore) Init(ctx context.Context) error {
	db, err := s.db()
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS monodb`); err != nil {
		return fmt.Errorf("create schema monodb: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS monodb.panel_users (
			username      text PRIMARY KEY CHECK (username <> ''),
			password_hash text NOT NULL,
			role          text NOT NULL CHECK (role IN ('viewer','operator','admin')),
			active        boolean NOT NULL DEFAULT true,
			created_at    timestamptz NOT NULL DEFAULT now(),
			updated_at    timestamptz NOT NULL DEFAULT now(),
			last_login_at timestamptz
		)`); err != nil {
		return fmt.Errorf("create table monodb.panel_users: %w", err)
	}
	return nil
}

func (s *pgUserStore) Count(ctx context.Context) (int, error) {
	db, err := s.db()
	if err != nil {
		return 0, err
	}
	var count int
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM monodb.panel_users`).Scan(&count)
	return count, err
}

func (s *pgUserStore) CountAdmins(ctx context.Context) (int, error) {
	db, err := s.db()
	if err != nil {
		return 0, err
	}
	var count int
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM monodb.panel_users WHERE role = 'admin' AND active`).Scan(&count)
	return count, err
}

func (s *pgUserStore) List(ctx context.Context) ([]PanelUser, error) {
	db, err := s.db()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT username, password_hash, role, active, created_at, updated_at, last_login_at
		FROM monodb.panel_users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var users []PanelUser
	for rows.Next() {
		user, err := scanPanelUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (s *pgUserStore) Get(ctx context.Context, username string) (PanelUser, error) {
	db, err := s.db()
	if err != nil {
		return PanelUser{}, err
	}
	row := db.QueryRowContext(ctx, `
		SELECT username, password_hash, role, active, created_at, updated_at, last_login_at
		FROM monodb.panel_users WHERE username = $1`, username)
	user, err := scanPanelUser(row)
	if errors.Is(err, sql.ErrNoRows) {
		return PanelUser{}, ErrUserNotFound
	}
	if err != nil {
		return PanelUser{}, err
	}
	return user, nil
}

func (s *pgUserStore) Create(ctx context.Context, user PanelUser) error {
	db, err := s.db()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO monodb.panel_users (username, password_hash, role, active)
		VALUES ($1, $2, $3, $4)`, user.Username, user.PasswordHash, user.Role, user.Active)
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		return ErrUserExists
	}
	return err
}

func (s *pgUserStore) Update(ctx context.Context, username, role string, active bool) error {
	db, err := s.db()
	if err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `
		UPDATE monodb.panel_users SET role = $2, active = $3, updated_at = now()
		WHERE username = $1`, username, role, active)
	if err != nil {
		return err
	}
	return requireAffected(res)
}

func (s *pgUserStore) SetPassword(ctx context.Context, username, hash string) error {
	db, err := s.db()
	if err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `
		UPDATE monodb.panel_users SET password_hash = $2, updated_at = now()
		WHERE username = $1`, username, hash)
	if err != nil {
		return err
	}
	return requireAffected(res)
}

func (s *pgUserStore) Delete(ctx context.Context, username string) error {
	db, err := s.db()
	if err != nil {
		return err
	}
	res, err := db.ExecContext(ctx, `DELETE FROM monodb.panel_users WHERE username = $1`, username)
	if err != nil {
		return err
	}
	return requireAffected(res)
}

func (s *pgUserStore) TouchLogin(ctx context.Context, username string) error {
	db, err := s.db()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `UPDATE monodb.panel_users SET last_login_at = now() WHERE username = $1`, username)
	return err
}

func requireAffected(res sql.Result) error {
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrUserNotFound
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanPanelUser(row rowScanner) (PanelUser, error) {
	var user PanelUser
	var lastLogin sql.NullTime
	err := row.Scan(&user.Username, &user.PasswordHash, &user.Role, &user.Active,
		&user.CreatedAt, &user.UpdatedAt, &lastLogin)
	if err != nil {
		return PanelUser{}, err
	}
	if lastLogin.Valid {
		t := lastLogin.Time
		user.LastLoginAt = &t
	}
	return user, nil
}

// SessionManager keeps login sessions in memory. Restarting the process logs
// everyone out; that is an accepted tradeoff for a single-instance panel.
type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]Session
	ttl      time.Duration
}

type Session struct {
	Username string
	Role     string
	Expires  time.Time
}

func NewSessionManager(ttl time.Duration) *SessionManager {
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return &SessionManager{sessions: make(map[string]Session), ttl: ttl}
}

func (m *SessionManager) Create(username, role string) (string, time.Time) {
	token := randomToken()
	expires := time.Now().Add(m.ttl)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(time.Now())
	m.sessions[token] = Session{Username: username, Role: role, Expires: expires}
	return token, expires
}

func (m *SessionManager) Get(token string) (Session, bool) {
	if token == "" {
		return Session{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	session, ok := m.sessions[token]
	if !ok {
		return Session{}, false
	}
	if time.Now().After(session.Expires) {
		delete(m.sessions, token)
		return Session{}, false
	}
	return session, true
}

func (m *SessionManager) Delete(token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, token)
}

func (m *SessionManager) InvalidateUser(username string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for token, session := range m.sessions {
		if session.Username == username {
			delete(m.sessions, token)
		}
	}
}

func (m *SessionManager) TTL() time.Duration {
	return m.ttl
}

func (m *SessionManager) pruneLocked(now time.Time) {
	for token, session := range m.sessions {
		if now.After(session.Expires) {
			delete(m.sessions, token)
		}
	}
}

func randomToken() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failure is fatal for sessions; fail closed.
		panic(fmt.Errorf("generate session token: %w", err))
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

// loginLimiter throttles failed logins per client IP and username.
type loginLimiter struct {
	mu       sync.Mutex
	attempts map[string]*loginAttempt
	max      int
	window   time.Duration
}

type loginAttempt struct {
	fails int
	first time.Time
}

func newLoginLimiter(max int, window time.Duration) *loginLimiter {
	return &loginLimiter{attempts: make(map[string]*loginAttempt), max: max, window: window}
}

func (l *loginLimiter) Allowed(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	attempt, ok := l.attempts[key]
	if !ok {
		return true, 0
	}
	if now.Sub(attempt.first) >= l.window {
		delete(l.attempts, key)
		return true, 0
	}
	if attempt.fails >= l.max {
		return false, l.window - now.Sub(attempt.first)
	}
	return true, 0
}

func (l *loginLimiter) Failure(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	attempt, ok := l.attempts[key]
	if !ok || now.Sub(attempt.first) >= l.window {
		l.attempts[key] = &loginAttempt{fails: 1, first: now}
		return
	}
	attempt.fails++
}

func (l *loginLimiter) Success(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.attempts, key)
}

type AuthOptions struct {
	LegacyUsername    string
	LegacyConfigValue string
	Disabled          bool
	CookieSecure      bool
	SessionTTL        time.Duration
}

// Authenticator owns login, sessions, Basic fallback and authorization.
type Authenticator struct {
	store     UserStore
	sessions  *SessionManager
	options   AuthOptions
	limiter   *loginLimiter
	loginTmpl string
	dummyHash []byte
}

const dummyPassword = "dummy"

func NewAuthenticator(options AuthOptions, store UserStore) *Authenticator {
	dummy, err := bcrypt.GenerateFromPassword([]byte(dummyPassword), bcrypt.DefaultCost)
	if err != nil {
		dummy = []byte("$2a$10$invalidinvalidinvalidinvalidinvalidinvalidinvalidinvalidinv")
	}
	return &Authenticator{
		store:     store,
		sessions:  NewSessionManager(options.SessionTTL),
		options:   options,
		limiter:   newLoginLimiter(5, 5*time.Minute),
		loginTmpl: "login.html",
		dummyHash: dummy,
	}
}

func (a *Authenticator) StoreAvailable() bool {
	return a.store != nil
}

// UserView is the subset templates need.
type UserView struct {
	Username       string
	Role           string
	RoleLabel      string
	IsAdmin        bool
	CanKill        bool
	CanManageDB    bool
	CanManageUsers bool
	Permissions    []Permission
}

func viewFor(username, role string) UserView {
	view := UserView{
		Username:       username,
		Role:           role,
		RoleLabel:      RoleLabel(role),
		IsAdmin:        role == RoleAdmin,
		CanKill:        RoleHasPermission(role, PermKillQuery),
		CanManageDB:    RoleHasPermission(role, PermManageDBUsers),
		CanManageUsers: RoleHasPermission(role, PermManagePanelUsers),
	}
	for _, perm := range allPermissions {
		if RoleHasPermission(role, perm) {
			view.Permissions = append(view.Permissions, perm)
		}
	}
	return view
}

func (a *Authenticator) View(c *gin.Context) UserView {
	if user, ok := CurrentPanelUser(c); ok {
		return viewFor(user.Username, user.Role)
	}
	if a.options.Disabled {
		return viewFor("anonymous", RoleAdmin)
	}
	return UserView{}
}

func CurrentPanelUser(c *gin.Context) (*PanelUser, bool) {
	value, exists := c.Get("panelUser")
	if !exists {
		return nil, false
	}
	user, ok := value.(*PanelUser)
	return user, ok
}

func (a *Authenticator) setUser(c *gin.Context, username, role string) {
	c.Set("panelUser", &PanelUser{Username: username, Role: role, Active: true})
}

// Middleware authenticates cookie sessions first, then Basic credentials.
func (a *Authenticator) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if a.options.Disabled {
			a.setUser(c, "anonymous", RoleAdmin)
			c.Next()
			return
		}

		if cookie, err := c.Cookie(sessionCookieName); err == nil {
			if session, ok := a.sessions.Get(cookie); ok {
				a.setUser(c, session.Username, session.Role)
				c.Next()
				return
			}
		}

		if username, password, ok := c.Request.BasicAuth(); ok {
			if user, authenticated := a.verifyPassword(c.Request.Context(), username, password); authenticated {
				a.setUser(c, user.Username, user.Role)
				c.Next()
				return
			}
		}

		a.unauthorized(c)
		c.Abort()
	}
}

func (a *Authenticator) Require(perm Permission) gin.HandlerFunc {
	return func(c *gin.Context) {
		user, ok := CurrentPanelUser(c)
		if !ok {
			a.unauthorized(c)
			c.Abort()
			return
		}
		if !RoleHasPermission(user.Role, perm) {
			a.forbidden(c)
			c.Abort()
			return
		}
		c.Next()
	}
}

func (a *Authenticator) verifyPassword(ctx context.Context, username, password string) (PanelUser, bool) {
	// Legacy config credentials keep working as an admin (and allow emergency
	// access while the panel_users table is unavailable).
	if a.options.LegacyUsername != "" && a.options.LegacyConfigValue != "" {
		userMatch := subtle.ConstantTimeCompare([]byte(username), []byte(a.options.LegacyUsername)) == 1
		passMatch := subtle.ConstantTimeCompare([]byte(password), []byte(a.options.LegacyConfigValue)) == 1
		if userMatch && passMatch {
			return PanelUser{Username: username, Role: RoleAdmin, Active: true}, true
		}
	}

	if !a.StoreAvailable() {
		return PanelUser{}, false
	}

	user, err := a.store.Get(ctx, username)
	if err != nil {
		// Compare against a dummy hash so unknown users take similar time.
		_ = bcrypt.CompareHashAndPassword(a.dummyHash, []byte(password))
		return PanelUser{}, false
	}
	if !user.Active {
		return PanelUser{}, false
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return PanelUser{}, false
	}
	return user, true
}

func (a *Authenticator) LoginPage() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.HTML(http.StatusOK, a.loginTmpl, gin.H{
			"Next":           safeNext(c.Query("next")),
			"Error":          "",
			"ServiceUnready": !a.StoreAvailable() && a.options.LegacyUsername == "",
			"AuthDisabled":   a.options.Disabled,
		})
	}
}

func (a *Authenticator) Login() gin.HandlerFunc {
	return func(c *gin.Context) {
		if a.options.Disabled {
			c.Redirect(http.StatusFound, "/")
			return
		}

		username := strings.TrimSpace(c.PostForm("username"))
		password := c.PostForm("password")
		next := safeNext(c.PostForm("next"))
		key := c.ClientIP() + "|" + strings.ToLower(username)
		now := time.Now()

		if allowed, retry := a.limiter.Allowed(key, now); !allowed {
			c.Header("Retry-After", fmt.Sprintf("%d", int(retry.Seconds())+1))
			c.HTML(http.StatusTooManyRequests, a.loginTmpl, gin.H{
				"Next":  next,
				"Error": "Çok fazla başarısız deneme. Lütfen biraz bekleyin.",
			})
			return
		}

		user, ok := a.verifyPassword(c.Request.Context(), username, password)
		if !ok {
			a.limiter.Failure(key, now)
			audit(c, "login", username, "fail")
			c.HTML(http.StatusUnauthorized, a.loginTmpl, gin.H{
				"Next":  next,
				"Error": "Kullanıcı adı veya parola hatalı.",
			})
			return
		}

		a.limiter.Success(key)
		if a.store != nil {
			if err := a.store.TouchLogin(c.Request.Context(), user.Username); err != nil {
				log.Printf("panel user last_login update failed for %s: %v", user.Username, err)
			}
		}
		token, expires := a.sessions.Create(user.Username, user.Role)
		http.SetCookie(c.Writer, &http.Cookie{ // #nosec G124 -- Secure is configurable for plain-HTTP dev; HttpOnly/SameSite always set
			Name:     sessionCookieName,
			Value:    token,
			Path:     "/",
			HttpOnly: true,
			Secure:   a.options.CookieSecure,
			SameSite: http.SameSiteStrictMode,
			Expires:  expires,
			MaxAge:   int(time.Until(expires).Seconds()),
		})
		audit(c, "login", user.Username, "ok")
		c.Redirect(http.StatusFound, next)
	}
}

func (a *Authenticator) Logout() gin.HandlerFunc {
	return func(c *gin.Context) {
		if cookie, err := c.Cookie(sessionCookieName); err == nil {
			a.sessions.Delete(cookie)
		}
		if user, ok := CurrentPanelUser(c); ok {
			audit(c, "logout", user.Username, "ok")
		}
		a.clearCookie(c)
		c.Redirect(http.StatusFound, loginPath)
	}
}

func (a *Authenticator) clearCookie(c *gin.Context) {
	http.SetCookie(c.Writer, &http.Cookie{ // #nosec G124 -- Secure is configurable for plain-HTTP dev; HttpOnly/SameSite always set
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   a.options.CookieSecure,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
		Expires:  time.Unix(1, 0),
	})
}

func (a *Authenticator) Me() gin.HandlerFunc {
	return func(c *gin.Context) {
		view := a.View(c)
		c.JSON(http.StatusOK, gin.H{
			"username":    view.Username,
			"role":        view.Role,
			"permissions": view.Permissions,
		})
	}
}

func (a *Authenticator) unauthorized(c *gin.Context) {
	if isAPIRequest(c) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	target := c.Request.URL.RequestURI()
	c.Redirect(http.StatusFound, loginPath+"?next="+url.QueryEscape(target))
}

func (a *Authenticator) forbidden(c *gin.Context) {
	if isAPIRequest(c) {
		c.JSON(http.StatusForbidden, gin.H{"error": "insufficient permissions"})
		return
	}
	c.String(http.StatusForbidden, "Bu işlem için yetkiniz yok.")
}

func isAPIRequest(c *gin.Context) bool {
	return strings.HasPrefix(c.Request.URL.Path, "/api/") ||
		strings.Contains(c.GetHeader("Accept"), "application/json")
}

func safeNext(next string) string {
	if next == "" || !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		return "/"
	}
	return next
}

func audit(c *gin.Context, action, target, result string) {
	username := "-"
	if user, ok := CurrentPanelUser(c); ok {
		username = user.Username
	}
	log.Printf("audit action=%s user=%s target=%s result=%s ip=%s",
		action, username, target, result, c.ClientIP())
}

// SeedPanelAdmin inserts an initial admin when the table is empty.
func SeedPanelAdmin(ctx context.Context, store UserStore, username, password string) error {
	count, err := store.Count(ctx)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	if username == "" || password == "" {
		log.Print("WARNING: no panel users configured; run `monodb-manager -add-user <name> -role admin`")
		return nil
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if err := store.Create(ctx, PanelUser{
		Username:     username,
		PasswordHash: string(hash),
		Role:         RoleAdmin,
		Active:       true,
	}); err != nil && !errors.Is(err, ErrUserExists) {
		return err
	}
	log.Printf("seeded panel admin user %q from legacy auth config", username)
	return nil
}

func hashPanelPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func validUsername(username string) bool {
	if len(username) < 1 || len(username) > 64 {
		return false
	}
	for _, r := range username {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

func validPanelPassword(password string) bool {
	return len(password) >= 12
}

// UpsertPanelUser creates a user or resets an existing user's password/role.
func UpsertPanelUser(ctx context.Context, store UserStore, username, role, password string) error {
	hash, err := hashPanelPassword(password)
	if err != nil {
		return err
	}
	_, err = store.Get(ctx, username)
	if errors.Is(err, ErrUserNotFound) {
		return store.Create(ctx, PanelUser{
			Username:     username,
			PasswordHash: hash,
			Role:         role,
			Active:       true,
		})
	}
	if err != nil {
		return err
	}
	if err := store.SetPassword(ctx, username, hash); err != nil {
		return err
	}
	return store.Update(ctx, username, role, true)
}

// ChangePassword updates the signed-in user's password and drops their sessions.
func (a *Authenticator) ChangePassword() gin.HandlerFunc {
	return func(c *gin.Context) {
		user, ok := CurrentPanelUser(c)
		if !ok {
			a.unauthorized(c)
			return
		}
		var req struct {
			CurrentPassword string `json:"current_password"`
			NewPassword     string `json:"new_password"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
		if user.Username == a.options.LegacyUsername {
			c.JSON(http.StatusBadRequest, gin.H{"error": "legacy kullanıcının parolası config dosyasından değiştirilir"})
			return
		}
		if !validPanelPassword(req.NewPassword) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "yeni parola en az 12 karakter olmalı"})
			return
		}
		if !a.StoreAvailable() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "panel user store unavailable"})
			return
		}
		if _, ok := a.verifyPassword(c.Request.Context(), user.Username, req.CurrentPassword); !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "mevcut parola hatalı"})
			return
		}
		hash, err := hashPanelPassword(req.NewPassword)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "parola hashlenemedi"})
			return
		}
		if err := a.store.SetPassword(c.Request.Context(), user.Username, hash); err != nil {
			log.Printf("panel password change failed for %s: %v", user.Username, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "parola güncellenemedi"})
			return
		}
		a.sessions.InvalidateUser(user.Username)
		a.clearCookie(c)
		audit(c, "password_change", user.Username, "ok")
		c.JSON(http.StatusOK, gin.H{"message": "Parola güncellendi. Yeniden giriş yapın."})
	}
}

type panelUserResponse struct {
	Username    string     `json:"username"`
	Role        string     `json:"role"`
	Active      bool       `json:"active"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	LastLoginAt *time.Time `json:"last_login_at,omitempty"`
}

func toPanelUserResponse(user PanelUser) panelUserResponse {
	return panelUserResponse{
		Username:    user.Username,
		Role:        user.Role,
		Active:      user.Active,
		CreatedAt:   user.CreatedAt,
		UpdatedAt:   user.UpdatedAt,
		LastLoginAt: user.LastLoginAt,
	}
}

func (a *Authenticator) UsersPage() gin.HandlerFunc {
	return func(c *gin.Context) {
		users, err := a.store.List(c.Request.Context())
		if err != nil {
			log.Printf("panel user list failed: %v", err)
			c.String(http.StatusInternalServerError, "Kullanıcı listesi alınamadı.")
			return
		}
		out := make([]panelUserResponse, 0, len(users))
		for _, user := range users {
			out = append(out, toPanelUserResponse(user))
		}
		c.HTML(http.StatusOK, "settings_users.html", gin.H{
			"Users":       out,
			"Auth":        a.View(c),
			"RoleColumns": RoleColumns(),
			"RoleMatrix":  RoleMatrix(),
		})
	}
}

func (a *Authenticator) PasswordPage() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.HTML(http.StatusOK, "settings_password.html", gin.H{
			"Auth": a.View(c),
		})
	}
}

func (a *Authenticator) ListUsers() gin.HandlerFunc {
	return func(c *gin.Context) {
		users, err := a.store.List(c.Request.Context())
		if err != nil {
			log.Printf("panel user list failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "kullanıcı listesi alınamadı"})
			return
		}
		out := make([]panelUserResponse, 0, len(users))
		for _, user := range users {
			out = append(out, toPanelUserResponse(user))
		}
		c.JSON(http.StatusOK, gin.H{"users": out})
	}
}

func (a *Authenticator) CreateUser() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Role     string `json:"role"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
		req.Username = strings.TrimSpace(req.Username)
		if !validUsername(req.Username) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "kullanıcı adı 1-64 karakter; harf, rakam, . _ -"})
			return
		}
		if !ValidRole(req.Role) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "geçersiz rol"})
			return
		}
		if !validPanelPassword(req.Password) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "parola en az 12 karakter olmalı"})
			return
		}
		hash, err := hashPanelPassword(req.Password)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "parola hashlenemedi"})
			return
		}
		user := PanelUser{Username: req.Username, PasswordHash: hash, Role: req.Role, Active: true}
		if err := a.store.Create(c.Request.Context(), user); err != nil {
			if errors.Is(err, ErrUserExists) {
				c.JSON(http.StatusConflict, gin.H{"error": "kullanıcı zaten var"})
				return
			}
			log.Printf("panel user create failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "kullanıcı oluşturulamadı"})
			return
		}
		audit(c, "panel_user_create", req.Username, "ok")
		c.JSON(http.StatusCreated, gin.H{"message": "kullanıcı oluşturuldu", "username": req.Username})
	}
}

func (a *Authenticator) UpdateUser() gin.HandlerFunc {
	return func(c *gin.Context) {
		username := c.Param("username")
		var req struct {
			Role   string `json:"role"`
			Active *bool  `json:"active"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
			return
		}
		target, err := a.store.Get(c.Request.Context(), username)
		if errors.Is(err, ErrUserNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "kullanıcı bulunamadı"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "kullanıcı okunamadı"})
			return
		}
		role := req.Role
		if role == "" {
			role = target.Role
		}
		if !ValidRole(role) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "geçersiz rol"})
			return
		}
		active := target.Active
		if req.Active != nil {
			active = *req.Active
		}
		if target.Role == RoleAdmin && target.Active && (role != RoleAdmin || !active) {
			admins, err := a.store.CountAdmins(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "admin sayısı okunamadı"})
				return
			}
			if admins <= 1 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "son admin kullanıcı düşürülemez"})
				return
			}
		}
		if err := a.store.Update(c.Request.Context(), username, role, active); err != nil {
			log.Printf("panel user update failed for %s: %v", username, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "kullanıcı güncellenemedi"})
			return
		}
		a.sessions.InvalidateUser(username)
		audit(c, "panel_user_update", username, "ok")
		c.JSON(http.StatusOK, gin.H{"message": "kullanıcı güncellendi"})
	}
}

func (a *Authenticator) DeleteUser() gin.HandlerFunc {
	return func(c *gin.Context) {
		username := c.Param("username")
		if current, ok := CurrentPanelUser(c); ok && current.Username == username {
			c.JSON(http.StatusBadRequest, gin.H{"error": "kendi hesabınızı silemezsiniz"})
			return
		}
		target, err := a.store.Get(c.Request.Context(), username)
		if errors.Is(err, ErrUserNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "kullanıcı bulunamadı"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "kullanıcı okunamadı"})
			return
		}
		if target.Role == RoleAdmin && target.Active {
			admins, err := a.store.CountAdmins(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "admin sayısı okunamadı"})
				return
			}
			if admins <= 1 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "son admin silinemez"})
				return
			}
		}
		if err := a.store.Delete(c.Request.Context(), username); err != nil {
			log.Printf("panel user delete failed for %s: %v", username, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "kullanıcı silinemedi"})
			return
		}
		a.sessions.InvalidateUser(username)
		audit(c, "panel_user_delete", username, "ok")
		c.JSON(http.StatusOK, gin.H{"message": "kullanıcı silindi"})
	}
}

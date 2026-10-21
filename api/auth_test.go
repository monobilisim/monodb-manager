package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

type fakeStore struct {
	mu    sync.Mutex
	users map[string]PanelUser
}

func newFakeStore(users ...PanelUser) *fakeStore {
	store := &fakeStore{users: make(map[string]PanelUser)}
	for _, user := range users {
		store.users[user.Username] = user
	}
	return store
}

func (s *fakeStore) Init(context.Context) error { return nil }

func (s *fakeStore) Count(context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.users), nil
}

func (s *fakeStore) CountAdmins(context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, user := range s.users {
		if user.Role == RoleAdmin && user.Active {
			count++
		}
	}
	return count, nil
}

func (s *fakeStore) List(context.Context) ([]PanelUser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	users := make([]PanelUser, 0, len(s.users))
	for _, user := range s.users {
		users = append(users, user)
	}
	return users, nil
}

func (s *fakeStore) Get(_ context.Context, username string) (PanelUser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[username]
	if !ok {
		return PanelUser{}, ErrUserNotFound
	}
	return user, nil
}

func (s *fakeStore) Create(_ context.Context, user PanelUser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[user.Username]; exists {
		return ErrUserExists
	}
	if user.CreatedAt.IsZero() {
		user.CreatedAt = time.Now()
		user.UpdatedAt = user.CreatedAt
	}
	s.users[user.Username] = user
	return nil
}

func (s *fakeStore) Update(_ context.Context, username, role string, active bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[username]
	if !ok {
		return ErrUserNotFound
	}
	user.Role = role
	user.Active = active
	s.users[username] = user
	return nil
}

func (s *fakeStore) SetPassword(_ context.Context, username, hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[username]
	if !ok {
		return ErrUserNotFound
	}
	user.PasswordHash = hash
	s.users[username] = user
	return nil
}

func (s *fakeStore) Delete(_ context.Context, username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[username]; !ok {
		return ErrUserNotFound
	}
	delete(s.users, username)
	return nil
}

func (s *fakeStore) TouchLogin(context.Context, string) error { return nil }

func mustHash(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	return string(hash)
}

func testAuth(t *testing.T, store UserStore, options AuthOptions) (*Authenticator, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	auth := NewAuthenticator(options, store)

	router := gin.New()
	router.SetFuncMap(templateFuncs())
	tmpl := template.Must(template.New("").Funcs(templateFuncs()).ParseGlob("../templates/*.html"))
	router.SetHTMLTemplate(tmpl)

	router.GET("/login", auth.LoginPage())
	router.POST("/login", sameOriginOnly(), auth.Login())
	router.POST("/logout", auth.Middleware(), auth.Logout())

	app := router.Group("")
	app.Use(auth.Middleware(), sameOriginOnly())
	app.GET("/", func(c *gin.Context) { c.String(http.StatusOK, "home") })
	app.GET("/api/v1/auth/me", auth.Me())
	app.DELETE("/api/v1/auth/users/:username", auth.Require(PermManagePanelUsers), func(c *gin.Context) { c.Status(http.StatusOK) })
	app.DELETE("/api/v1/queries/:pid", auth.Require(PermKillQuery), func(c *gin.Context) { c.Status(http.StatusOK) })
	app.POST("/api/v1/users", auth.Require(PermManageDBUsers), func(c *gin.Context) { c.Status(http.StatusOK) })
	app.POST("/api/v1/auth/password", auth.ChangePassword())
	return auth, router
}

func loginRequest(router http.Handler, username, password string) *httptest.ResponseRecorder {
	form := url.Values{"username": {username}, "password": {password}}
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func sessionCookie(t *testing.T, w *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == sessionCookieName && cookie.Value != "" {
			return cookie
		}
	}
	t.Fatal("session cookie not set")
	return nil
}

func TestRolePermissions(t *testing.T) {
	tests := []struct {
		role string
		perm Permission
		want bool
	}{
		{RoleViewer, PermView, true},
		{RoleViewer, PermKillQuery, false},
		{RoleViewer, PermManageDBUsers, false},
		{RoleViewer, PermManagePanelUsers, false},
		{RoleOperator, PermKillQuery, true},
		{RoleOperator, PermManageDBUsers, true},
		{RoleOperator, PermManagePanelUsers, false},
		{RoleAdmin, PermManagePanelUsers, true},
	}
	for _, tc := range tests {
		if got := RoleHasPermission(tc.role, tc.perm); got != tc.want {
			t.Errorf("%s/%s = %v, want %v", tc.role, tc.perm, got, tc.want)
		}
	}
}

func TestSessionManagerExpiryAndInvalidate(t *testing.T) {
	manager := NewSessionManager(50 * time.Millisecond)
	token, _ := manager.Create("alice", RoleViewer)
	if _, ok := manager.Get(token); !ok {
		t.Fatal("session should be valid")
	}
	manager.InvalidateUser("alice")
	if _, ok := manager.Get(token); ok {
		t.Fatal("session should be invalidated")
	}

	token, _ = manager.Create("bob", RoleViewer)
	time.Sleep(70 * time.Millisecond)
	if _, ok := manager.Get(token); ok {
		t.Fatal("session should be expired")
	}
}

func TestLoginLogoutAndMe(t *testing.T) {
	store := newFakeStore(PanelUser{
		Username:     "viewer",
		PasswordHash: mustHash(t, "viewer-password"),
		Role:         RoleViewer,
		Active:       true,
	})
	_, router := testAuth(t, store, AuthOptions{SessionTTL: time.Hour})

	if w := loginRequest(router, "viewer", "wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong password status %d, want 401", w.Code)
	}

	w := loginRequest(router, "viewer", "viewer-password")
	if w.Code != http.StatusFound {
		t.Fatalf("login status %d, want 302", w.Code)
	}
	cookie := sessionCookie(t, w)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("me status %d, want 200", rec.Code)
	}
	var body struct {
		Username string `json:"username"`
		Role     string `json:"role"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode me: %v", err)
	}
	if body.Username != "viewer" || body.Role != RoleViewer {
		t.Fatalf("me = %+v", body)
	}

	req = httptest.NewRequest(http.MethodPost, "/logout", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("logout status %d, want 302", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	req.AddCookie(cookie)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("me after logout status %d, want 401", rec.Code)
	}
}

func TestBasicAuthFallbackAndPermissions(t *testing.T) {
	store := newFakeStore(PanelUser{
		Username:     "viewer",
		PasswordHash: mustHash(t, "viewer-password"),
		Role:         RoleViewer,
		Active:       true,
	})
	_, router := testAuth(t, store, AuthOptions{
		LegacyUsername:    "ops",
		LegacyConfigValue: "legacy-secret-value",
		SessionTTL:        time.Hour,
	})

	request := func(method, path, user, pass string) int {
		req := httptest.NewRequest(method, path, nil)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w.Code
	}

	if code := request(http.MethodGet, "/api/v1/auth/me", "ops", "legacy-secret-value"); code != http.StatusOK {
		t.Fatalf("legacy basic status %d, want 200", code)
	}
	if code := request(http.MethodGet, "/api/v1/auth/me", "viewer", "viewer-password"); code != http.StatusOK {
		t.Fatalf("store basic status %d, want 200", code)
	}
	if code := request(http.MethodGet, "/api/v1/auth/me", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("anonymous status %d, want 401", code)
	}
	if code := request(http.MethodDelete, "/api/v1/queries/1", "viewer", "viewer-password"); code != http.StatusForbidden {
		t.Fatalf("viewer kill status %d, want 403", code)
	}
	if code := request(http.MethodPost, "/api/v1/users", "viewer", "viewer-password"); code != http.StatusForbidden {
		t.Fatalf("viewer db user status %d, want 403", code)
	}
	if code := request(http.MethodPost, "/api/v1/users", "ops", "legacy-secret-value"); code != http.StatusOK {
		t.Fatalf("legacy admin db user status %d, want 200", code)
	}
}

func TestLoginRateLimit(t *testing.T) {
	store := newFakeStore(PanelUser{
		Username:     "viewer",
		PasswordHash: mustHash(t, "viewer-password"),
		Role:         RoleViewer,
		Active:       true,
	})
	_, router := testAuth(t, store, AuthOptions{SessionTTL: time.Hour})

	for i := range 5 {
		if w := loginRequest(router, "viewer", "wrong"); w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status %d, want 401", i+1, w.Code)
		}
	}
	if w := loginRequest(router, "viewer", "wrong"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("rate limit status %d, want 429", w.Code)
	}
}

func TestSeedPanelAdmin(t *testing.T) {
	store := newFakeStore()
	if err := SeedPanelAdmin(context.Background(), store, "admin", "legacy-password"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	user, err := store.Get(context.Background(), "admin")
	if err != nil {
		t.Fatalf("get seeded admin: %v", err)
	}
	if user.Role != RoleAdmin || !user.Active {
		t.Fatalf("seeded user = %+v", user)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte("legacy-password")); err != nil {
		t.Fatalf("seeded hash does not verify: %v", err)
	}

	if err := SeedPanelAdmin(context.Background(), store, "other", "another-password"); err != nil {
		t.Fatalf("second seed: %v", err)
	}
	if count, _ := store.Count(context.Background()); count != 1 {
		t.Fatalf("seed must be idempotent, got %d users", count)
	}
}

func TestChangePassword(t *testing.T) {
	store := newFakeStore(PanelUser{
		Username:     "viewer",
		PasswordHash: mustHash(t, "old-password-value"),
		Role:         RoleViewer,
		Active:       true,
	})
	auth, router := testAuth(t, store, AuthOptions{SessionTTL: time.Hour})
	token, _ := auth.sessions.Create("viewer", RoleViewer)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/password",
		strings.NewReader(`{"current_password":"old-password-value","new_password":"new-password-value"}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("password change status %d, want 200: %s", w.Code, w.Body.String())
	}
	if _, ok := auth.sessions.Get(token); ok {
		t.Fatal("session should be invalidated after password change")
	}
	user, _ := store.Get(context.Background(), "viewer")
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte("new-password-value")); err != nil {
		t.Fatalf("new password does not verify: %v", err)
	}
}

func TestPGUserStoreIntegration(t *testing.T) {
	dsn := os.Getenv("MONODB_TEST_DSN")
	if dsn == "" {
		t.Skip("MONODB_TEST_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	store := NewPGUserStore(func() (*sql.DB, error) { return db, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}

	username := fmt.Sprintf("test_%d", time.Now().UnixNano())
	hash := mustHash(t, "integration-password")
	if err := store.Create(ctx, PanelUser{Username: username, PasswordHash: hash, Role: RoleViewer, Active: true}); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), username) })

	user, err := store.Get(ctx, username)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if user.Role != RoleViewer {
		t.Fatalf("role = %s", user.Role)
	}
	if err := store.SetPassword(ctx, username, mustHash(t, "integration-password-2")); err != nil {
		t.Fatalf("set password: %v", err)
	}
	if err := store.Update(ctx, username, RoleOperator, true); err != nil {
		t.Fatalf("update: %v", err)
	}
	user, _ = store.Get(ctx, username)
	if user.Role != RoleOperator {
		t.Fatalf("updated role = %s", user.Role)
	}
}

func TestUpsertPanelUser(t *testing.T) {
	store := newFakeStore()
	ctx := context.Background()
	if err := UpsertPanelUser(ctx, store, "alice", RoleViewer, "alice-password-123"); err != nil {
		t.Fatalf("create: %v", err)
	}
	user, _ := store.Get(ctx, "alice")
	if user.Role != RoleViewer || !user.Active {
		t.Fatalf("user = %+v", user)
	}
	if err := UpsertPanelUser(ctx, store, "alice", RoleOperator, "alice-password-456"); err != nil {
		t.Fatalf("update: %v", err)
	}
	user, _ = store.Get(ctx, "alice")
	if user.Role != RoleOperator {
		t.Fatalf("role = %s", user.Role)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte("alice-password-456")); err != nil {
		t.Fatalf("new password does not verify: %v", err)
	}
}

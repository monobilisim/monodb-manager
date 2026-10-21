package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"text/template"

	"github.com/gin-gonic/gin"
)

func TestReplaceBadgeWithDashboard(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"https://uptime.example.com/api/badge/14/status?style=for-the-badge", "https://uptime.example.com/dashboard/14"},
		{"https://uptime.example.com/api/badge/14/status", "https://uptime.example.com/dashboard/14"},
		{"https://uptime.example.com/api/badge/14", "https://uptime.example.com/dashboard/14"},
	}
	for _, tc := range tests {
		if got := replaceBadgeWithDashboard(tc.in); got != tc.want {
			t.Errorf("replaceBadgeWithDashboard(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestPMMEmbedURL(t *testing.T) {
	got := pmmEmbedURL("https://pmm.example.com/graph/d/abc?refresh=2s&kiosk=tv")
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("unparseable URL %q: %v", got, err)
	}
	q := u.Query()
	if q.Get("kiosk") != "1" {
		t.Errorf("kiosk = %q, want 1", q.Get("kiosk"))
	}
	if q.Get("refresh") != "2s" {
		t.Errorf("refresh = %q, want 2s", q.Get("refresh"))
	}
	if pmmEmbedURL("") != "" {
		t.Error("empty input must stay empty")
	}

	opened := pmmOpenURL(got)
	if strings.Contains(opened, "kiosk") {
		t.Errorf("pmmOpenURL left kiosk in %q", opened)
	}
}

func TestConnectionString(t *testing.T) {
	cfg := PostgreSQLServer{
		User:     "monitor",
		Password: `p a"ss/wörd`,
		SSLMode:  "require",
	}
	dsn := connectionString(cfg, "2001:db8::1", 5432, "app db")
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("unparseable DSN %q: %v", dsn, err)
	}
	if u.Host != "[2001:db8::1]:5432" {
		t.Errorf("host = %q", u.Host)
	}
	if u.Path != "/app db" {
		t.Errorf("path = %q", u.Path)
	}
	if got, _ := u.User.Password(); got != cfg.Password {
		t.Errorf("password = %q, want %q", got, cfg.Password)
	}
	if u.Query().Get("sslmode") != "require" {
		t.Errorf("sslmode missing from %q", dsn)
	}
}

func TestSelectedDatabases(t *testing.T) {
	got, err := selectedDatabases([]string{"app@prod", "reporting@prod", "app@uat"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got["prod"]) != 2 || len(got["uat"]) != 1 {
		t.Errorf("unexpected grouping: %#v", got)
	}

	if _, err := selectedDatabases([]string{"postgres@prod"}); err == nil {
		t.Error("postgres database must be rejected")
	}
	if _, err := selectedDatabases([]string{"no-server-suffix"}); err == nil {
		t.Error("missing server suffix must be rejected")
	}
}

func TestSplitCompound(t *testing.T) {
	name, server, ok := splitCompound("ops@corp@prod")
	if !ok || name != "ops@corp" || server != "prod" {
		t.Errorf("splitCompound = %q, %q, %v", name, server, ok)
	}
	if _, _, ok := splitCompound("@prod"); ok {
		t.Error("empty name must not parse")
	}
}

func TestSameOriginOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(sameOriginOnly())
	router.POST("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	request := func(origin string) int {
		req := httptest.NewRequest(http.MethodPost, "http://panel.example.com/x", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w.Code
	}

	if code := request(""); code != http.StatusOK {
		t.Errorf("no Origin: status %d, want 200", code)
	}
	if code := request("http://panel.example.com"); code != http.StatusOK {
		t.Errorf("same Origin: status %d, want 200", code)
	}
	if code := request("http://evil.example.com"); code != http.StatusForbidden {
		t.Errorf("cross Origin: status %d, want 403", code)
	}
}

func TestTemplatesParse(t *testing.T) {
	tmpl, err := template.New("").Funcs(templateFuncs()).ParseGlob("../templates/*.html")
	if err != nil {
		t.Fatalf("template parse failed: %v", err)
	}
	for _, name := range []string{"status.html", "topology.html", "users.html", "query.html", "query_analytics.html", "login.html", "settings_users.html", "settings_password.html"} {
		if tmpl.Lookup(name) == nil {
			t.Errorf("template %q is missing", name)
		}
	}
}

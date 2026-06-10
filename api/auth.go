package api

import (
	"crypto/rand"
	"log"
	"net/http"
	"strings"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
)

const (
	sessionName    = "monodb_session"
	sessionAuthKey = "authenticated"
	sessionUserKey = "username"
	sessionMaxAge  = 86400 // 24h
)

// authService bundles the SQLite store with the panel's auth behaviour so
// handlers can reach both the credential checks and the audit log.
type authService struct {
	store *Store
}

// newSessionStore builds a cookie-backed session store. The secret is used to
// authenticate and encrypt the session cookie; when none is configured a random
// per-process secret is generated (sessions then reset on restart).
func newSessionStore(secret string) cookie.Store {
	key := []byte(secret)
	if len(key) < 32 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			log.Fatalf("auth: failed to generate session secret: %v", err)
		}
		log.Println("auth: no session_secret configured; generated an ephemeral one (sessions reset on restart)")
	}
	store := cookie.NewStore(key)
	store.Options(sessions.Options{
		Path:     "/",
		MaxAge:   sessionMaxAge,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	return store
}

// bootstrapAdmin ensures at least one panel account exists. On an empty store it
// creates an "admin" user with a freshly generated random password and prints
// the credentials to the log exactly once.
func bootstrapAdmin(store *Store) {
	n, err := store.CountAppUsers()
	if err != nil {
		log.Fatalf("auth: failed to count panel users: %v", err)
	}
	if n > 0 {
		return
	}

	pw, err := randomPassword(18)
	if err != nil {
		log.Fatalf("auth: failed to generate admin password: %v", err)
	}
	if err := store.CreateAppUser("admin", pw); err != nil {
		log.Fatalf("auth: failed to create bootstrap admin: %v", err)
	}

	log.Println("========================================================")
	log.Println(" monodb-manager: created initial panel admin account")
	log.Println("   username: admin")
	log.Printf("   password: %s", pw)
	log.Println(" Store this password now - it is shown only once.")
	log.Println("========================================================")
}

// currentActor returns the logged-in panel username for the request, or "-" when
// none is present (e.g. on the login endpoint itself).
func currentActor(c *gin.Context) string {
	if u, ok := c.Get(sessionUserKey); ok {
		if s, ok := u.(string); ok && s != "" {
			return s
		}
	}
	session := sessions.Default(c)
	if v := session.Get(sessionUserKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "-"
}

// AuthRequired protects UI and API routes. Unauthenticated browser requests are
// redirected to /login; unauthenticated API/AJAX requests get a 401 JSON body.
func (a *authService) AuthRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		authed, _ := session.Get(sessionAuthKey).(bool)
		if !authed {
			if wantsJSON(c) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Yetkisiz - lütfen giriş yapın"})
				return
			}
			c.Redirect(http.StatusFound, "/login")
			c.Abort()
			return
		}
		// Expose the username to downstream handlers (audit actor).
		if v := session.Get(sessionUserKey); v != nil {
			c.Set(sessionUserKey, v)
		}
		c.Next()
	}
}

// wantsJSON reports whether the request expects a JSON response rather than an
// HTML redirect (API group, XHR, or explicit Accept header).
func wantsJSON(c *gin.Context) bool {
	if strings.HasPrefix(c.Request.URL.Path, "/api/") {
		return true
	}
	if c.GetHeader("X-Requested-With") == "XMLHttpRequest" {
		return true
	}
	return strings.Contains(c.GetHeader("Accept"), "application/json")
}

// loginPage renders the login form. If already authenticated it redirects home.
func (a *authService) loginPage(c *gin.Context) {
	session := sessions.Default(c)
	if authed, _ := session.Get(sessionAuthKey).(bool); authed {
		c.Redirect(http.StatusFound, "/")
		return
	}
	c.HTML(http.StatusOK, "login.html", gin.H{
		"Error": c.Query("error"),
	})
}

// login validates credentials, establishes a session, and writes an audit entry
// for both success and failure.
func (a *authService) login(c *gin.Context) {
	username := strings.TrimSpace(c.PostForm("username"))
	password := c.PostForm("password")
	ip := c.ClientIP()

	ok, err := a.store.VerifyAppUser(username, password)
	if err != nil {
		log.Printf("auth: verify error for %q: %v", username, err)
		c.HTML(http.StatusInternalServerError, "login.html", gin.H{
			"Error": "Sunucu hatası, tekrar deneyin",
		})
		return
	}
	if !ok {
		_ = a.store.WriteAudit(AuditEntry{
			Actor: orDash(username), Action: ActionLoginFailed,
			Target: username, IP: ip, Result: ResultFailure,
			Detail: "geçersiz kullanıcı adı veya parola",
		})
		c.HTML(http.StatusUnauthorized, "login.html", gin.H{
			"Error": "Geçersiz kullanıcı adı veya parola",
		})
		return
	}

	session := sessions.Default(c)
	session.Set(sessionAuthKey, true)
	session.Set(sessionUserKey, username)
	if err := session.Save(); err != nil {
		log.Printf("auth: failed to save session for %q: %v", username, err)
		c.HTML(http.StatusInternalServerError, "login.html", gin.H{
			"Error": "Oturum oluşturulamadı",
		})
		return
	}

	_ = a.store.WriteAudit(AuditEntry{
		Actor: username, Action: ActionLogin, IP: ip, Result: ResultSuccess,
	})
	c.Redirect(http.StatusFound, "/")
}

// logout clears the session and records the event.
func (a *authService) logout(c *gin.Context) {
	actor := currentActor(c)
	session := sessions.Default(c)
	session.Clear()
	session.Options(sessions.Options{Path: "/", MaxAge: -1})
	_ = session.Save()

	_ = a.store.WriteAudit(AuditEntry{
		Actor: actor, Action: ActionLogout, IP: c.ClientIP(), Result: ResultSuccess,
	})
	c.Redirect(http.StatusFound, "/login")
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

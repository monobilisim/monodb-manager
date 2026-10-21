# Auth + rol mekanizması (monodb-manager)

> Branch: **`feat/auth-roles`** (mevcut commit edilmemiş sertleştirme WIP'i taşınarak).
> Kararlar: roller `viewer / operator / admin` (operator = sorgu kill + DB kullanıcı/yetki yönetimi),
> kullanıcı deposu **PostgreSQL tablosu**, login sayfası + cookie, Basic fallback, legacy config admin seed, global roller.

## Context

Panel şu an tek kullanıcılı HTTP Basic Auth (`AuthConfig`, `basicAuth`) ile korunuyor; parola config'te düz metin,
rol/izin yok, tüm route'lar aynı yetkide. Yedas'ta `admin.yedas.srv.biz.tr` bu şekilde canlı.
Hedef: çoklu kullanıcı, bcrypt hash, oturum cookie'si, route bazlı roller ve panel kullanıcı yönetimi.

## Approach

### Kimlik doğrulama
- **Login sayfası** (`GET /login`, `POST /login`) + sunucu tarafı oturum.
  - Rastgele 32 byte token, `HttpOnly`, `SameSite=Strict`, `Secure` (varsayılan true), `Path=/`, TTL 12h.
  - Oturumlar bellek içi `map[token]Session` + periyodik temizlik; kullanıcı güncellenince/silinince
    o kullanıcının oturumları düşürülür. Tek instance olduğu için dağıtık store gerekmez.
- **Basic Auth fallback** korunur (curl/otomasyon): her istekte kullanıcı tablosundan bcrypt doğrulaması;
  legacy config kullanıcısı admin olarak kabul edilir. Basic isteklerde oturum cookie'si aranmaz.
- **Login rate-limit:** bellek içi, `ip|username` başına 5 başarısız deneme / 5 dk; sabit gecikme.
- **CSRF:** mevcut `sameOriginOnly` mutasyonlarda kalır; cookie `SameSite=Strict`. Login/logout POST.

### Kullanıcı deposu (PostgreSQL)
- Config'te seçilen sunucunun **leader** bağlantısı (`auth.server`, boşsa `servers[0]`) üzerinden:
```sql
CREATE SCHEMA IF NOT EXISTS monodb;
CREATE TABLE IF NOT EXISTS monodb.panel_users (
  username      text PRIMARY KEY CHECK (username <> ''),
  password_hash text NOT NULL,
  role          text NOT NULL CHECK (role IN ('viewer','operator','admin')),
  active        boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  last_login_at timestamptz
);
```
- Startup'ta şema/tablo `IF NOT EXISTS` ile oluşturulur. DB erişilemezse: legacy Basic varsa degraded mod
  (login 503, Basic admin çalışır), yoksa `log.Fatal` (fail-closed).
- İlk açılışta tablo boşsa legacy `auth.username/password` → bcrypt ile admin olarak seed edilir.
  Ek olarak `MONODB_ADMIN_PASSWORD` env desteği (kullanıcı adı `admin`) opsiyonel.
- CLI kurtarma: `monodb-manager -add-user <username> -role <role>` parolayı stdin'den okur (DB'ye yazar).

### Roller ve izinler (global)
| İzin | viewer | operator | admin |
|---|---|---|---|
| Sayfalar/API görüntüleme (status, topology, users listesi, queries, analytics) | ✓ | ✓ | ✓ |
| Sorgu sonlandırma (`DELETE /api/v1/queries/:pid`) | – | ✓ | ✓ |
| DB kullanıcı oluştur/sil/yetki (`POST/DELETE /api/v1/users`) | – | ✓ | ✓ |
| Panel kullanıcı yönetimi (`/api/v1/auth/users`, `/settings/users`) | – | – | ✓ |
| Kendi parolasını değiştirme (`POST /api/v1/auth/password`) | ✓ | ✓ | ✓ |

- Route→izin eşlemesi `api/auth.go` içinde tek tablo; middleware `requirePerm`.
- HTML isteklerinde yetkisiz → `/login` veya 403 sayfası; API'de JSON 401/403.
- UI: yetkisiz butonlar gizlenir (query kill, kullanıcı oluştur/sil, ayarlar linki), sidebar'da
  kullanıcı/rol + çıkış.

### Uyumluluk
- `auth.username/password` korunur: seed + Basic fallback (admin). Config'te plaintext kalır; README'de
  deprecated notu. Yedas config'i değişmeden çalışır.
- `auth.disabled: true` yalnız açıkça istenirse kimliksiz mod (mevcut davranış); varsayılan kapalı.
- `auth.server`, `auth.session_ttl`, `auth.cookie_secure` opsiyonel; makul varsayılanlar.

## Varsayımlar (plan onayı ile uygulanacak)

- Kullanıcı tablosu, izlenen kümenin leader'ında `monodb.panel_users`; hangi sunucu olduğu `auth.server`
  (boşsa `servers[0]`). Yedas'ta `calik-yedas-patroni`.
- Oturum TTL 12 saat, cookie `Secure` varsayılan true (Caddy TLS); HTTP dev için `cookie_secure: false`.
- Kendi parolasını değiştirme tüm rollere açık; panel kullanıcı CRUD sadece admin.
- Kimliksiz mod yalnız `auth.disabled: true` ile; varsayılan kapalı.
- Branch, plan onayından sonra `git switch -c feat/auth-roles` ile açılır; mevcut WIP taşınır, commit'ler ayrı atılır.

## Files to modify

- `api/auth.go` (yeni) — bcrypt, PG store, session manager, permission matrisi, middleware, login/logout, user CRUD
- `api/main.go` — AuthConfig genişletme, startup wiring, route'lara middleware, page data'ya kullanıcı/izinler,
  CLI `-add-user`, audit log satırları
- `api/main_test.go` / `api/auth_test.go` — store fake, permission matrisi, login/session/CSRF testleri
- `templates/login.html` (yeni), `templates/settings_users.html` (yeni), `templates/settings_password.html` (yeni)
- `templates/_sidebar.html` — kullanıcı/rol/çıkış, admin ayarlar linki
- `templates/query.html`, `templates/users.html`, `templates/status.html`, `templates/topology.html` — yetkiye göre aksiyon
- `README.md` — auth/rol yapılandırması, migration, CLI, güvenlik notları

## Reuse

- `basicAuth`, `sameOriginOnly`, `securityHeaders`, `templateFuncs`, `getTemplatesDir` (`api/main.go`)
- `ConnectionManager.GetConnection` / `GetServerConfig` — store için leader bağlantısı
- `golang.org/x/crypto/bcrypt` (v0.58 zaten modülde; direct require)
- Mevcut Tailwind/Manrope tasarımı, `_sidebar.html`, `static/app.css`
- `log.Printf` audit deseni; ek log altyapısı yok

## Steps

- [x] `git switch -c feat/auth-roles` (mevcut WIP taşınır; commit'ler onay sonrası)
- [x] `api/auth.go`: bcrypt hash/verify, `UserStore` arayüzü + PG implementasyonu, session manager, izin matrisi
- [x] `AuthConfig` genişlet + startup'ta store init/seed/legacy fallback; `-add-user` CLI
- [x] Login/logout/me/password endpoint'leri + login template; rate-limit
- [x] Route middleware: view / kill / db_manage / panel_admin; 401/403 davranışı
- [x] Panel kullanıcı CRUD API + `/settings/users` sayfası (admin), `/settings/password` (tümü)
- [x] Şablonlarda izin bazlı gizleme; sidebar kullanıcı/rol/çıkış; audit log
- [x] Testler: bcrypt, fake store ile login/session/expiry/invalidate, permission matrisi, 401/403, legacy seed,
  template parse; PG store için env guard'lı opsiyonel entegrasyon testi
- [x] README + `go vet`, `go test`, statik build, lokal canlı doğrulama

## Verification

- `go test ./...`, `go vet ./...`, `CGO_ENABLED=0 go build`
- Lokal: geçici config + test DB → ilk açılışta admin seed; login cookie; viewer `DELETE /queries/:pid` → 403;
  operator aynı istek → 200; operator `POST /api/v1/users` → 200; viewer → 403; admin `/api/v1/auth/users` → 200;
  logout sonrası 401; yanlış parola 5 kez → 429.
- Tarayıcı: login, sidebar kullanıcı/rol, viewer'da aksiyonların gizli olması, admin kullanıcı CRUD,
  parola değiştirme, çıkış.
- Yedas: mevcut `admin.yedas` basic auth admin olarak çalışmaya devam eder; login sayfası ve roller canlıda doğrulanır.

## Risks / notlar

- Panel kullanıcıları PG'de: DB tamamen düşerse login çalışmaz; legacy Basic admin acil erişim için kalır.
- Basic fallback her istekte bcrypt maliyeti getirir (otomasyon için kabul edilebilir; cache ileride).
- `monodb_admin` superuser; panel kullanıcıları ile DB rolleri karıştırılmamalı (`monodb.panel_users` ayrı şema).
- Oturumlar bellek içi: restart'ta herkes yeniden giriş yapar (kabul edilen tradeoff).

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![GPL License][license-shield]][license-url]

[![Readme in English](https://img.shields.io/badge/Readme-English-blue)](README.md)

<div align="center">
<a href="https://mono.net.tr/">
  <img src="https://r2.mono.tr/logo/Mono-Logo.svg" width="340"/>
</a>

<h2 align="center">monodb-manager</h2>
<b>monodb-manager</b> is a lightweight web dashboard for monitoring and managing
Patroni-managed PostgreSQL clusters. It auto-discovers cluster topology through
the Patroni REST API, surfaces live replication and node health, lets you manage
database users and grants across multiple clusters, and embeds Percona PMM
metrics — all from a single, dependency-free Go binary.
</div>

---

## Table of Contents

- [Table of Contents](#table-of-contents)
- [Features](#features)
- [Screens](#screens)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API](#api)
- [Building](#building)
- [License](#license)

---

## Features

- **Patroni cluster discovery**
  - Auto-discovers cluster members, leader and replicas through the Patroni REST API.
  - Routes write operations to the current leader and refreshes topology continuously.
  - Multi-cluster aware: manage several Patroni clusters from one instance.

- **Live topology view**
  - Shows leader/replica roles, member state, timeline and replication lag.
  - Enriches Patroni data with `pg_stat_replication` (sync state, lag in bytes/seconds).

- **User & grant management**
  - List, create and grant database access to PostgreSQL users.
  - Cross-cluster operations using `dbname@server` compound naming.
  - Configurable ignore-list to hide system/internal users from views.

- **Active query monitoring**
  - Inspect running queries per cluster with PID, user, database and duration.

- **Status dashboard**
  - HAProxy port health badges.
  - Service node availability checks.
  - Embedded Percona PMM status and Query Analytics (QAN) iframes.

- **Single static binary**
  - Written in Go, ships with embedded HTML templates, styles and fonts, no external runtime or CDN access needed.
  - Optional HTTP basic authentication (`auth` block) for deployments exposed beyond a trusted network.

---

## Screens

| Route               | Description                                              |
| ------------------- | -------------------------------------------------------- |
| `/`                 | Status dashboard (HAProxy ports, services, PMM)          |
| `/topology`         | Patroni cluster topology and replication health          |
| `/users`            | User and database grant management                       |
| `/query`            | Active PostgreSQL queries                                |
| `/query-analytics`  | Embedded PMM Query Analytics                             |
| `/dashboards`       | Configured PMM dashboards, embedded and tabbed           |

---

## Installation

### From a release

Pre-built, self-contained binaries (templates are embedded) are published on the
[Releases](https://github.com/monobilisim/monodb-manager/releases) page for
Linux and macOS (amd64 / arm64):

```bash
# Pick the asset matching your OS/arch, e.g. linux x86_64
VERSION=vX.Y.Z
curl -fsSL -o monodb-manager.tar.gz \
  https://github.com/monobilisim/monodb-manager/releases/download/${VERSION}/monodb-manager_linux_x86_64.tar.gz
tar xzf monodb-manager.tar.gz
sudo install -m 0755 monodb-manager /usr/local/bin/monodb-manager
```

### From source

```bash
go install github.com/monobilisim/monodb-manager@latest
```

See [Building](#building) to compile a local binary.

---

## Configuration

monodb-manager runs in multi-server mode and requires a YAML config file passed
via the `-config` flag. A minimal example:

```yaml
# Panel authentication. Users are stored in monodb.panel_users on auth.server
# (default: first configured server). The legacy username/password below seeds
# the first admin and keeps HTTP Basic Auth available for automation; remove it
# after you have created named users. Terminate TLS in front of the app.
auth:
  username: monodb
  password: "replace-with-a-strong-password"
  server: prod-patroni    # optional, defaults to servers[0]
  session_ttl: 12h        # optional
  cookie_secure: true     # optional; set false only for plain-HTTP dev
  # disabled: true        # explicit opt-out of authentication (not recommended)

# List of Patroni-managed PostgreSQL clusters
servers:
  - name: prod-patroni
    user: postgres
    password: change-me-before-deploy
    dbname: postgres
    sslmode: disable
    patroni_nodes:
      - "10.0.0.1"
      - "10.0.0.2"
      - "10.0.0.3"
    patroni_port: 8008      # Patroni REST API port (default 8008)
    connect_to_leader: true # only connect to the leader

# Users hidden from the active-query view
ignore_users:
  - postgres
  - replicator

# Status-page HAProxy port badges
ports:
  - port: 5432
    type: tcp
    status: "Read/Write"
  - port: 5433
    type: tcp
    status: "Read Only"

# Status-page service availability checks
services:
  - name: PMM Server
    nodes:
      - url: "https://pmm.example.com"

# Percona PMM embeds
pmm_status_url: "https://pmm.example.com/graph/d/.../status"
pmm_qan_url: "https://pmm.example.com/graph/d/.../qan"

# Dashboards embedded under /dashboards, one visible at a time (tabs).
# `name` is the tab label and the ?d= selector, `url` is the Grafana dashboard.
pmm_dashboards:
  - name: PostgreSQL Overview
    url: "https://pmm.example.com/graph/d/postgresql-instance-overview/postgresql-instance-overview?var-environment=prod"
  - name: Node Summary
    url: "https://pmm.example.com/graph/d/node-instance-summary/node-instance-summary"

# UI badge refresh interval in milliseconds (default 3000)
badge_refresh_interval: 3000

# Optional log file path
log_file: "/var/log/monodb-manager.log"
```

### Users and roles

Panel users are stored in `monodb.panel_users` on the leader of `auth.server`.
Passwords are bcrypt hashes; sessions are `HttpOnly` cookies (12h default).
Roles are global across all configured clusters:

| Role | Permissions |
| --- | --- |
| `viewer` | Read-only pages and API (status, topology, users list, active queries, analytics) |
| `operator` | viewer + terminate queries + create/drop database users and grants |
| `admin` | operator + manage panel users and settings |

On first start, the legacy `auth.username` / `auth.password` pair is inserted as
an `admin` user and remains usable through HTTP Basic Auth (automation and
emergency access). Create named users from **Panel Kullanıcıları** or the CLI:

```bash
read -rsp 'New password: ' PMM_PW; echo
printf '%s\n' "$PMM_PW" | monodb-manager -config /etc/mono/monodb-manager.yaml \
  -add-user alice -role operator
```

The password must be at least 12 characters. Changing the legacy user's password
is done in the config file; named users can change their own password under
**Parola**.

---

## Usage

Run the server, pointing it at your config file:

```bash
monodb-manager -config /etc/mono/monodb-manager.yaml -server-port 8080
```

Flags:

| Flag           | Default      | Description                            |
| -------------- | ------------ | -------------------------------------- |
| `-config`      | _(required)_ | Path to the YAML configuration file    |
| `-server-port` | `8080`       | HTTP port to listen on                 |
| `-templates`   | _(auto)_     | Override the HTML templates directory  |
| `-add-user`    |              | Create/update a panel user and exit (password from stdin) |
| `-role`        | `admin`      | Role for `-add-user`: `viewer`, `operator`, `admin` |

Then open `http://localhost:8080` and sign in at `/login`.

> The config file path is mandatory; single-server mode is not supported.

---

## API

All JSON endpoints are grouped under `/api/v1`. Authentication is a session
cookie (browser) or HTTP Basic (automation); authorization depends on the role.

| Method | Endpoint              | Description                                  |
| ------ | --------------------- | -------------------------------------------- |
| `GET`  | `/healthz`            | Liveness probe (always unauthenticated)      |
| `GET`  | `/login`, `POST /login` | Login page / credentials                   |
| `POST` | `/logout`             | End the current session                      |
| `GET`  | `/api/v1/auth/me`     | Current user, role and permissions           |
| `POST` | `/api/v1/auth/password` | Change own password                        |
| `GET`  | `/api/v1/auth/users`  | List panel users (admin)                     |
| `POST` | `/api/v1/auth/users`  | Create a panel user (admin)                  |
| `PUT`  | `/api/v1/auth/users/:username` | Update role/active (admin)          |
| `DELETE` | `/api/v1/auth/users/:username` | Delete panel user (admin)         |
| `GET`  | `/api/v1/servers`     | List configured Patroni clusters (no secrets)|
| `GET`  | `/api/v1/status`      | Status-page data (ports, services, PMM URLs) |
| `GET`  | `/api/v1/topology`    | Patroni topology + replication detail        |
| `GET`  | `/api/v1/users`       | Aggregated users and databases               |
| `POST` | `/api/v1/users`       | Create a user and grant database access (operator+) |
| `DELETE` | `/api/v1/users/:username` | Drop a role (`username@server`, operator+) |
| `GET`  | `/api/v1/queries`     | Active queries across clusters               |
| `DELETE` | `/api/v1/queries/:pid?server=` | Terminate a backend (operator+)     |

`POST /api/v1/users` returns `warnings` when the role was created but one or
more grants failed; the response stays `200` because the role exists.

---

## Building

monodb-manager is a standard Go module (Go 1.27.3+):

```bash
go build -o monodb-manager .
```

Run directly from source:

```bash
go run . -config /etc/mono/monodb-manager.yaml
```

The binary embeds `templates/*.html`, `static/app.css` and the self-hosted
fonts, so releases need no network access. The stylesheet is compiled from
`src/app.css` with Tailwind and committed:

```bash
npm ci
npm run build:css   # regenerate static/app.css after changing templates
```

`-templates <dir>` still overrides the embedded templates at runtime for local
experiments.

---

## License

monodb-manager is licensed under GPL-3.0-only. See the [LICENSE](LICENSE) file for details.

[contributors-shield]: https://img.shields.io/github/contributors/monobilisim/monodb-manager.svg?style=for-the-badge
[contributors-url]: https://github.com/monobilisim/monodb-manager/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/monobilisim/monodb-manager.svg?style=for-the-badge
[forks-url]: https://github.com/monobilisim/monodb-manager/network/members
[stars-shield]: https://img.shields.io/github/stars/monobilisim/monodb-manager.svg?style=for-the-badge
[stars-url]: https://github.com/monobilisim/monodb-manager/stargazers
[issues-shield]: https://img.shields.io/github/issues/monobilisim/monodb-manager.svg?style=for-the-badge
[issues-url]: https://github.com/monobilisim/monodb-manager/issues
[license-shield]: https://img.shields.io/github/license/monobilisim/monodb-manager.svg?style=for-the-badge
[license-url]: https://github.com/monobilisim/monodb-manager/blob/main/LICENSE

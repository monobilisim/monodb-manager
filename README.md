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
  - Written in Go, ships with embedded HTML templates, no external runtime needed.

---

## Screens

| Route               | Description                                              |
| ------------------- | -------------------------------------------------------- |
| `/`                 | Status dashboard (HAProxy ports, services, PMM)          |
| `/topology`         | Patroni cluster topology and replication health          |
| `/users`            | User and database grant management                       |
| `/query`            | Active PostgreSQL queries                                |
| `/query-analytics`  | Embedded PMM Query Analytics                             |

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
# List of Patroni-managed PostgreSQL clusters
servers:
  - name: prod-patroni
    user: postgres
    password: "changeme"
    dbname: postgres
    sslmode: disable
    patroni_nodes:
      - "10.0.0.1"
      - "10.0.0.2"
      - "10.0.0.3"
    patroni_port: 8008      # Patroni REST API port (default 8008)
    prefer_leader: true     # connect to the leader by default
    connect_to_leader: true # only connect to the leader

# Databases surfaced in the UI
databases:
  - app_db
  - reporting

# Users hidden from the active-query and user views
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

# UI badge refresh interval in milliseconds (default 3000)
badge_refresh_interval: 3000

# Optional log file path
log_file: "/var/log/monodb-manager.log"
```

> The config file path is mandatory; single-server mode is not supported.

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

Then open `http://localhost:8080` in your browser.

---

## API

All JSON endpoints are grouped under `/api/v1`:

| Method | Endpoint              | Description                                  |
| ------ | --------------------- | -------------------------------------------- |
| `GET`  | `/api/v1/servers`     | List configured Patroni clusters             |
| `GET`  | `/api/v1/status`      | Status-page data (ports, services, PMM URLs) |
| `GET`  | `/api/v1/topology`    | Patroni topology + replication detail        |
| `GET`  | `/api/v1/users`       | Aggregated users and databases               |
| `POST` | `/api/v1/users`       | Create a user and grant database access      |
| `GET`  | `/api/v1/queries`     | Active queries across clusters               |

---

## Building

monodb-manager is a standard Go module (Go 1.25+):

```bash
go build -o monodb-manager .
```

Run directly from source:

```bash
go run . -config /etc/mono/monodb-manager.yaml
```

The resulting binary embeds the HTML templates from the `templates/` directory.

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

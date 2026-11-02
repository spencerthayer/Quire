# Quire

**Quire** is an open-source, self-hosted RSS aggregation, AI curation, and automated publication platform written in Rust. It is designed to run as a single lightweight container on a home server or Portainer stack, persisting all data in SQLite without requiring external cloud infrastructure.

---

## Project Overview

Quire's goal is to turn noisy RSS and Atom feeds into structured, readable daily or weekly publications (EPUB and plain text) for e-readers or text-to-speech listening in **ElevenReader**.

### Implementation Status

Quire is being developed in incremental phases (see [`docs/roadmap.md`](docs/roadmap.md)):

| Capability | Status | Phase |
|---|---|---|
| Rust HTTP server (`axum`), structured logging, graceful shutdown | **Implemented** | Phase 1 |
| SQLite database (`WAL` mode, foreign keys, automatic migrations) | **Implemented** | Phase 1 |
| Browser interface (`Dashboard`, `Feeds`, `Articles`, `Editions`, `Settings`) | **Implemented** | Phase 1 |
| Feed subscription CRUD (add, edit, enable/disable, delete, SSRF URL validation) | **Implemented** | Phase 1 |
| Three-tier configuration (`env` > SQLite `settings` > defaults) & CSRF protection | **Implemented** | Phase 1 |
| Health check endpoint (`GET /health`) & CLI probe (`quire healthcheck`) | **Implemented** | Phase 1 |
| Multi-stage Docker image, Docker Compose, Portainer stack & GHCR CI/CD | **Implemented** | Phase 1 |
| Automated RSS/Atom feed polling, full-text HTML extraction, deduplication | Planned | Phase 2 |
| Article library search, categorization, and retention policies | Planned | Phase 3 |
| AI relevance scoring, classification, and editorial briefings (Ollama / OpenAI) | Planned | Phase 4 |
| Scheduled daily/weekly edition composition and EPUB / TXT export | Planned | Phase 5 |
| Automated ElevenReader publication delivery | Planned | Phase 6 |
| Multi-profile schedules, per-source rules, and advanced automation | Planned | Phase 7 |

> **Note on Current Scope**: Phase 1 establishes the application foundation, SQLite persistence, feed management UI, and container deployment pipeline. Automatic RSS polling, AI summarization, EPUB generation, and ElevenReader delivery are **not yet active** and are scheduled for Phases 2–6.

> **Note on ElevenReader Integration**: ElevenReader does not currently publish an official public REST API for third-party document uploads. Quire's planned Phase 6 integration will rely on an experimental, undocumented Firebase token refresh and upload workflow unless an official ElevenLabs/ElevenReader API becomes available.

> **Security Notice**: Phase 1 does not include built-in user login authentication. Deploy Quire only on a trusted private homelab network or behind an authenticating reverse proxy (such as Authelia, Authentik, Pangolin, or Cloudflare Access). Do not expose an unauthenticated Quire instance directly to the public internet.

---

## Technology Stack

* **Language**: Rust (2024 edition)
* **HTTP Server**: [Axum](https://github.com/tokio-rs/axum) + [Tokio](https://tokio.rs/)
* **Database**: SQLite via [SQLx](https://github.com/launchbadge/sqlx) (WAL mode, embedded migrations)
* **Templates & UI**: Server-rendered HTML via [Askama](https://github.com/askama-rs/askama) (no Node.js runtime required)
* **Planned Pipeline Libraries**: `reqwest`, `feed-rs`, `dom_smoothie`, `epub-builder`
* **Deployment**: Docker Compose & Portainer Stacks (`linux/amd64` and `linux/arm64`)

---

## Architecture Overview

Quire runs as **one Rust executable, one SQLite database, and one Docker container**:

* **Configuration Precedence**: Environment variables (`QUIRE_*`) take highest priority and lock corresponding fields in the browser UI. Nonsecret settings not locked by environment variables can be edited in `/settings` and stored in SQLite.
* **Database**: Stored at `/data/quire.db` (configurable via `QUIRE_DATABASE_URL`). Automatic migrations initialize `feeds`, `articles`, `settings`, `editions`, and `edition_articles` on startup.
* **Web Routes**:
  * `GET /` — System dashboard with feed, article, edition counts, and SQLite health status.
  * `GET /feeds`, `POST /feeds`, `GET /feeds/{id}/edit`, `POST /feeds/{id}`, `POST /feeds/{id}/toggle`, `POST /feeds/{id}/delete` — Full feed subscription management with CSRF protection and URL validation.
  * `GET /articles` — Stored articles view (queries SQLite `articles`).
  * `GET /editions` — Generated editions view (queries SQLite `editions`).
  * `GET /settings`, `POST /settings` — Effective configuration view and nonsecret database settings editor.
  * `GET /health` — Machine-readable JSON health check verifying SQLite connectivity, WAL mode, and foreign key enforcement.

See [`docs/architecture.md`](docs/architecture.md) for full details.

---

## Local Development

### Prerequisites

* Rust toolchain (`rustc` / `cargo` supporting Rust 2024 edition, e.g., Rust 1.85+)
* Docker & Docker Compose (optional, for container builds)

### Running Locally with Cargo

1. Clone the repository:
   ```bash
   git clone https://github.com/spencerthayer/Quire.git
   cd Quire
   ```

2. (Optional) Review `.env.example`. By default, running `cargo run` stores the SQLite database in `./data/quire.db` and listens on `0.0.0.0:8080`:
   ```bash
   QUIRE_DATA_DIR=./data QUIRE_DATABASE_URL=sqlite://./data/quire.db cargo run
   ```

3. Open `http://localhost:8080` in your browser, or check health:
   ```bash
   curl -s http://localhost:8080/health
   ```

### Development & Testing Commands

```bash
# Check code formatting
cargo fmt --all -- --check

# Run Clippy linter
cargo clippy --all-targets --all-features -- -D warnings

# Run automated unit and integration tests (uses isolated temporary SQLite databases)
cargo test

# Build release binary
cargo build --release
```

---

## Docker Compose Deployment

### Build and Run Locally

```bash
docker compose config
docker compose build
docker compose up -d
```

Verify container status and health:

```bash
docker compose ps
curl -s http://localhost:8080/health
```

---

## Portainer Deployment (Homelab Stack)

GitHub Actions automatically builds and publishes multi-architecture (`linux/amd64` and `linux/arm64`) images to GitHub Container Registry on pushes to `main`:

* `ghcr.io/spencerthayer/quire:latest`
* `ghcr.io/spencerthayer/quire:sha-<commit-sha>`

### Deploying in Portainer

1. In Portainer, navigate to **Stacks** → **Add stack**.
2. Name the stack `quire` and paste the following Compose configuration into the **Web editor**:

```yaml
services:
  quire:
    image: ghcr.io/spencerthayer/quire:latest
    container_name: quire
    restart: unless-stopped
    ports:
      - "${QUIRE_PORT:-8080}:8080"
    environment:
      QUIRE_HOST: "0.0.0.0"
      QUIRE_PORT: "8080"
      QUIRE_DATA_DIR: "/data"
      QUIRE_DATABASE_URL: "sqlite:///data/quire.db"
      QUIRE_LOG_LEVEL: "${QUIRE_LOG_LEVEL:-info}"
      QUIRE_TIMEZONE: "${QUIRE_TIMEZONE:-America/Los_Angeles}"
    volumes:
      - quire_data:/data
    healthcheck:
      test:
        - CMD
        - /app/quire
        - healthcheck
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 30s

volumes:
  quire_data:
```

3. (Optional) Add environment variables in Portainer's **Environment variables** section (`QUIRE_PORT`, `QUIRE_TIMEZONE`, `QUIRE_LOG_LEVEL`). For reproducible rollbacks, pin `image:` to an immutable commit tag (`ghcr.io/spencerthayer/quire:sha-...`).
4. Click **Deploy the stack**.
5. Persistent data: All SQLite database files live inside the `quire_data` volume mounted at `/data` and survive container restarts, recreations, and image upgrades.

---

## Configuration Reference

| Variable | Default | Phase Status | Description |
|---|---|---|---|
| `QUIRE_HOST` | `0.0.0.0` | Active (Phase 1) | Bind address for the HTTP server |
| `QUIRE_PORT` | `8080` | Active (Phase 1) | HTTP listen port |
| `QUIRE_LOG_LEVEL` | `info` | Active (Phase 1) | Log filter (`error`, `warn`, `info`, `debug`, `trace`) |
| `QUIRE_DATA_DIR` | `/data` (or `./data` locally) | Active (Phase 1) | Directory for persistent SQLite data and generated files |
| `QUIRE_DATABASE_URL` | `sqlite:///data/quire.db` | Active (Phase 1) | SQLite connection URL |
| `QUIRE_TIMEZONE` | `America/Los_Angeles` | Active (Phase 1) | IANA timezone (locks UI setting when set via env) |
| `QUIRE_AI_PROVIDER` | *(empty)* | Reserved (Phase 4) | LLM provider (`ollama`, `openai`, `openrouter`) |
| `QUIRE_AI_BASE_URL` | *(empty)* | Reserved (Phase 4) | Base URL for OpenAI-compatible or Ollama server |
| `QUIRE_AI_MODEL` | *(empty)* | Reserved (Phase 4) | Model identifier for scoring and summarization |
| `QUIRE_AI_API_KEY` | *(empty)* | Reserved (Phase 4) | Secret API key (environment only; never displayed) |
| `QUIRE_ELEVENREADER_REFRESH_TOKEN` | *(empty)* | Reserved (Phase 6) | Secret Firebase refresh token (environment only; never displayed) |

---

## Project Structure

```text
Quire/
├── .agents/
├── .cursor -> .agents
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── publish-container.yml
├── .dockerignore
├── .env.example
├── .gitignore
├── AGENTS.md
├── Cargo.lock
├── Cargo.toml
├── Dockerfile
├── LICENSE
├── Quire.code-workspace
├── README.md
├── compose.yaml
├── config/
│   └── README.md
├── docs/
│   ├── architecture.md
│   ├── references.md
│   └── roadmap.md
├── migrations/
│   └── 0001_initial_schema.sql
├── repos/                  # Ignored local reference clones
│   └── .gitignore
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── config.rs
│   ├── error.rs
│   ├── articles/
│   │   └── mod.rs
│   ├── db/
│   │   └── mod.rs
│   ├── editions/
│   │   └── mod.rs
│   ├── feeds/
│   │   └── mod.rs
│   └── web/
│       └── mod.rs
├── static/
│   └── styles.css
├── templates/
│   ├── base.html
│   ├── dashboard.html
│   ├── feeds.html
│   ├── feed_edit.html
│   ├── articles.html
│   ├── editions.html
│   └── settings.html
└── tests/
    └── integration_tests.rs
```

---

## License

Released under the [MIT License](LICENSE).

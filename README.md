# Operonx [temprary name ]

Rust backend for our software , name of software not choosen still. The Next.js app remains the frontend while backend responsibilities move here.

## Stack

- Axum + Tokio for HTTP and async runtime
- Tower HTTP for tracing and CORS middleware
- SQLx + PostgreSQL for persistence and migrations
- Argon2id PHC password hashes for credentials
- HMAC-SHA256 signed access tokens in HTTP-only cookies
- UUID v7 identifiers for sortable primary keys
- Tracing + env filters for structured runtime diagnostics

## Local Setup

Start the local PostgreSQL database from the repository root:

```powershell
docker compose up -d postgres
```

The development database URL is:

```text
postgres://postgres:operon_dev@localhost:5432/operon
```

For Windows Rust builds, install either Visual Studio Build Tools with the Visual C++ workload or a GNU toolchain that provides `gcc.exe` and `dlltool.exe`.

Copy `.env.example` to `.env` and update secrets locally.

```powershell
cargo check
cargo run
```

The API listens on `127.0.0.1:8080` by default. In development, if that port is already occupied, `cargo run` automatically tries nearby ports such as `8081` and logs the bound address. The Next.js `/api/coding/*` proxy tries `8080` then `8081` unless `OPERON_API_URL` is set.

## Initial Routes

- `GET /healthz`
- `GET /readyz`
- `POST /auth/signup`
- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`

## Migration Direction

The first migration creates the durable core for the app: users, auth sessions, OAuth accounts, provider profiles, conversations, messages, runs, run events, memories, and audit logs. The durable `runs` and `run_events` tables are the base for Copilot-like long-running agent continuity.

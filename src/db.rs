use std::time::Duration;

use anyhow::Result;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

pub async fn connect(database_url: &str) -> Result<Pool<Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        // Bumped from 5s → 15s: cold-start latency against pooled/remote
        // Postgres (Prisma pooler, Supabase pooler, Neon) can exceed 5s on
        // the first connection after the Rust server boots. With the old
        // timeout, /auth/me returned 500 "pool timed out" on every fresh
        // server start. 15s is still well under typical request timeouts
        // (browser fetch ~30s, LB ~60s) so real exhaustion still surfaces.
        .acquire_timeout(Duration::from_secs(15))
        // Recycle dead connections instead of handing them to a request.
        // Without this, a TCP RST or idle-closed connection (common with
        // remote PgBouncer/Prisma poolers) yields a transient error on
        // the next acquire.
        .test_before_acquire(true)
        .connect(database_url)
        .await?;

    Ok(pool)
}

#[allow(dead_code)]
pub async fn migrate(pool: &Pool<Postgres>) -> Result<()> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

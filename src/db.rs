use std::time::Duration;

use anyhow::Result;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};

pub async fn connect(database_url: &str) -> Result<Pool<Postgres>> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await?;

    Ok(pool)
}

pub async fn migrate(pool: &Pool<Postgres>) -> Result<()> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

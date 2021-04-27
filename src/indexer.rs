use std::path::Path;
use tokio::fs::File;
use tokio::fs::create_dir_all;
use tokio::fs::read_dir;

use sqlx::Row;

use crate::mem_db;


static STANDARD_ROOT: &str = "/standard";
static OPTIMISED_ROOT: &str = "/optimised";


pub struct FileIndex {
    path: String,
    pool: mem_db::SqlitePool,
    matcher: regex::Regex,
}

impl FileIndex {
    pub async fn create(path: impl Into<String>) -> anyhow::Result<Self> {
        let pool = mem_db::SqlitePool::connect(
            "file:file_index_mem?mode=memory&cache=shared",
            10,
        ).await?;

        let re = regex::Regex::new("(?P<route>[/a-zA-Z0-9]+)+(?:\\?type=(?P<type>[a-zA-Z]*))?")?;


        let inst = Self {
            path: path.into(),
            pool,
            matcher: re,
        };

        inst.ensure_tables().await?;

        Ok(inst)
    }

    async fn ensure_tables(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await;

        sqlx::query("CREATE TABLE IF NOT EXISTS cdn_index(path TEXT, file_type TEXT)")
            .execute( conn.as_inner())
            .await?;

        Ok(())
    }

    pub async fn resolve_route(&self, route: &str) -> Option<String> {
        let captures = self.matcher.captures(route)?;
        let route = captures.name("route")?.as_str();
        let type_ = captures.name("type")
            .map(|val| val.as_str());

        let file_type = self.get_file_type(route, type_).await
            .map(|res| Some(res))
            .unwrap_or_else(|_| None)?;

        let mut file_prefix: String;
        if file_type == "webp" {
            file_prefix = format!("{}{}{}", self.path, OPTIMISED_ROOT, route)
        } else {
            file_prefix = format!("{}{}{}", self.path, STANDARD_ROOT, route)
        }

        let resolved = format!("{}.{}", file_prefix, file_type);

        Some(resolved)
    }

    async fn get_file_type(
        &self,
        route: &str,
        maybe_type: Option<&str>,
    ) -> anyhow::Result<String> {
        let mut conn = self.pool.acquire().await;

        let maybe_result = if let Some(type_) = maybe_type {
            sqlx::query("SELECT file_type FROM cdn_index WHERE path = ? AND file_type = ?")
                .bind(route)
                .bind(type_)
                .fetch_one(conn.as_inner())
                .await?
        } else {
            sqlx::query("SELECT file_type FROM cdn_index WHERE path = ? AND file_type = 'webp'")
                .bind(route)
                .fetch_one(conn.as_inner())
                .await?
        };


        let res: String = res.try_get("cdn_index")?;

        Ok(res)
    }
}


async fn watch_directory(path: &str, pool: mem_db::SqlitePool) -> anyhow::Result<()> {


    Ok(())
}

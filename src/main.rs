use sqlx::Row;

mod mem_db;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = mem_db::SqlitePool::connect(
        "file:memdb1?mode=memory&cache=shared",
        10,
    ).await?;

    {
        let mut conn = pool.acquire().await;
        sqlx::query("CREATE TABLE bob (id INTEGER, name STRING)")
            .execute(conn.as_inner())
            .await?;

    }

    {
        let mut conn = pool.acquire().await;
        sqlx::query("INSERT INTO bob (id, name) VALUES (?, ?)")
            .bind(123i32)
            .bind("bobby")
            .execute(conn.as_inner())
            .await?;
    }

    {
        let mut conn = pool.acquire().await;
        let res = sqlx::query("SELECT * FROM bob")
            .fetch_all(conn.as_inner())
            .await?;

        for row in res {
            println!("{:?}", row.columns());
        }
    }

    Ok(())
}


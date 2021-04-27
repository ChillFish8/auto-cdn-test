use sqlx::Row;

mod mem_db;
mod indexer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let thing = indexer::FileIndex::create("./cdn").await?;
    let out = thing.resolve_route("/hello/world/data?type=webp").await;
    println!("{:?}", out);
    Ok(())
}


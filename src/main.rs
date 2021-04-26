use sqlx::sqlite::SqliteConnection;
use sqlx::sqlite::SqlitePool as OtherPool;
use sqlx::{Row, Connection};

use async_channel::{Sender, Receiver, unbounded};
use tokio::task::JoinHandle;
use futures_util::FutureExt;


use std::mem;
use std::borrow::BorrowMut;
use std::sync::Arc;


pub struct ConnectionHandle {
    conn: SqliteConnection,
    tx: Sender<SqliteConnection>,
}

impl ConnectionHandle {
    pub fn new(conn: SqliteConnection, tx: Sender<SqliteConnection>) -> Self {
        Self {
            conn,
            tx,
        }
    }

    pub fn release(self) {
        let conn = self.conn;
        self.tx.try_send(conn)
            .expect("failed to return connection to pool");
    }

    pub fn as_inner(&mut self) -> &mut SqliteConnection {
        self.conn.borrow_mut()
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let conn = mem::replace(
            &mut self.conn,
            // doesnt matter because dropping
            unsafe { mem::MaybeUninit::uninit().assume_init() },
        );

        self.tx.try_send(conn)
            .expect("failed to return connection to pool");
    }
}


enum Message {
    Acquire,
    Shutdown,
}


#[derive(Clone)]
pub struct SqlitePoolHandler {
    tx: Sender<Message>,
    rx: Receiver<SqliteConnection>,

    pool_returner: Sender<SqliteConnection>,
    task: Arc<JoinHandle<anyhow::Result<()>>>,
}

impl SqlitePoolHandler {
    fn new(
        tx: Sender<Message>,
        rx: Receiver<SqliteConnection>,
        pool_returner: Sender<SqliteConnection>,
        task: JoinHandle<anyhow::Result<()>>,
    ) -> Self {
        Self {
            tx,
            rx,
            pool_returner,
            task: Arc::new(task),
        }
    }

    pub async fn acquire(&self) -> ConnectionHandle {
        let _ = self.tx.send(Message::Acquire).await;
        let conn = self.rx
            .recv()
            .await
            .expect("failed to receive connection, is the pool dead?");

        ConnectionHandle::new(conn, self.pool_returner.clone())
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        if let Some(res) = self.task.now_or_never() {
            let inner = res?;
            inner?
        };

        if let Err(_) = self.tx.send(Message::Shutdown).await {
            return Err(anyhow::Error::msg("failed to shutdown worker with notification"))
        };


        Ok(())
    }
}


struct SqlitePool {
    uri: String,
    connections: Vec<SqliteConnection>,

    waker: Receiver<Message>,
    returning_conns: Receiver<SqliteConnection>,
    outbound_conns: Sender<SqliteConnection>,
}

impl SqlitePool {
    const DEFAULT_NUM_CONNS: usize = 10;

    pub async fn connect(uri: &str) -> anyhow::Result<SqlitePoolHandler> {
        Self::connect_and_begin(uri, Self::DEFAULT_NUM_CONNS)
    }

    pub async fn connect_with_num(uri: &str, connections: usize) -> anyhow::Result<SqlitePoolHandler> {
        Self::connect_and_begin(uri, connections)
    }

    async fn connect_and_begin(uri: &str, conn_count: usize) -> anyhow::Result<SqlitePoolHandler> {
        let mut connections = Vec::with_capacity(conn_count);
        for _ in 0..connections {
            let conn = SqliteConnection::connect(uri).await?;
            connections.push(conn);
        }

        let (waker_tx, waker_rx) = unbounded();
        let (out_tx, out_rx) = unbounded();
        let (returner_tx, returner_rx) = unbounded();

        let inst = Self {
            uri: uri.into(),
            connections,
            waker: waker_rx,
            returning_conns: returner_rx,
            outbound_conns: out_tx,
        };

        let task = tokio::spawn(inst.run_watcher());

        let handle = SqlitePoolHandler::new(
            waker_tx,
            out_rx,
            returner_tx,
            task,
        );

        Ok(handle)
    }

    async fn run_watcher(mut self) -> anyhow::Result<()> {

    }
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut conn1 = SqliteConnection::connect(
        "file:memdb1?mode=memory&cache=shared"
    ).await?;

    let mut conn2 = SqliteConnection::connect(
        "file:memdb1?mode=memory&cache=shared"
    ).await?;

    sqlx::query("CREATE TABLE bob (id INTEGER, name STRING)")
        .execute(&mut conn2)
        .await?;

    sqlx::query("INSERT INTO bob (id, name) VALUES (?, ?)")
        .bind(123i32)
        .bind("bobby")
        .execute(&mut conn1)
        .await?;

    let res = sqlx::query("SELECT * FROM bob")
        .fetch_all(&mut conn2)
        .await?;

    for row in res {
        println!("{:?}", row.columns());
    }

    Ok(())
}


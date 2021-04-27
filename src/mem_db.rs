#![allow(unused)]

use sqlx::sqlite::SqliteConnection;
use sqlx::{Row, Connection};

use async_channel::{Sender, Receiver, unbounded, bounded};
use tokio::task::JoinHandle;
use futures_util::FutureExt;

use std::mem;


/// A acquired connection handler.
///
/// This struct can be used to interact with the underlying
/// connection while maintaining the pool's state.
///
/// If this handle is dropped the connection is automatically returned to
/// the pool.
///
/// Due to this struct implementing and returning the connection on drop
/// it does not have a explicit release method.
///
/// ```rust
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let pool = SqlitePool::connect(
///         "file:memdb1?mode=memory&cache=shared"
///     ).await?;
///
///     {
///         let mut conn: ConnectionHandle = pool.acquire();
///         let con_ref = conn.as_inner();
///     }
/// }
/// ```
pub struct ConnectionHandle {
    conn: Option<SqliteConnection>,
    connection_return: Sender<SqliteConnection>,
}

impl ConnectionHandle {
    /// Creates a new connection handle from a given connection
    /// and the returner channel.
    fn new(
        conn: SqliteConnection,
        connection_return: Sender<SqliteConnection>,
    ) -> Self {
        Self {
            conn: Some(conn),
            connection_return,
        }
    }

    /// Returns the mutable inner reference to the `SqliteConnection`
    pub fn as_inner(&mut self) -> &mut SqliteConnection {
        self.conn.as_mut().unwrap()
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let conn = self.conn.take().unwrap();

        self.connection_return.try_send(conn)
            .expect("failed to return connection to pool");
    }
}


/// A pool of `sqlx::sqlite::SqliteConnection`'s.
///
/// This pool works in a simple round robin style pool where
/// connections are taken then added back to a queue of connections
/// operating on a first come first served basis.
///
/// If something wants to acquire a connection while all are taken
/// it will yield control until one is ready.
#[derive(Clone)]
pub struct SqlitePool {
    pool_send: Sender<SqliteConnection>,
    pool: Receiver<SqliteConnection>,
}

impl SqlitePool {
    /// Connects to the database with n amount of connections and adds
    /// them to the pool.
    pub async fn connect(
        uri: &str,
        connections: usize,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = bounded(connections + 2);
        for _ in 0..connections {
            let conn = SqliteConnection::connect(uri).await?;
            let _ = tx.send(conn).await;
        }

        Ok(Self {
            pool_send: tx,
            pool: rx,
        })
    }

    /// Acquire a connection from the pool.
    ///
    /// Panics if the pool has been shutdown before.
    pub async fn acquire(&self) -> ConnectionHandle {
        let conn = self.pool
            .recv()
            .await
            .expect("failed to receive connection, is the pool dead?");

        ConnectionHandle::new(conn, self.pool_send.clone())
    }

    /// Shutdown the SqlitePool.
    ///
    /// After this you cannot acquire connections.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        while let Ok(conn) = self.pool.try_recv() {
            let _ = conn.close().await;
        }

        self.pool.close();

        Ok(())
    }
}

#![allow(unused)]

use sqlx::sqlite::SqliteConnection;
use sqlx::{Row, Connection};

use async_channel::{Sender, Receiver, unbounded, bounded};
use tokio::task::JoinHandle;
use futures_util::FutureExt;

use std::mem;


pub struct ConnectionHandle {
    conn: Option<SqliteConnection>,
    tx: Sender<Message>,
}

impl ConnectionHandle {
    fn new(conn: SqliteConnection, tx: Sender<Message>) -> Self {
        Self {
            conn: Some(conn),
            tx,
        }
    }

    pub fn as_inner(&mut self) -> &mut SqliteConnection {
        self.conn.as_mut().unwrap()
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let conn = self.conn.take().unwrap();

        self.tx.try_send(Message::ConnectionReturn(conn))
            .expect("failed to return connection to pool");
    }
}


enum Message {
    Acquire,
    ConnectionReturn(SqliteConnection),
    Shutdown,
}


#[derive(Clone)]
pub struct SqlitePoolHandler {
    tx: Sender<Message>,
    rx: Receiver<SqliteConnection>,
    task: Receiver<JoinHandle<anyhow::Result<()>>>,
}

impl SqlitePoolHandler {
    fn new(
        tx: Sender<Message>,
        rx: Receiver<SqliteConnection>,
        task: Receiver<JoinHandle<anyhow::Result<()>>>,
    ) -> Self {
        Self {
            tx,
            rx,
            task,
        }
    }

    pub async fn acquire(&self) -> ConnectionHandle {
        let _ = self.tx.send(Message::Acquire).await;
        let conn = self.rx
            .recv()
            .await
            .expect("failed to receive connection, is the pool dead?");

        ConnectionHandle::new(conn, self.tx.clone())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let task = self.task.recv().await
            .expect("failed to get shutdown task");

        if let Some(res) = task.now_or_never() {
            let inner = res?;
            inner?
        };

        if let Err(_) = self.tx.send(Message::Shutdown).await {
            return Err(anyhow::Error::msg("failed to shutdown worker with notification"))
        };

        Ok(())
    }
}


pub struct SqlitePool {
    uri: String,
    connections: (Sender<SqliteConnection>, Receiver<SqliteConnection>),

    waker: Receiver<Message>,
    outbound_conns: Sender<SqliteConnection>,
}

impl SqlitePool {
    const DEFAULT_NUM_CONNS: usize = 10;

    pub async fn connect(uri: &str) -> anyhow::Result<SqlitePoolHandler> {
        Self::connect_and_begin(uri, Self::DEFAULT_NUM_CONNS).await
    }

    pub async fn connect_with_num(uri: &str, connections: usize) -> anyhow::Result<SqlitePoolHandler> {
        Self::connect_and_begin(uri, connections).await
    }

    async fn connect_and_begin(uri: &str, conn_count: usize) -> anyhow::Result<SqlitePoolHandler> {
        let connections = bounded(conn_count + 2);
        for _ in 0..conn_count {
            let conn = SqliteConnection::connect(uri).await?;
            let _ = connections.0.send(conn).await;
        }

        let (waker_tx, waker_rx) = unbounded();
        let (out_tx, out_rx) = unbounded();
        let (task_tx, task_rx) = bounded(1);

        let inst = Self {
            uri: uri.into(),
            connections,
            waker: waker_rx,
            outbound_conns: out_tx,
        };

        let task = tokio::spawn(inst.run_watcher());
        task_tx.send(task).await?;

        let handle = SqlitePoolHandler::new(
            waker_tx,
            out_rx,
            task_rx,
        );

        Ok(handle)
    }

    async fn run_watcher(self) -> anyhow::Result<()> {
        loop {
            let msg = self.waker.recv().await?;
            match msg {
                Message::Shutdown => break,
                Message::ConnectionReturn(conn) => {
                    if let Err(_) = self.connections.0.try_send(conn) {
                        return Err(anyhow::Error::msg("failed to add connection back to pool"))
                    };
                },
                Message::Acquire => {
                    let conn = self.connections.1.recv().await?;
                    if let Err(_) = self.outbound_conns.send(conn).await {
                        return Err(anyhow::Error::msg("failed to send connection to acquirer"))
                    };
                },
            }
        }

        Ok(())
    }
}
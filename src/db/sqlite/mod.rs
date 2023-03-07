use std::path::Path;

use eyre::{Context, Result};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;

use crate::db::{Db, FileInfo, Snapshot, SnapshotInfo};
use crate::Hash;

pub struct SqliteDb {
    db: rusqlite::Connection,
}

impl SqliteDb {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let db = spawn_blocking(|| {
            rusqlite::Connection::open(path).context("Failed to open or create database")
        })
        .await??;
        let db = Self::init(db).await?;

        Ok(Self { db })
    }

    pub async fn open_memory() -> Result<Self> {
        Ok(Self {
            db: Self::init(
                rusqlite::Connection::open_in_memory().context("Failed to open in memory")?,
            )
            .await?,
        })
    }

    async fn init(mut db: rusqlite::Connection) -> Result<rusqlite::Connection> {
        spawn_blocking(move || {
            let migrations = Migrations::new(MIGRATIONS.iter().copied().map(M::up).collect());
            migrations
                .to_latest(&mut db)
                .context("Initializing database")?;
            Ok(db)
        })
        .await?
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Archive {
    id: i64,
    machine_name: String,
    write_lock: i64,
}

const MIGRATIONS: &[&str] = &[include_str!("migrations/00001.sql")];

impl Db for SqliteDb {
    fn file(&self, path: &Path, snapshot: Snapshot) -> Result<Option<FileInfo>> {
        todo!()
    }

    fn record_new_file(&mut self, file: FileInfo) -> Result<()> {
        todo!()
    }

    fn record_file_removed(&mut self, file: &Path) -> Result<()> {
        todo!()
    }

    fn record_file_modified(&mut self, file: &Path, chunks: Vec<Hash>) -> Result<()> {
        todo!()
    }

    fn prune_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        todo!()
    }

    fn snapshot(&self, snapshot: Snapshot) -> Result<SnapshotInfo> {
        todo!()
    }

    fn snapshots(&self) -> Result<Vec<SnapshotInfo>> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn validate_migrations() {
        Migrations::new(MIGRATIONS.iter().copied().map(M::up).collect())
            .validate()
            .unwrap()
    }

    #[test]
    fn init() {
        SqliteDb::open_memory().init().unwrap()
    }
}

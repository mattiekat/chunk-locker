use std::num::{NonZeroI64, NonZeroU64};
use std::path::Path;

use eyre::{Context, Result};
use rusqlite_migration::{Migrations, M};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::task::spawn_blocking;

use crate::db::{Archive, ArchiveDb, Db, FileInfo, Snapshot, SnapshotId};
use crate::Hash;

pub struct SqliteDb {
    db: rusqlite::Connection,
}

impl SqliteDb {
    /// Opens and initializes the database from disk. This will create a new database if it does not
    /// exist already and will apply migrations if it is outdated.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let db = rusqlite::Connection::open(path).context("Failed to open or create database")?;
        let db = Self::init(db)?;

        Ok(Self { db })
    }

    /// Opens and initializes the database in memory
    pub fn open_memory() -> Result<Self> {
        Ok(Self {
            db: Self::init(
                rusqlite::Connection::open_in_memory().context("Failed to open in memory")?,
            )?,
        })
    }

    fn init(mut db: rusqlite::Connection) -> Result<rusqlite::Connection> {
        let migrations = Migrations::new(MIGRATIONS.iter().copied().map(M::up).collect());
        migrations
            .to_latest(&mut db)
            .context("Initializing database")?;
        Ok(db)
    }
}

/// The special Sqlite RowId type that can be None to indicate it should be set on insert but will
/// always be a value when read from the db.
#[repr(transparent)]
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
struct RowId(Option<i64>);

impl RowId {
    const fn new_unset() -> Self {
        Self(None)
    }

    const fn new(id: i64) -> Self {
        Self(Some(id))
    }

    const fn as_int(self) -> i64 {
        self.0.expect("Row id was not set")
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct DbArchive {
    id: RowId,
    machine_name: String,
    write_lock: i64,
}

struct DbArchivePath {
    rowid: RowId,
    archive_id: i64,
}

const MIGRATIONS: &[&str] = &[include_str!("migrations/00001.sql")];

impl Db for SqliteDb {
    type ADB = Self;

    fn archives(&self) -> Result<Vec<Archive>> {
        todo!()
    }

    fn archive(&self, name: &str) -> Result<Archive> {
        todo!()
    }

    unsafe fn force_claim_archive_lock(&mut self, name: &str) -> Result<bool> {
        todo!()
    }

    fn create_archive(self, name: &str) -> Result<Self::ADB> {
        todo!()
    }

    fn open_archive(self, name: &str) -> Result<Self::ADB> {
        todo!()
    }
}

impl ArchiveDb for SqliteDb {
    fn file(&self, path: &Path, snapshot: SnapshotId) -> Result<Option<FileInfo>> {
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

    fn prune_snapshot(&mut self, snapshot: SnapshotId) -> Result<()> {
        todo!()
    }

    fn snapshot(&self, snapshot: SnapshotId) -> Result<Snapshot> {
        todo!()
    }

    fn snapshots(&self) -> Result<Vec<Snapshot>> {
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
        let db = SqliteDb::open_memory().unwrap();
    }
}

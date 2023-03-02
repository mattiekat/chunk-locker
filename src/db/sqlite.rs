//! ## Version
//! A table which defines the current version of each of the other tables.
//! - table_name
//! - version
//!
//! ## Archive
//! Represents backups of a specific machine path.
//! - machine name
//! - total archive size
//! - root directory for backup
//! - fs type (e.g. ext4, xfs, ntfs, hfs...)
//! - remote
//!
//! ## Snapshot
//! A single point in time for an archive
//! - snapshot id
//! - hashing algorithm used
//! - encryption algorithm used
//! - software version used
//! - time started
//! - time finished
//!
//! ## Directory
//! Includes files and other directories that are contained in it.
//! - directory id
//! - snapshot id
//! - partial path (just one segment)
//! - parent directory id
//! - permissions
//! - owner
//! - group
//! - creation time
//! - modification time
//!
//! ## File
//! A single file and the chunks that compose it
//! - file id
//! - snapshot id
//! - parent directory id
//! - filename
//! - checksum
//! - permissions
//! - owner
//! - group
//! - creation time
//! - modification time
//! - file size
//! - compression algorithm used (e,g, raw files might not get compressed)
//!
//! ## File Relation
//! - File id
//! - Chunk id
//! - Snapshot id
//! - Ordering
//!
//! ## Chunk
//! - Chunk id
//! - hash
//! - (how to find it if not just storing them by hashes in the blobstore)

use crate::db::{Db, FileInfo, Snapshot, SnapshotInfo};
use crate::Hash;
use itertools::Itertools;
use rusqlite::OpenFlags;
use std::path::Path;
use tokio::task::spawn_blocking;

pub struct SqliteDb {
    db: rusqlite::Connection,
}

impl SqliteDb {
    pub async fn open(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let db = spawn_blocking(|| {
            rusqlite::Connection::open(path).expect("Failed to open or create database")
        })
        .await
        .expect("Thread error");

        Self { db }
    }

    pub async fn open_memory() -> Self {
        Self {
            db: rusqlite::Connection::open_in_memory().expect("Failed to open in memory"),
        }
    }
}

macro_rules! create_table {
    ($table_name:literal, $($row:literal),+$(,)?) => {
        concat!("CREATE TABLE IF NOT EXISTS ", $table_name, " (", $( $row, ",", )+ ") STRICT;")
    }
}

impl Db for SqliteDb {
    fn init(&mut self) -> eyre::Result<()> {
        self.db.execute(
            create_table!(
                "version",
                "table_name TEXT PRIMARY KEY",
                "version INTEGER NOT NULL",
            ),
            (),
        );
        self.db.execute(
            create_table!(
                "archive",
                "id INTEGER PRIMARY KEY",
                "machine_name TEXT NOT NULL",
                "write_lock INTEGER",
            ),
            (),
        );
        self.db.execute(
            create_table!(
                "snapshot",
                "id INTEGER PRIMARY KEY",
                "archive_id INTEGER NOT NULL",
                "hash_type TEXT NOT NULL",
                "encryption_type TEXT NOT NULL",
                "chunking_type TEXT NOT NULL",
                "software_version INTEGER NOT NULL",
                "time_started INTEGER NOT NULL",
                "time_finished INTEGER",
                "total_files INTEGER",
                "total_directories INTEGER",
                "uncompressed_size INTEGER",
                "compressed_size INTEGER",
                "deduplicated_size INTEGER",
                "FOREIGN KEY (archive_id)
                    REFERENCES archive (id)
                        ON UPDATE CASCADE
                        ON DELETE CASCADE"
            ),
            (),
        );
        self.db.execute(
            create_table!(
                "filesystem_entry",
                "id INTEGER PRIMARY KEY",
                "snapshot_id INTEGER NOT NULL",
                "parent_directory_id INTEGER",
                "partial_path TEXT NOT NULL",
                "permissions INTEGER NOT NULL",
                "owner INTEGER NOT NULL",
                "group INTEGER NOT NULL",
                "creation_time INTEGER NOT NULL",
                "modification_time INTEGER NOT NULL",
                "FOREIGN KEY (snapshot_id)
                    REFERENCES snapshot (id)
                        ON UPDATE CASCADE
                        ON DELETE CASCADE",
                "FOREIGN KEY (parent_directory_id)
                    REFERENCES directory (id)
                        ON UPDATE CASCADE
                        ON DELETE SET NULL",
            ),
            (),
        );
        self.db.execute(
            create_table!(
                "file",
                "id INTEGER PRIMARY KEY",
                "filesystem_entry_id INTEGER NOT NULL",
                "compression_type TEXT NOT NULL",
                "original_size INTEGER NOT NULL",
                "stored_size INTEGER NOT NULL",
                "checksum BLOB NOT NULL",
                "FOREIGN KEY (filesystem_entry_id)
                    REFERENCES filesystem_entry (id)
                        ON UPDATE CASCADE
                        ON DELETE CASCADE",
            ),
            (),
        );
        self.db.execute(create_table!(
            "file_relation",
            "file_id INTEGER NOT NULL",
            "chunk_id INTEGER NOT NULL",
            "chunk_ordinal INTEGER NOT NULL",
            "FOREIGN KEY (file_id)
                REFERENCES file (id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE",
        ), ());

        self.db.execute(create_table!(
            "chunk",
            "chunk_id INTEGER PRIMARY KEY",
            "hash BLOB UNIQUE NOT NULL",
        ), ());
        todo!()
    }

    fn file(&self, path: &Path, snapshot: Snapshot) -> eyre::Result<Option<FileInfo>> {
        todo!()
    }

    fn record_new_file(&mut self, file: FileInfo) -> eyre::Result<()> {
        todo!()
    }

    fn record_file_removed(&mut self, file: &Path) -> eyre::Result<()> {
        todo!()
    }

    fn record_file_modified(&mut self, file: &Path, chunks: Vec<Hash>) -> eyre::Result<()> {
        todo!()
    }

    fn prune_snapshot(&mut self, snapshot: Snapshot) -> eyre::Result<()> {
        todo!()
    }

    fn snapshot(&self, snapshot: Snapshot) -> eyre::Result<SnapshotInfo> {
        todo!()
    }

    fn snapshots(&self) -> eyre::Result<Vec<SnapshotInfo>> {
        todo!()
    }
}

//! Database format:
//! # Event Format
//! Each entity should be stored as a chain of events, with each snapshot making possible updates to
//! it.
//!
//! Main problem with this approach is ensuring there is no corruption on the remote. It would be
//! really easy for an update to go missing and we just assume it never happened. It also makes it
//! take a really long time to figure out the details for a specific file since you have to merge
//! a bunch of events which is bad for a FUSE implementation. It also takes a lot of time to prune
//! information from.
//!
//! ## Archive (`archive`)
//! ### Create
//! - root directory to backup
//! - machine name
//! - host FS type
//! - remote
//! - timestamp
//! - exclusion patterns
//! - inclusion patterns
//!
//! ## Snapshot (`snapshot-<snapid>`)
//! ### Snapshot Started
//! - previous snapshot id
//! - timestamp
//! - hashing algorithm used
//! - encryption algorithm used
//! - software version used
//!
//! ### Snapshot Completed
//! - timestamp
//!
//! ### Add Directory
//! - parent directory id
//! - directory id
//! - partial path (just one segment)
//!
//! ## Directory (`directory-<id>/<snapid>`)
//! ### Add Directory
//! - parent directory id
//! - directory id
//! - partial path (just one segment)
//! - created time
//!
//! ### Update Permissions
//! - permissions
//!
//! # Update Owner
//! - owner
//! - group
//!
//! # Update Modtime
//! - timestamp
//!
//! ## File (`file-<id>-<snapid>`)
//!

#![allow(unused)]

use crate::Hash;
use async_trait::async_trait;
use eyre::Result;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{oneshot, Mutex};

mod async_db;
mod sqlite;

#[async_trait]
trait DbLoader {
    type Output: Db;

    async fn load() -> Result<Self::Output>;
}

/// ## DB reqs:
/// - Know if file on disk is different (so get hash/size/mod time of last snapshoted version)
/// - Get ordered list of chunk hashes that comprise a given file
/// - Know what compression algo was used for a given file
/// - Add new files for a new snapshot
/// - Remove files for a new snapshot
/// - Modify files for a new snapshot
/// - Prune a snapshot (okay if this is a moderately costly op)
/// - Know what the latest snapshot was (to verify if it is the same as the remote)
/// - Upgradeable
///
/// ## Safe Assumptions:
/// - Snapshots are immutable (except the current one until `completed: true`)
/// - Pruning will never remove the latest snapshot
/// - hashes will not collide
trait Db {
    type ADB: ArchiveDb;

    fn archives(&self) -> Result<Vec<Archive>>;
    fn archive(&self, name: &str) -> Result<Archive>;

    /// Returns true if it had to use force to lock the archive. Calling this function may cause
    /// database corruption if the other entity to have locked the database is still running. This
    /// should be called if the user is able to verify the application that locked the db
    /// is no longer running.
    unsafe fn force_claim_archive_lock(&mut self, name: &str) -> Result<bool>;

    fn create_archive(self, name: &str) -> Result<Self::ADB>;
    fn open_archive(self, name: &str) -> Result<Self::ADB>;
}

trait ArchiveDb {
    fn file(&self, path: &Path, snapshot: SnapshotId) -> Result<Option<FileInfo>>;

    fn record_new_file(&mut self, file: FileInfo) -> Result<()>;
    fn record_file_removed(&mut self, file: &Path) -> Result<()>;
    fn record_file_modified(&mut self, file: &Path, chunks: Vec<Hash>) -> Result<()>;

    fn prune_snapshot(&mut self, snapshot: SnapshotId) -> Result<()>;
    fn snapshot(&self, snapshot: SnapshotId) -> Result<Snapshot>;
    fn snapshots(&self) -> Result<Vec<Snapshot>>;
}

enum CompressionAlgorithm {}

enum SnapshotId {
    Latest,
    Id(u64),
}

struct Snapshot {
    date: SystemTime,
    id: u64,
    completed: bool,
}

struct FileInfo {
    hash: Hash,
    chunks: Vec<Hash>,
    compression_algorithm: CompressionAlgorithm,
}

struct Archive {
    name: String,
    machine_name: String,
    write_lock: Option<i64>,
}

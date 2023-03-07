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

use std::collections::VecDeque;
use crate::Hash;
use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, oneshot};
use eyre::Result;

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

    fn archives(&self) -> Result<Vec<()>>;
    fn archive(&self, name: &str) -> Result<()>;

    fn create_archive(self) -> Result<Self::ADB>;
    fn open_archive(self) -> Result<Self::ADB>;
}

trait ArchiveDb {
    fn file(&self, path: &Path, snapshot: Snapshot) -> Result<Option<FileInfo>>;

    fn record_new_file(&mut self, file: FileInfo) -> Result<()>;
    fn record_file_removed(&mut self, file: &Path) -> Result<()>;
    fn record_file_modified(&mut self, file: &Path, chunks: Vec<Hash>) -> Result<()>;

    fn prune_snapshot(&mut self, snapshot: Snapshot) -> Result<()>;
    fn snapshot(&self, snapshot: Snapshot) -> Result<SnapshotInfo>;
    fn snapshots(&self) -> Result<Vec<SnapshotInfo>>;
}

enum CompressionAlgorithm {}

enum Snapshot {
    Latest,
    Id(u64),
}

struct SnapshotInfo {
    date: SystemTime,
    id: u64,
    completed: bool,
}

struct FileInfo {
    hash: Hash,
    chunks: Vec<Hash>,
    compression_algorithm: CompressionAlgorithm,
}

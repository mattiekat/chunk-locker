#![allow(unused)]

use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use eyre::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};

use crate::Hash;

mod async_db;
mod sqlite;

trait DbLoader {
    type Output: Db;

    fn load() -> Result<Self::Output>;
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

    fn create_archive(&self, name: &str) -> Result<Self::ADB>;
    fn open_archive(&self, name: &str) -> Result<Self::ADB>;
}

trait ArchiveDb {
    type View: SnapshotView;
    type IWriter: IncrementalSnapshotWriter;
    type FWriter: FullSnapshotWriter;

    fn snapshot(&self, snapshot: SnapshotId) -> Result<Self::View>;
    fn snapshots(&self) -> Result<Vec<Snapshot>>;

    fn create_incremental_snapshot(&self) -> Result<Self::IWriter>;
    fn create_full_snapshot(&self) -> Result<Self::FWriter>;
    fn prune_snapshot(&mut self, snapshot: SnapshotId) -> Result<()>;
}

/// View of directories, files, and symlinks captured by a snapshot. This will compose multiple
/// snapshots together if the snapshot is incremental.
trait SnapshotView {
    // if path is None, it will list the roots
    fn list_filesystem_entries(&self, path: Option<&Path>) -> Result<Vec<()>>;
    fn file(&self, path: &Path) -> Result<Option<FileInfo>>;
}

trait IncrementalSnapshotWriter: SnapshotView {
    type BaseView: SnapshotView;

    // Access the previous snapshot that this is building on.
    fn base(&self) -> &Self::BaseView;

    fn record_new_directory(&mut self, path: &Path) -> Result<()>;
    fn record_directory_removed(&mut self, path: &Path) -> Result<()>;

    fn record_new_file(&mut self, file: FileInfo) -> Result<()>;
    fn record_file_removed(&mut self, file: &Path) -> Result<()>;
    fn record_file_modified(&mut self, file: &Path, chunks: Vec<Hash>) -> Result<()>;
}

trait FullSnapshotWriter: SnapshotView {
    fn record_directory(&mut self, path: &Path) -> Result<()>;
    fn record_file(&mut self, file: FileInfo) -> Result<()>;
    fn record_chunk(&mut self) -> Result<()>;
}

enum CompressionAlgorithm {}

#[derive(Copy, Clone)]
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
    remote: Remote,
}

#[derive(Serialize, Deserialize)]
enum Remote {}

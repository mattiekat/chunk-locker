#![allow(unused)]

use crate::Hash;
use async_trait::async_trait;
use std::path::Path;
use std::time::SystemTime;

#[async_trait]
trait DbLoader {
    type Output: Db;

    async fn load() -> eyre::Result<Self::Output>;
}

/// Async-ifying wrapper to handle sync backend databases.
/// DB calls all run in a single thread with channels to funnel data to and from asynchronously.
struct ArchiveDb;

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
    fn file(&self, path: Path, snapshot: Snapshot) -> eyre::Result<Option<FileInfo>>;
    fn record_new_file(&mut self, file: FileInfo) -> eyre::Result<()>;
    fn record_file_removed(&mut self, file: Path) -> eyre::Result<()>;
    fn record_file_modified(&mut self, file: Path, chunks: Vec<Hash>) -> eyre::Result<()>;
    fn prune_snapshot(&mut self, snapshot: Snapshot) -> eyre::Result<()>;
    fn snapshot(&self, snapshot: Snapshot) -> eyre::Result<SnapshotInfo>;
    fn snapshots(&self) -> eyre::Result<Vec<SnapshotInfo>>;
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

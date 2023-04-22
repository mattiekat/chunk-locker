use std::mem;
use std::path::PathBuf;
use std::sync::mpsc as stdmpsc;

use tokio::sync::oneshot;

use crate::db::{Db, FileInfo, SnapshotId};

enum DbOp {
    GetFile {
        path: PathBuf,
        snapshot: SnapshotId,
        cb: oneshot::Sender<eyre::Result<Option<FileInfo>>>,
    },
}

/// Async-ifying wrapper to handle sync backend databases.
/// DB calls all run in a single thread with channels to funnel data to and from asynchronously.
struct AsyncDb {
    thread: Option<std::thread::JoinHandle<()>>,
    sender: Option<stdmpsc::Sender<DbOp>>,
}

impl AsyncDb {
    pub fn start<D: Db + Send + 'static>(db: D) -> Self {
        let (tx, rx) = stdmpsc::channel();
        let task = AsyncDbTask {
            db,
            request_queue: rx,
        };
        Self {
            thread: Some(std::thread::spawn(move || task.run())),
            sender: Some(tx),
        }
    }
}

impl Drop for AsyncDb {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            mem::drop(sender)
        }
        if let Some(thread) = self.thread.take() {
            thread.join().expect("Failed to join thread")
        }
    }
}

struct AsyncDbTask<D> {
    request_queue: stdmpsc::Receiver<DbOp>,
    db: D,
}

impl<D: Db> AsyncDbTask<D> {
    fn run(mut self) {

        for op in self.request_queue {
            match op {
                DbOp::GetFile { path, snapshot, cb } => {
                    cb.send(self.db.file(path.as_path(), snapshot));
                }
            }
        }
    }
}

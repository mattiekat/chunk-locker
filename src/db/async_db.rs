use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use std::collections::VecDeque;
use std::time::Duration;
use crate::db::Db;

enum DbOp {

}

enum DbOpResponse {

}

/// Async-ifying wrapper to handle sync backend databases.
/// DB calls all run in a single thread with channels to funnel data to and from asynchronously.
struct AsyncDb {
    thread: std::thread::JoinHandle<()>,
    request_queue: Arc<Mutex<VecDeque<(DbOp, oneshot::Sender<DbOpResponse>)>>>,
}

impl AsyncDb {
    pub fn start<D: Db + Send>(db: D) -> Self {
        let request_queue = Default::default();
        let thread = std::thread::spawn(AsyncDb::run(db, request_queue.clone()));
        Self {
            thread,
            request_queue,
        }
    }

    fn run<D: Db>(mut db: D, request_queue: Arc<Mutex<VecDeque<(DbOp, oneshot::Sender<DbOpResponse>)>>>) {
        loop {
            db.init().expect("Failed to initialize db");

            let next = request_queue.blocking_lock().pop_front();
            let Some((op, cb)) = next else {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            };

            match op {

            }
        }
    }


}

impl Drop for AsyncDb {
    fn drop(&mut self) {
        todo!()
    }
}

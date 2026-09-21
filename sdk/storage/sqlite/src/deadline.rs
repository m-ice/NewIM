use rusqlite::Connection;
use std::{sync::mpsc, thread, time::Duration};
/// Bounds SQLite work, including corrupt-file checks; not an OS I/O deadline.
pub(crate) struct Deadline {
    done: Option<mpsc::Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Deadline {
    pub(crate) fn new(conn: &Connection, millis: u64) -> Self {
        let handle = conn.get_interrupt_handle();
        let (done, wait) = mpsc::channel();
        let worker = thread::spawn(move || {
            if wait.recv_timeout(Duration::from_millis(millis)).is_err() {
                handle.interrupt();
            }
        });
        Self {
            done: Some(done),
            worker: Some(worker),
        }
    }
}
impl Drop for Deadline {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

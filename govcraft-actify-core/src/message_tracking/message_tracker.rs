use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
// use govcraft_actify_core::prelude::*;
pub struct MessageTracker {
    counter: AtomicUsize,
    notify: Arc<Notify>,
}

impl MessageTracker {
    async fn track_start(&self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }

    async fn track_end(&self) {
        let new_count = self.counter.fetch_sub(1, Ordering::SeqCst) - 1;
        if new_count == 0 {
            self.notify.notify_one();
        }
    }
}
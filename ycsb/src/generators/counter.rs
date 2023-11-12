use super::{Generator, NumberGenerator};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Counter {
    counter: AtomicU64,
}

impl Counter {
    pub fn new(start: u64) -> Self {
        Counter {
            counter: AtomicU64::new(start),
        }
    }
}

impl NumberGenerator for Counter {}
impl Generator<u64> for Counter {
    // fn last(&self) -> u64 { self.counter.load(Ordering::Relaxed) - 1 }

    fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

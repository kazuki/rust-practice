use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use serde::Deserialize;
use toml::Table;

use crate::db::DB;
use crate::workloads::Workload;

pub struct Client<T: DB, U: Workload> {
    props: ClientProperties,

    db: T,
    workload: Arc<U>,

    thread_index: u32,
    thread_count: u32,

    progress: Arc<AtomicU64>,
}

impl<T: DB, U: Workload> Client<T, U> {
    pub fn new(
        client_props: ClientProperties,
        props: Table,
        workload: Arc<U>,
        thread_index: u32,
        thread_count: u32,
        progress: Arc<AtomicU64>,
    ) -> Self {
        Client {
            props: client_props.clone(),
            db: T::new(props),
            workload,
            thread_index,
            thread_count,
            progress,
        }
    }

    pub fn init_workload(&mut self) {
        self.workload.init(self.thread_index, self.thread_count);
    }

    pub fn init_database(&mut self) {}

    pub fn setup_initial_data(&mut self) {
        let count = (self.props.record_count / (self.thread_count as u64))
            + (if (self.thread_index as u64) < self.props.record_count % (self.thread_count as u64)
            {
                1
            } else {
                0
            });
        for _ in 0..count {
            self.workload.do_insert(&mut self.db);
            self.progress.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn benchmark(&mut self) {
        let count = (self.props.operation_count / (self.thread_count as u64))
            + (if (self.thread_index as u64)
                < self.props.operation_count % (self.thread_count as u64)
            {
                1
            } else {
                0
            });
        for _ in 0..count {
            self.progress.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct ClientProperties {
    #[serde(rename = "operationcount", default = "default_operation_count")]
    pub operation_count: u64,

    #[serde(rename = "recordcount", default = "default_record_count")]
    pub record_count: u64,
}

impl ClientProperties {
    pub fn parse(props: Table) -> Result<Self, toml::de::Error> {
        props.try_into()
    }
}

fn default_operation_count() -> u64 {
    0
}
fn default_record_count() -> u64 {
    0
}

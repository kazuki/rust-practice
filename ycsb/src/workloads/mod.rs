use toml::Table;

use crate::{client::ClientProperties, CoreProperties};

mod core;
use crate::db::DB;
pub use core::CoreWorkload;

pub trait Workload: 'static + std::marker::Send + std::marker::Sync {
    fn new(core_props: &CoreProperties, client_props: &ClientProperties, props: &Table) -> Self;
    fn init(&self, thread_idx: u32, thread_count: u32);

    fn do_insert<T: DB>(&self, db: &mut T);
}

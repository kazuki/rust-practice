use toml::Table;

mod std_btree;
pub use std_btree::{StdBTreeMapMutex, StdBTreeMapRwLock};

pub type ValueListType = Vec<(String, Vec<u8>)>;

pub trait DB {
    fn new(props: Table) -> Self;

    fn insert(&mut self, table: &str, key: String, values: ValueListType) -> Result<(), ()>;
}

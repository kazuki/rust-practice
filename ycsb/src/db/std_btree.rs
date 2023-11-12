use std::collections::{BTreeMap, HashMap};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};

use once_cell::sync::Lazy;
use toml::Table;

use crate::db::DB;

type RowValueType = Vec<Option<Vec<u8>>>;
type ColumnMappingType = (HashMap<String, usize>, Vec<String>);

static COLUMN_MAPPING_LEN: Lazy<Arc<AtomicUsize>> = Lazy::new(|| Arc::new(AtomicUsize::new(0)));
static COLUMN_MAPPING: Lazy<Arc<RwLock<ColumnMappingType>>> =
    Lazy::new(|| Arc::new(RwLock::new((HashMap::new(), Vec::new()))));

static MUTEX_INSTANCE: Lazy<Arc<Mutex<BTreeMap<String, RowValueType>>>> =
    Lazy::new(|| Arc::new(Mutex::new(BTreeMap::new())));

static RWLOCK_INSTANCE: Lazy<Arc<RwLock<BTreeMap<String, RowValueType>>>> =
    Lazy::new(|| Arc::new(RwLock::new(BTreeMap::new())));

pub struct StdBTreeMapMutex {
    columns_cache: ColumnMappingType,
    n_columns: Arc<AtomicUsize>,
    columns: Arc<RwLock<ColumnMappingType>>,
    db: Arc<Mutex<BTreeMap<String, RowValueType>>>,
}

pub struct StdBTreeMapRwLock {}

fn get_key_indices(
    columns: &Arc<RwLock<ColumnMappingType>>,
    cache: &mut ColumnMappingType,
    values: &Vec<(String, Vec<u8>)>,
) -> (Vec<isize>, usize) {
    let mut key_to_idx: Vec<isize> = Vec::with_capacity(values.len());
    let mut max_idx = 0usize;
    {
        let x = columns.read().unwrap();
        for (k, v) in values {
            match x.0.get(k) {
                Some(i) => {
                    key_to_idx.push(*i as isize);
                    max_idx = std::cmp::max(max_idx, *i);
                }
                _ => {
                    key_to_idx.push(-1);
                }
            }
        }
    }
    {
        let mut x = columns.write().unwrap();
        for i in 0..values.len() {
            if key_to_idx[i] >= 0 {
                continue;
            }
            let new_idx = x.1.len();
            x.1.push(values[i].0.clone());
            x.0.insert(values[i].0.clone(), new_idx);
            key_to_idx[i] = new_idx as isize;
            max_idx = new_idx;
        }
    }
    return (key_to_idx, max_idx + 1);
}

fn convert_to_row(
    n_columns: &Arc<AtomicUsize>,
    columns: &Arc<RwLock<ColumnMappingType>>,
    cache: &mut ColumnMappingType,
    values: &mut Vec<(String, Vec<u8>)>,
) -> RowValueType {
    let mut row = RowValueType::with_capacity(std::cmp::max(cache.1.len(), values.len()));
    row.resize(row.capacity(), None);

    if cache.1.len() != n_columns.load(Ordering::Relaxed) {
        let x = columns.read().unwrap();
        for i in cache.1.len()..x.0.len() {
            cache.1.push(x.1[i].clone());
            cache.0.insert(x.1[i].clone(), x.0[&x.1[i]]);
        }
    }

    while !values.is_empty() {
        if let Some(idx) = cache.0.get(&values[0].0) {
            if row.len() <= *idx {
                row.resize(idx + 1, None);
            }
            row[*idx] = Some(values.swap_remove(0).1);
        } else {
            let mut x = columns.write().unwrap();
            let new_idx = x.1.len();
            x.1.push(values[0].0.clone());
            x.0.insert(values[0].0.clone(), new_idx);
            cache.1.push(values[0].0.clone());
            cache.0.insert(values[0].0.clone(), new_idx);
            n_columns.fetch_add(0, Ordering::Relaxed);
            if row.len() <= new_idx {
                row.resize(new_idx + 1, None);
            }
            row[new_idx] = Some(values.swap_remove(0).1);
        }
    }

    return row;
}

impl DB for StdBTreeMapMutex {
    fn new(props: Table) -> Self {
        StdBTreeMapMutex {
            columns_cache: (HashMap::new(), Vec::new()),
            n_columns: COLUMN_MAPPING_LEN.clone(),
            columns: COLUMN_MAPPING.clone(),
            db: MUTEX_INSTANCE.clone(),
        }
    }

    fn insert(
        &mut self,
        _: &str,
        key: String,
        mut values: Vec<(String, Vec<u8>)>,
    ) -> Result<(), ()> {
        let row = convert_to_row(
            &self.n_columns,
            &self.columns,
            &mut self.columns_cache,
            &mut values,
        );
        {
            let mut x = self.db.lock().unwrap();
            x.insert(key, row);
        }
        Ok(())
    }
}

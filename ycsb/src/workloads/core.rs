use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;

use rand::{thread_rng, Rng, RngCore};
use serde::Deserialize;
use toml::Table;

use crate::db::{ValueListType, DB};
use crate::generators::{self, Generator, NumberGenerator};
use crate::workloads::Workload;
use crate::{client::ClientProperties, CoreProperties};

pub struct CoreWorkload {
    client_props: ClientProperties,
    props: Properties,

    key_sequence: generators::Counter,
    ordered_inserts: bool,

    field_length_generator: Box<dyn NumberGenerator>,
    field_names: Vec<String>,
}

impl Workload for CoreWorkload {
    fn new(core_props: &CoreProperties, client_props: &ClientProperties, props: &Table) -> Self {
        let props: Properties = props.clone().try_into().unwrap();
        let key_sequence = generators::Counter::new(props.insert_start);
        let ordered_inserts = props.insert_order != "hashed";
        let mut field_names = Vec::with_capacity(props.field_count as usize);
        for i in 0..props.field_count {
            let mut n = props.field_name_prefix.clone();
            n.push_str(&i.to_string());
            field_names.push(n);
        }
        let field_length_generator = {
            match &*props.field_length_distribution {
                "constant" => Box::new(generators::Constant::new(props.field_length as u64)),
                _ => {
                    panic!("invalid fieldlengthdistribution");
                }
            }
        };
        let mut ret = CoreWorkload {
            client_props: client_props.clone(),
            props,
            key_sequence,
            ordered_inserts,
            field_length_generator,
            field_names,
        };
        ret.init_internal();
        return ret;
    }

    fn init(&self, thread_idx: u32, thread_count: u32) {}

    fn do_insert<T: DB>(&self, db: &mut T) {
        let key = self.build_key(self.key_sequence.next());
        let values = self.build_values(&key);
        let _ = db.insert(&self.props.table, key, values);
    }
}

impl CoreWorkload {
    fn init_internal(&mut self) {}

    fn build_key(&self, mut n: u64) -> String {
        let keynum = {
            if !self.ordered_inserts {
                let mut hasher = DefaultHasher::new();
                hasher.write_u64(n);
                n = hasher.finish();
            }
            n.to_string()
        };
        let fill = (self.props.zero_padding as i32) - (keynum.len() as i32);
        let mut key = "user".to_string();
        for _ in 0..fill {
            key.push('0');
        }
        key.push_str(&keynum);
        return key;
    }

    fn build_values(&self, key: &str) -> ValueListType {
        let mut ret = ValueListType::with_capacity(self.field_names.len());
        let mut rng = thread_rng();
        for i in 0..self.field_names.len() {
            let mut v = Vec::<u8>::with_capacity(self.field_length_generator.next() as usize);
            unsafe {
                v.set_len(v.capacity());
            }
            rng.fill_bytes(&mut v);
            ret.push((self.field_names[i].clone(), v));
        }
        return ret;
    }
}

#[derive(Deserialize, Debug)]
struct Properties {
    #[serde(default = "default_table")]
    table: String,

    #[serde(rename = "fieldcount", default = "default_field_count")]
    field_count: u32,

    #[serde(rename = "fieldnameprefix", default = "default_field_name_prefix")]
    field_name_prefix: String,

    #[serde(rename = "fieldlength", default = "default_field_length")]
    field_length: u32,

    #[serde(rename = "minfieldlength", default = "default_field_length_min")]
    field_length_min: u32,

    #[serde(
        rename = "fieldlengthdistribution",
        default = "default_field_length_distribution"
    )]
    field_length_distribution: String,

    #[serde(
        rename = "requestdistribution",
        default = "default_request_distribution"
    )]
    request_distribution: String,

    #[serde(rename = "minscanlength", default = "default_min_scan_length")]
    min_scan_length: u32,

    #[serde(rename = "maxscanlength", default = "default_max_scan_length")]
    max_scan_length: u32,

    #[serde(
        rename = "scanlengthdistribution",
        default = "default_scan_length_distribution"
    )]
    scan_length_distribution: String,

    #[serde(rename = "insertstart", default = "default_insert_start")]
    insert_start: u64,

    #[serde(rename = "insertcount")]
    insert_count: Option<u64>,

    #[serde(rename = "zeropadding", default = "default_zero_padding")]
    zero_padding: u32,

    #[serde(rename = "readallfields", default = "default_read_all_fields")]
    read_all_fields: bool,

    #[serde(
        rename = "readallfieldsbyname",
        default = "default_read_all_fields_by_name"
    )]
    read_all_fields_by_name: bool,

    #[serde(rename = "writeallfields", default = "default_write_all_fields")]
    write_all_fields: bool,

    #[serde(rename = "insertorder", default = "default_insert_order")]
    insert_order: String,

    #[serde(rename = "insertproportion", default = "default_insert_proportion")]
    insert_proportion: f64,
}

fn default_table() -> String {
    "usertable".to_string()
}
fn default_field_count() -> u32 {
    10
}
fn default_field_name_prefix() -> String {
    "field".to_string()
}
fn default_field_length() -> u32 {
    100
}
fn default_field_length_min() -> u32 {
    1
}
fn default_field_length_distribution() -> String {
    "constant".to_string()
}
fn default_request_distribution() -> String {
    "uniform".to_string()
}
fn default_min_scan_length() -> u32 {
    1
}
fn default_max_scan_length() -> u32 {
    1000
}
fn default_scan_length_distribution() -> String {
    "uniform".to_string()
}
fn default_insert_start() -> u64 {
    0
}
fn default_zero_padding() -> u32 {
    1
}
fn default_read_all_fields() -> bool {
    true
}
fn default_read_all_fields_by_name() -> bool {
    false
}
fn default_write_all_fields() -> bool {
    false
}
fn default_insert_order() -> String {
    "hashed".to_string()
}
fn default_insert_proportion() -> f64 {
    0.0
}

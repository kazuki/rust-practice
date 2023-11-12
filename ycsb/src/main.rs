mod client;
mod db;
mod generators;
mod workloads;

use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{
    borrow::BorrowMut,
    ops::{Add, AddAssign},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier, Mutex,
    },
};

use client::{Client, ClientProperties};
use db::{StdBTreeMapMutex, StdBTreeMapRwLock, DB};
use workloads::Workload;

use serde::Deserialize;
use toml::Table;

fn main() -> Result<(), anyhow::Error> {
    let args = parse_args();
    let core_props: CoreProperties = args.clone().try_into()?;
    let client_props = ClientProperties::parse(args.clone())?;

    let state = match &*core_props.workload {
        "core" => init_clients::<workloads::CoreWorkload>(&core_props, &client_props, &args),
        _ => {
            panic!("invalid workload");
        }
    };
    let show_progress = |count: u64| {
        let mut prev_percentage = 0u64;
        loop {
            thread::sleep(std::time::Duration::from_millis(50));
            let progress: u64 = state
                .clients
                .iter()
                .map(|x| x.progress.load(Ordering::Relaxed))
                .sum();
            let percentage = progress * 100 / count;
            if prev_percentage != percentage {
                println!("  {}% ({}/{})", percentage, progress, count);
                prev_percentage = percentage;
            }
            if progress == count {
                break;
            }
        }
    };

    println!("initializing...");
    state.barrier.wait();
    let start_time = Instant::now();

    println!("setup initial data...");
    show_progress(client_props.record_count);
    state.barrier.wait();
    let end_time: Instant = state
        .clients
        .iter()
        .map(|x| *x.insert_end_time.lock().unwrap())
        .max()
        .unwrap();
    let insert_time = (end_time - start_time).as_secs_f64();
    println!(
        "{:.2} s ({:.2} ops)",
        insert_time,
        (client_props.record_count as f64) / insert_time
    );

    println!("START");
    show_progress(client_props.operation_count);
    state.barrier.wait();

    for client_handle in state.clients {
        client_handle.join_handle.join().unwrap();
    }

    Ok(())
}

struct State {
    barrier: Arc<Barrier>,
    clients: Vec<ClientHandle>,
}

struct ClientHandle {
    progress: Arc<AtomicU64>,
    insert_end_time: Arc<Mutex<Instant>>,
    benchmark_end_time: Arc<Mutex<Instant>>,
    join_handle: JoinHandle<()>,
}

fn init_clients<U: Workload>(
    core_props: &CoreProperties,
    client_props: &ClientProperties,
    props: &Table,
) -> State {
    let workload = U::new(&core_props, &client_props, &props);
    match &*core_props.db {
        "std_btreemap_mutex" => init_clients_internal::<StdBTreeMapMutex, U>(
            &core_props,
            &client_props,
            &props,
            workload,
        ),
        _ => {
            panic!("invalid db");
        }
    }
}

fn init_clients_internal<T: DB, U: Workload>(
    core_props: &CoreProperties,
    client_props: &ClientProperties,
    props: &Table,
    workload: U,
) -> State {
    let mut clients = Vec::with_capacity(core_props.thread_count as usize);
    let workload = Arc::new(workload);
    let barrier = Arc::new(Barrier::new(core_props.thread_count as usize + 1));
    for i in 0..core_props.thread_count {
        let thread_index = i;
        let thread_count = core_props.thread_count;
        let client_props = client_props.clone();
        let props = props.clone();
        let workload = workload.clone();
        let barrier = barrier.clone();
        let progress = Arc::new(AtomicU64::new(0));
        let progress_client = progress.clone();
        let insert_end_time = Arc::new(Mutex::new(Instant::now()));
        let benchmark_end_time = Arc::new(Mutex::new(Instant::now()));
        let insert_end_time_client = insert_end_time.clone();
        let benchmark_end_time_client = benchmark_end_time.clone();
        let join_handle = thread::spawn(move || {
            let mut client = Client::<T, U>::new(
                client_props,
                props,
                workload,
                thread_index,
                thread_count,
                progress_client.clone(),
            );

            client.init_workload();
            client.init_database();
            barrier.wait();

            client.setup_initial_data();
            {
                let mut x = insert_end_time_client.lock().unwrap();
                *x = Instant::now();
            }
            barrier.wait();
            progress_client.store(0, Ordering::Release);

            client.benchmark();
            {
                let mut x = benchmark_end_time_client.lock().unwrap();
                *x = Instant::now();
            }
            barrier.wait();
        });
        clients.push(ClientHandle {
            progress,
            insert_end_time,
            benchmark_end_time,
            join_handle,
        });
    }
    State { barrier, clients }
}

fn parse_args() -> Table {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    args.reverse();

    let mut overwrites: Vec<(String, String)> = Vec::new();
    let mut ret = Table::new();

    while !args.is_empty() {
        let k = args.pop().unwrap();
        match &*k {
            "-P" => {
                let path = args.pop().unwrap();
                let toml_text = std::fs::read_to_string(path).unwrap();
                let tbl = toml_text.parse::<Table>().unwrap();
                ret.extend(tbl);
            }
            "-p" => {
                let kv = args.pop().unwrap();
                let (k, v) = kv.split_once('=').unwrap();
                overwrites.push((k.to_string(), v.to_string()));
            }
            _ => {}
        }
    }

    for (k, v) in overwrites {
        ret.insert(k, {
            if let Ok(t0) = v.parse::<i64>() {
                toml::Value::from(t0)
            } else if let Ok(t1) = v.parse::<f64>() {
                toml::Value::from(t1)
            } else if let Ok(t2) = v.to_ascii_lowercase().parse::<bool>() {
                toml::Value::from(t2)
            } else {
                toml::Value::from(v.clone())
            }
        });
    }

    return ret;
}

#[derive(Deserialize, Clone, Debug)]
pub struct CoreProperties {
    pub workload: String,

    #[serde(default = "default_db")]
    pub db: String,

    #[serde(rename = "maxexecutiontime", default = "default_max_execution_time")]
    pub max_execution_time: u32,

    #[serde(rename = "threadcount", default = "default_thread_count")]
    pub thread_count: u32,

    #[serde(rename = "target", default = "default_target")]
    pub target: u32,
}

fn default_db() -> String {
    "std_btreemap_mutex".to_string()
}
fn default_max_execution_time() -> u32 {
    0
}
fn default_thread_count() -> u32 {
    1
}
fn default_target() -> u32 {
    0
}

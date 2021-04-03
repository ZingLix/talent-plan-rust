#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::{KvStore, KvsEngine, SledServer};
use rand::Rng;
use sled::Db;
use std::iter;
use tempfile::TempDir;

fn generate_key_list() -> Vec<String> {
    let mut list = Vec::new();
    for _ in 1..100 {
        list.push(random_string());
    }
    list
}

fn random_string() -> String {
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(10, 10000);
    rng.sample_iter(&rand::distributions::Alphanumeric)
        .take(len)
        .collect::<String>()
}

fn set_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let key_list = generate_key_list();
                    (KvStore::open(temp_dir.path()).unwrap(), key_list)
                },
                |(mut store, key_list)| {
                    for key in key_list {
                        store.set(format!("{}", key), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("sled", |b, _| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (
                    SledServer::new(Db::start_default(&temp_dir).unwrap()),
                    generate_key_list(),
                )
            },
            |(mut db, key_list)| {
                for key in key_list {
                    db.set(format!("key{}", key), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    c.bench("set_bench", bench);
}

fn get_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let key_list = generate_key_list();
                    let mut store = KvStore::open(temp_dir.path()).unwrap();
                    for key in &key_list {
                        store.set(format!("{}", key), "value".to_string()).unwrap();
                    }
                    (store, key_list)
                },
                |(mut store, key_list)| {
                    for key in key_list {
                        store.get(format!("{}", key)).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("sled", |b, _| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();

                let mut sled = SledServer::new(Db::start_default(&temp_dir).unwrap());
                let key_list = generate_key_list();
                for key in &key_list {
                    sled.set(format!("{}", key), "value".to_string()).unwrap();
                }
                (sled, key_list)
            },
            |(mut db, key_list)| {
                for key in key_list {
                    db.get(format!("{}", key)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    c.bench("get_bench", bench);
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);

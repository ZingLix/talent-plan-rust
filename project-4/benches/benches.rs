#[macro_use]
extern crate criterion;

use criterion::Criterion;
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::KvsServer;
use kvs::{KvStore, SledServer};

use tempfile::TempDir;

fn write_sq_kv(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let server = KvsServer::new(
        KvStore::open(temp_dir.path().join("kvs")).unwrap(),
        SharedQueueThreadPool::new(0).unwrap(),
    );

    c.bench_function("write_sq_kv", move |b| {
        b.iter(|| {
            for key in 1..100 {
                server.set(format!("key{}", key), "value".to_owned());
            }
        })
    });
}

fn read_sq_kv(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let server = KvsServer::new(
        KvStore::open(temp_dir.path().join("kvs")).unwrap(),
        SharedQueueThreadPool::new(0).unwrap(),
    );

    for key in 1..100 {
        server.set(format!("key{}", key), "value".to_owned());
    }
    c.bench_function("read_sq_kv", move |b| {
        b.iter(|| {
            for key in 1..100 {
                server.get(format!("key{}", key));
            }
        })
    });
}

fn write_ry_kv(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let server = KvsServer::new(
        KvStore::open(temp_dir.path().join("kvs")).unwrap(),
        RayonThreadPool::new(0).unwrap(),
    );

    c.bench_function("write_ry_kv", move |b| {
        b.iter(|| {
            for key in 1..100 {
                server.set(format!("key{}", key), "value".to_owned());
            }
        })
    });
}

fn read_ry_kv(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let server = KvsServer::new(
        KvStore::open(temp_dir.path().join("kvs")).unwrap(),
        RayonThreadPool::new(0).unwrap(),
    );

    for key in 1..100 {
        server.set(format!("key{}", key), "value".to_owned());
    }
    std::thread::sleep(std::time::Duration::from_secs(1));
    c.bench_function("read_sq_kv", move |b| {
        b.iter(|| {
            for key in 1..100 {
                server.get(format!("key{}", key));
            }
        })
    });
}
fn write_ry_sled(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let server = KvsServer::new(
        SledServer::new(sled::Db::start_default(temp_dir.path().join("sled")).unwrap()),
        SharedQueueThreadPool::new(0).unwrap(),
    );

    c.bench_function("write_ry_sled", move |b| {
        b.iter(|| {
            for key in 1..100 {
                server.set(format!("key{}", key), "value".to_owned());
            }
        })
    });
}

fn read_ry_sled(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let server = KvsServer::new(
        SledServer::new(sled::Db::start_default(temp_dir.path().join("sled")).unwrap()),
        RayonThreadPool::new(0).unwrap(),
    );

    for key in 1..100 {
        server.set(format!("key{}", key), "value".to_owned());
    }
    c.bench_function("read_ry_sled", move |b| {
        b.iter(|| {
            for key in 1..100 {
                server.get(format!("key{}", key));
            }
        })
    });
}
// criterion_group!(benches, write_sq_kv, write_ry_kv);
// criterion_group!(benches1, read_sq_kv, read_ry_kv);

// criterion_group!(benches2, write_ry_kv, write_ry_sled);

// criterion_group!(benches3, read_ry_kv, read_ry_sled);
// criterion_main!(benches2, benches3, benches2, benches3);

criterion_group!(
    benches,
    write_sq_kv,
    read_sq_kv,
    write_ry_kv,
    read_ry_kv,
    write_ry_sled,
    read_ry_sled
);

criterion_main!(benches);

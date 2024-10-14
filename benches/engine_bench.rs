use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use sled;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    // group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(10);
    group.bench_function("kvs", |bencher| {
        bencher.iter_batched(
            || {
                println!("kvs set setup");
                let temp_dir = TempDir::new().unwrap();
                (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(store, _temp_dir)| {
                println!("kvs set");
                for i in 1..(1 << 8) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |bencher| {
        bencher.iter_batched(
            || {
                println!("sled set setup");
                let temp_dir = TempDir::new().unwrap();
                (SledKvsEngine::new(sled::open(&temp_dir).unwrap()), temp_dir)
            },
            |(db, _temp_dir)| {
                println!("sled set");
                for i in 1..(1 << 8) {
                    db.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    // group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(10);
    // for i in &vec![8, 12, 16, 20]
    for i in &vec![8, 12] {
        group.bench_with_input(format!("kvs_{}", i), i, |bencher, i| {
            let temp_dir = TempDir::new().unwrap();
            let store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            bencher.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1..1 << i)))
                    .unwrap();
            })
        });
    }
    // for i in &vec![8, 12, 16, 20]
    for i in &vec![8, 12] {
        group.bench_with_input(format!("sled_{}", i), i, |bencher, i| {
            let temp_dir = TempDir::new().unwrap();
            let db = SledKvsEngine::new(sled::open(&temp_dir).unwrap());
            for key_i in 1..(1 << i) {
                db.set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            bencher.iter(|| {
                db.get(format!("key{}", rng.gen_range(1..1 << i))).unwrap();
            })
        });
    }
    group.finish();
}

// 这里的 benches 是函数名
// 不强制要求一定是 "benches"， 可以自己随便写
// 只要 criterion_group 这里定义的名字和下面 criterion_main 里面用的名字一致即可
criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);

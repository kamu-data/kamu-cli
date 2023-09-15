// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use rand::{Rng, SeedableRng};

///////////////////////////////////////////////////////////////////////////////

async fn setup(
    tempdir: &Path,
    orig_rows: usize,
    changed_rows: usize,
    added_rows: usize,
) -> (String, String) {
    use datafusion::arrow::array;
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    let ctx = SessionContext::new();

    let prev = tempdir.join("prev").to_str().unwrap().to_string();
    let new = tempdir.join("new").to_str().unwrap().to_string();

    let mut offset = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows);
    let mut observed = array::StringBuilder::new();
    let mut pk1 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows);
    let mut pk2 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows);
    let mut cmp1 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows);
    let mut cmp2 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows);
    let mut aux1 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows);
    let mut aux2 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows);

    let mut new_pk1 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows + added_rows);
    let mut new_pk2 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows + added_rows);
    let mut new_cmp1 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows + added_rows);
    let mut new_cmp2 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows + added_rows);
    let mut new_aux1 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows + added_rows);
    let mut new_aux2 = array::PrimitiveBuilder::<Int64Type>::with_capacity(orig_rows + added_rows);

    for i in 0..orig_rows {
        offset.append_value(i as i64);
        observed.append_value("I");
    }

    let mut buf = Vec::with_capacity(orig_rows + added_rows);
    buf.resize(orig_rows + added_rows, 0);

    let mut rng = rand::rngs::SmallRng::seed_from_u64(123127986998);

    rng.try_fill(&mut buf[..]).unwrap();
    pk1.append_slice(&buf[..orig_rows]);
    new_pk1.append_slice(&buf[..]);

    rng.try_fill(&mut buf[..]).unwrap();
    pk2.append_slice(&buf[..orig_rows]);
    new_pk2.append_slice(&buf[..]);

    rng.try_fill(&mut buf[..]).unwrap();
    cmp1.append_slice(&buf[..orig_rows]);
    rng.try_fill(&mut buf[..changed_rows]).unwrap();
    new_cmp1.append_slice(&buf[..]);

    rng.try_fill(&mut buf[..]).unwrap();
    cmp2.append_slice(&buf[..orig_rows]);
    rng.try_fill(&mut buf[..changed_rows]).unwrap();
    new_cmp2.append_slice(&buf[..]);

    rng.try_fill(&mut buf[..]).unwrap();
    aux1.append_slice(&buf[..orig_rows]);
    new_aux1.append_slice(&buf[..]);

    rng.try_fill(&mut buf[..]).unwrap();
    aux2.append_slice(&buf[..orig_rows]);
    new_aux2.append_slice(&buf[..]);

    ctx.read_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("offset", DataType::Int64, false),
                Field::new("observed", DataType::Utf8, false),
                Field::new("pk1", DataType::Int64, false),
                Field::new("pk2", DataType::Int64, false),
                Field::new("cmp1", DataType::Int64, false),
                Field::new("cmp2", DataType::Int64, false),
                Field::new("aux1", DataType::Int64, false),
                Field::new("aux2", DataType::Int64, false),
            ])),
            vec![
                Arc::new(offset.finish()),
                Arc::new(observed.finish()),
                Arc::new(pk1.finish()),
                Arc::new(pk2.finish()),
                Arc::new(cmp1.finish()),
                Arc::new(cmp2.finish()),
                Arc::new(aux1.finish()),
                Arc::new(aux2.finish()),
            ],
        )
        .unwrap(),
    )
    .unwrap()
    .write_parquet(&prev, DataFrameWriteOptions::default(), None)
    .await
    .unwrap();

    ctx.read_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("pk1", DataType::Int64, false),
                Field::new("pk2", DataType::Int64, false),
                Field::new("cmp1", DataType::Int64, false),
                Field::new("cmp2", DataType::Int64, false),
                Field::new("aux1", DataType::Int64, false),
                Field::new("aux2", DataType::Int64, false),
            ])),
            vec![
                Arc::new(new_pk1.finish()),
                Arc::new(new_pk2.finish()),
                Arc::new(new_cmp1.finish()),
                Arc::new(new_cmp2.finish()),
                Arc::new(new_aux1.finish()),
                Arc::new(new_aux2.finish()),
            ],
        )
        .unwrap(),
    )
    .unwrap()
    .write_parquet(&new, DataFrameWriteOptions::default(), None)
    .await
    .unwrap();

    (prev, new)
}

///////////////////////////////////////////////////////////////////////////////

async fn merge_snapshot(prev_path: &str, new_path: &str, expected_rows: usize) {
    use kamu_ingest_datafusion::*;

    let ctx = SessionContext::with_config(SessionConfig::new());

    ctx.register_parquet("prev", prev_path, ParquetReadOptions::default())
        .await
        .unwrap();

    ctx.register_parquet("new", new_path, ParquetReadOptions::default())
        .await
        .unwrap();

    let prev = ctx.table("prev").await.unwrap();
    let new = ctx.table("new").await.unwrap();

    let res = MergeStrategySnapshot::new(
        "offset".to_string(),
        opendatafabric::MergeStrategySnapshot {
            primary_key: vec!["pk1".to_string(), "pk2".to_string()],
            compare_columns: Some(vec!["cmp1".to_string(), "cmp2".to_string()]),
            observation_column: None,
            obsv_added: None,
            obsv_changed: None,
            obsv_removed: None,
        },
    )
    .merge(Some(prev), new)
    .unwrap();

    let res = res.cache().await.unwrap();

    assert_eq!(res.count().await.unwrap(), expected_rows);
}

///////////////////////////////////////////////////////////////////////////////

fn bench(c: &mut Criterion) {
    let orig_rows = 1000_000;
    let changed_rows = 200_000;
    let added_rows = 100_000;

    let tempdir = tempfile::tempdir().unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();

    let (prev, new) = rt.block_on(setup(tempdir.path(), orig_rows, changed_rows, added_rows));

    let mut group = c.benchmark_group("merge");
    group.sample_size(10);
    group.bench_function("snapshot", |b| {
        b.iter(|| rt.block_on(merge_snapshot(&prev, &new, changed_rows + added_rows)))
    });
}

///////////////////////////////////////////////////////////////////////////////

criterion_group!(benches, bench);
criterion_main!(benches);

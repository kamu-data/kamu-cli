// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{TimeZone, Utc};
use dill::Catalog;
use kamu_datasets::*;

use crate::helpers::{init_dataset_entry, init_test_account};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_and_get_statistics(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let stats_repo = catalog
        .get_one::<dyn DatasetStatisticsRepository>()
        .unwrap();

    let has_result = stats_repo.has_any_stats().await;
    assert_matches!(has_result, Ok(false));

    let get_result = stats_repo
        .get_dataset_statistics(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_result, Err(GetDatasetStatisticsError::NotFound(e))
        if e.dataset_id == dataset_id && e.block_ref == odf::BlockRef::Head);

    let statistics = DatasetStatistics {
        last_pulled: Some(Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap()),
        num_records: 42,
        data_size: 1234,
        checkpoints_size: 4321,
    };

    let set_result = stats_repo
        .set_dataset_statistics(&dataset_id, &odf::BlockRef::Head, statistics)
        .await;
    assert_matches!(set_result, Ok(()));

    let has_result = stats_repo.has_any_stats().await;
    assert_matches!(has_result, Ok(true));

    let get_result = stats_repo
        .get_dataset_statistics(&dataset_id, &odf::BlockRef::Head)
        .await;
    println!("{statistics:?}");
    assert_matches!(get_result, Ok(s) if s == statistics);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_overwrite_statistics(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let stats_repo = catalog
        .get_one::<dyn DatasetStatisticsRepository>()
        .unwrap();

    let statistics_1 = DatasetStatistics {
        last_pulled: Some(Utc.timestamp_opt(1000, 0).unwrap()),
        num_records: 10,
        data_size: 100,
        checkpoints_size: 200,
    };

    let statistics_2 = DatasetStatistics {
        last_pulled: Some(Utc.timestamp_opt(2000, 0).unwrap()),
        num_records: 20,
        data_size: 200,
        checkpoints_size: 400,
    };

    stats_repo
        .set_dataset_statistics(&dataset_id, &odf::BlockRef::Head, statistics_1)
        .await
        .unwrap();

    stats_repo
        .set_dataset_statistics(&dataset_id, &odf::BlockRef::Head, statistics_2)
        .await
        .unwrap();

    let get_result = stats_repo
        .get_dataset_statistics(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_result, Ok(s) if s == statistics_2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_multiple_datasets_statistics(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_1_id = odf::DatasetID::new_seeded_ed25519(b"some-id-1");
    let dataset_2_id = odf::DatasetID::new_seeded_ed25519(b"some-id-2");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_1_id,
        &odf::DatasetName::new_unchecked("foo"),
        odf::DatasetKind::Root,
    )
    .await;

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_2_id,
        &odf::DatasetName::new_unchecked("bar"),
        odf::DatasetKind::Root,
    )
    .await;

    let stats_repo = catalog
        .get_one::<dyn DatasetStatisticsRepository>()
        .unwrap();

    let statistics_1 = DatasetStatistics {
        last_pulled: None,
        num_records: 11,
        data_size: 101,
        checkpoints_size: 201,
    };

    let statistics_2 = DatasetStatistics {
        last_pulled: None,
        num_records: 22,
        data_size: 202,
        checkpoints_size: 402,
    };

    stats_repo
        .set_dataset_statistics(&dataset_1_id, &odf::BlockRef::Head, statistics_1)
        .await
        .unwrap();

    stats_repo
        .set_dataset_statistics(&dataset_2_id, &odf::BlockRef::Head, statistics_2)
        .await
        .unwrap();

    let get_1_result = stats_repo
        .get_dataset_statistics(&dataset_1_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_1_result, Ok(s) if s == statistics_1);

    let get_2_result = stats_repo
        .get_dataset_statistics(&dataset_2_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_2_result, Ok(s) if s == statistics_2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::{TimeZone, Utc};
use database_common::testing::{
    RacingTransactionOutcome,
    in_committed_transaction,
    race_transactions,
};
use dill::Catalog;
use kamu_datasets::*;

use crate::helpers::{
    init_committed_entries,
    init_dataset_entry,
    init_test_account,
    remove_dataset_entry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_and_get_statistics(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
        data_size_bytes: 1234,
        checkpoints_size_bytes: 4321,
        num_object_links: 11,
        object_links_size_bytes: 34,
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
    assert_matches!(get_result, Ok(s) if s == statistics);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_overwrite_statistics(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
        data_size_bytes: 100,
        checkpoints_size_bytes: 200,
        num_object_links: 20,
        object_links_size_bytes: 50,
    };

    let statistics_2 = DatasetStatistics {
        last_pulled: Some(Utc.timestamp_opt(2000, 0).unwrap()),
        num_records: 20,
        data_size_bytes: 200,
        checkpoints_size_bytes: 400,
        num_object_links: 40,
        object_links_size_bytes: 100,
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
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_1_id = odf::DatasetID::new_seeded_ed25519(b"some-id-1");
    let dataset_2_id = odf::DatasetID::new_seeded_ed25519(b"some-id-2");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_1_id,
        &odf::DatasetName::new_unchecked("foo"),
        odf::DatasetKind::Root,
    )
    .await;

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
        data_size_bytes: 101,
        checkpoints_size_bytes: 201,
        num_object_links: 21,
        object_links_size_bytes: 51,
    };

    let statistics_2 = DatasetStatistics {
        last_pulled: None,
        num_records: 22,
        data_size_bytes: 202,
        checkpoints_size_bytes: 402,
        num_object_links: 22,
        object_links_size_bytes: 52,
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

pub async fn test_remove_dataset_entry_removes_statistics(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-remove-stats");
    let dataset_name = odf::DatasetName::new_unchecked("remove-stats-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let stats_repo = catalog
        .get_one::<dyn DatasetStatisticsRepository>()
        .unwrap();

    let statistics = DatasetStatistics {
        last_pulled: Some(Utc.timestamp_opt(1000, 0).unwrap()),
        num_records: 10,
        data_size_bytes: 100,
        checkpoints_size_bytes: 200,
        ..Default::default()
    };

    // Add statistics
    stats_repo
        .set_dataset_statistics(&dataset_id, &odf::BlockRef::Head, statistics)
        .await
        .unwrap();

    // Verify statistics exist
    let has_result = stats_repo.has_any_stats().await;
    assert_matches!(has_result, Ok(true));

    // Remove dataset entry
    remove_dataset_entry(catalog, &dataset_id).await;

    // Verify statistics are removed
    let has_result = stats_repo.has_any_stats().await;
    assert_matches!(has_result, Ok(false));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_total_statistics(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_1_id = odf::DatasetID::new_seeded_ed25519(b"some-id-1");
    let dataset_2_id = odf::DatasetID::new_seeded_ed25519(b"some-id-2");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_1_id,
        &odf::DatasetName::new_unchecked("foo"),
        odf::DatasetKind::Root,
    )
    .await;

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
        data_size_bytes: 101,
        checkpoints_size_bytes: 201,
        num_object_links: 21,
        object_links_size_bytes: 51,
    };

    let statistics_2 = DatasetStatistics {
        last_pulled: None,
        num_records: 22,
        data_size_bytes: 202,
        checkpoints_size_bytes: 402,
        num_object_links: 22,
        object_links_size_bytes: 52,
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

    let total_statistic = stats_repo
        .get_total_statistic_by_account_id(&test_account_id)
        .await;

    let expected_total_statistic = TotalStatistic {
        num_records: statistics_1.num_records + statistics_2.num_records,
        data_size_bytes: statistics_1.data_size_bytes + statistics_2.data_size_bytes,
        checkpoints_size_bytes: statistics_1.checkpoints_size_bytes
            + statistics_2.checkpoints_size_bytes,
        num_object_links: statistics_1.num_object_links + statistics_2.num_object_links,
        object_links_size_bytes: statistics_1.object_links_size_bytes
            + statistics_2.object_links_size_bytes,
    };

    assert_matches!(total_statistic, Ok(s) if s == expected_total_statistic);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Two transactions set statistics of one dataset reference: both commit, and
/// the one that commits last wins, as with an upsert
pub async fn test_set_statistics_in_concurrent_transactions(catalog: &Catalog) {
    let (_, entries) = init_committed_entries(catalog, &[("foo", odf::DatasetKind::Root)]).await;
    let dataset_id = entries[0].id.clone();
    let second_dataset_id = dataset_id.clone();

    let first_statistics = DatasetStatistics {
        num_records: 1,
        ..DatasetStatistics::default()
    };
    let second_statistics = DatasetStatistics {
        num_records: 2,
        ..DatasetStatistics::default()
    };

    let outcome = race_transactions(
        catalog,
        |catalog| async move {
            set_statistics(&catalog, &dataset_id, first_statistics)
                .await
                .unwrap();
        },
        move |catalog| async move {
            set_statistics(&catalog, &second_dataset_id, second_statistics).await
        },
    )
    .await;

    assert_matches!(outcome, RacingTransactionOutcome::Committed);
    let committed_statistics = in_committed_transaction(catalog, |catalog| async move {
        catalog
            .get_one::<dyn DatasetStatisticsRepository>()
            .unwrap()
            .get_dataset_statistics(&entries[0].id, &odf::BlockRef::Head)
            .await
            .unwrap()
    })
    .await;
    pretty_assertions::assert_eq!(committed_statistics, second_statistics);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn set_statistics(
    catalog: &Catalog,
    dataset_id: &odf::DatasetID,
    statistics: DatasetStatistics,
) -> Result<(), SetDatasetStatisticsError> {
    catalog
        .get_one::<dyn DatasetStatisticsRepository>()
        .unwrap()
        .set_dataset_statistics(dataset_id, &odf::BlockRef::Head, statistics)
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

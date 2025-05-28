// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use dill::Catalog;
use messaging_outbox::{
    ConsumptionBoundaryNotFoundError,
    CreateConsumptionBoundaryError,
    DuplicateConsumptionBoundaryError,
    OutboxMessageConsumptionBoundary,
    OutboxMessageConsumptionRepository,
    OutboxMessageID,
    UpdateConsumptionBoundaryError,
};
use rand::Rng;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_CONSUMER: &str = "test-consumer";
const TEST_PRODUCER: &str = "test-producer";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_outbox_consumptions_initially(catalog: &Catalog) {
    let consumption_repo = catalog
        .get_one::<dyn OutboxMessageConsumptionRepository>()
        .unwrap();

    let boundaries: Vec<_> = read_boundaries(consumption_repo.as_ref()).await;
    assert_eq!(0, boundaries.len());

    let res = consumption_repo
        .find_consumption_boundary(TEST_CONSUMER, TEST_PRODUCER)
        .await;
    assert_matches!(res, Ok(None));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_consumption(catalog: &Catalog) {
    let consumption_repo = catalog
        .get_one::<dyn OutboxMessageConsumptionRepository>()
        .unwrap();

    // 1st create should pass
    let boundary = OutboxMessageConsumptionBoundary {
        consumer_name: TEST_CONSUMER.to_string(),
        producer_name: TEST_PRODUCER.to_string(),
        last_consumed_message_id: OutboxMessageID::new(5),
    };
    let res = consumption_repo
        .create_consumption_boundary(boundary.clone())
        .await;
    assert_matches!(res, Ok(_));

    // Try reading created record

    let boundaries: Vec<_> = read_boundaries(consumption_repo.as_ref()).await;
    assert_eq!(boundaries, vec![boundary.clone()],);

    let res = consumption_repo
        .find_consumption_boundary(TEST_CONSUMER, TEST_PRODUCER)
        .await;
    assert_matches!(res, Ok(Some(a_boundary)) if a_boundary == boundary);

    // 2nd create attempt should fail
    let res = consumption_repo
        .create_consumption_boundary(boundary.clone())
        .await;
    assert_matches!(
        res,
        Err(
            CreateConsumptionBoundaryError::DuplicateConsumptionBoundary(
                DuplicateConsumptionBoundaryError {
                    consumer_name,
                    producer_name,
                }
            )
        ) if consumer_name == TEST_CONSUMER && producer_name == TEST_PRODUCER
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_existing_consumption(catalog: &Catalog) {
    let consumption_repo = catalog
        .get_one::<dyn OutboxMessageConsumptionRepository>()
        .unwrap();

    let boundary = OutboxMessageConsumptionBoundary {
        consumer_name: TEST_CONSUMER.to_string(),
        producer_name: TEST_PRODUCER.to_string(),
        last_consumed_message_id: OutboxMessageID::new(5),
    };

    // Create
    let res = consumption_repo
        .create_consumption_boundary(boundary.clone())
        .await;
    assert_matches!(res, Ok(_));

    // Update
    let updated_boundary = OutboxMessageConsumptionBoundary {
        last_consumed_message_id: OutboxMessageID::new(15),
        ..boundary
    };
    let res = consumption_repo
        .update_consumption_boundary(updated_boundary.clone())
        .await;
    assert_matches!(res, Ok(_));

    // Read
    let boundaries: Vec<_> = read_boundaries(consumption_repo.as_ref()).await;
    assert_eq!(boundaries, vec![updated_boundary.clone()],);

    let res = consumption_repo
        .find_consumption_boundary(TEST_CONSUMER, TEST_PRODUCER)
        .await;
    assert_matches!(res, Ok(Some(a_boundary)) if a_boundary == updated_boundary);

    // 2nd update should pass
    let updated_boundary_2 = OutboxMessageConsumptionBoundary {
        last_consumed_message_id: OutboxMessageID::new(25),
        ..updated_boundary
    };
    let res = consumption_repo
        .update_consumption_boundary(updated_boundary_2.clone())
        .await;
    assert_matches!(res, Ok(_));

    // Read
    let boundaries: Vec<_> = read_boundaries(consumption_repo.as_ref()).await;
    assert_eq!(boundaries, vec![updated_boundary_2.clone()],);

    let res = consumption_repo
        .find_consumption_boundary(TEST_CONSUMER, TEST_PRODUCER)
        .await;
    assert_matches!(res, Ok(Some(a_boundary)) if a_boundary == updated_boundary_2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_cannot_update_consumption_before_creation(catalog: &Catalog) {
    let consumption_repo = catalog
        .get_one::<dyn OutboxMessageConsumptionRepository>()
        .unwrap();

    let boundary = OutboxMessageConsumptionBoundary {
        consumer_name: TEST_CONSUMER.to_string(),
        producer_name: TEST_PRODUCER.to_string(),
        last_consumed_message_id: OutboxMessageID::new(5),
    };

    // Update without create
    let updated_boundary = OutboxMessageConsumptionBoundary {
        last_consumed_message_id: OutboxMessageID::new(15),
        ..boundary
    };
    let res = consumption_repo
        .update_consumption_boundary(updated_boundary.clone())
        .await;
    assert_matches!(
        res,
        Err(
            UpdateConsumptionBoundaryError::ConsumptionBoundaryNotFound(
                ConsumptionBoundaryNotFoundError {
                    consumer_name,
                    producer_name,
                }
            )
        ) if consumer_name == TEST_CONSUMER && producer_name == TEST_PRODUCER
    );

    // Read
    let boundaries: Vec<_> = read_boundaries(consumption_repo.as_ref()).await;
    assert_eq!(boundaries.len(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_multiple_boundaries(catalog: &Catalog) {
    let consumption_repo = catalog
        .get_one::<dyn OutboxMessageConsumptionRepository>()
        .unwrap();

    let mut rng = rand::thread_rng();

    for producer_suffix in ["A", "B"] {
        for consumer_suffix in ["X", "Y"] {
            let boundary = OutboxMessageConsumptionBoundary {
                consumer_name: format!("{TEST_CONSUMER}_{consumer_suffix}"),
                producer_name: format!("{TEST_PRODUCER}_{producer_suffix}"),
                last_consumed_message_id: OutboxMessageID::new(rng.r#gen()),
            };
            let res = consumption_repo.create_consumption_boundary(boundary).await;
            assert_matches!(res, Ok(_));
        }
    }

    // Read
    let mut boundaries: Vec<_> = read_boundaries(consumption_repo.as_ref()).await;
    boundaries.sort();

    assert_eq!(
        boundaries
            .iter()
            .map(|b| (b.producer_name.as_str(), b.consumer_name.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("test-producer_A", "test-consumer_X"),
            ("test-producer_A", "test-consumer_Y"),
            ("test-producer_B", "test-consumer_X"),
            ("test-producer_B", "test-consumer_Y"),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn read_boundaries(
    outbox_message_consumption_repo: &dyn OutboxMessageConsumptionRepository,
) -> Vec<OutboxMessageConsumptionBoundary> {
    use futures::TryStreamExt;
    outbox_message_consumption_repo
        .list_consumption_boundaries()
        .try_collect()
        .await
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

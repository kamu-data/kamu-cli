// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use futures::TryStreamExt;
use kamu_flow_system::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let event_store = catalog
        .get_one::<dyn FlowConfigurationEventStore>()
        .unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(0, num_events);

    let flow_key = FlowKey::dataset(
        DatasetID::new_seeded_ed25519(b"foo"),
        DatasetFlowType::Ingest,
    );
    let events: Vec<_> = event_store
        .get_events(&flow_key, GetEventsOpts::default())
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(events, []);

    let dataset_ids: Vec<_> = event_store
        .list_all_dataset_ids()
        .await
        .try_collect()
        .await
        .unwrap();

    assert_eq!(dataset_ids, []);
}

/////////////////////////////////////////////////////////////////////////////////////////

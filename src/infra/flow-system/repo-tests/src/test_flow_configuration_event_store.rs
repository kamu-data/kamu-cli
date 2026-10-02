// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::Utc;
use dill::Catalog;
use futures::TryStreamExt;
use kamu_adapter_flow_dataset::{FlowConfigRuleCompact, ingest_dataset_binding};
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let event_store = catalog
        .get_one::<dyn FlowConfigurationEventStore>()
        .unwrap();

    let num_events = event_store.total_events_stored().await.unwrap();

    assert_eq!(0, num_events);

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let flow_binding = ingest_dataset_binding(&dataset_id);

    let events: Vec<_> = event_store
        .get_events(&flow_binding, GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(events, []);

    let dataset_bindings = event_store
        .all_bindings_for_scope(&flow_binding.scope)
        .await
        .unwrap();
    assert!(
        dataset_bindings.is_empty(),
        "Expected no bindings, found: {dataset_bindings:?}"
    );

    let all_bindings = event_store
        .stream_all_existing_flow_bindings()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(
        all_bindings.is_empty(),
        "Expected no bindings, found: {all_bindings:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_streams(catalog: &Catalog) {
    let event_store = catalog
        .get_one::<dyn FlowConfigurationEventStore>()
        .unwrap();

    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let flow_binding_1 = ingest_dataset_binding(&dataset_id_1);

    let event_1_1 = FlowConfigurationEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding_1.clone(),
        rule: FlowConfigRuleCompact::try_new(100_000, 1000)
            .unwrap()
            .into_flow_config(),
        retry_policy: None,
    };
    let event_1_2 = FlowConfigurationEventModified {
        event_time: Utc::now(),
        flow_binding: flow_binding_1.clone(),
        rule: FlowConfigRuleCompact::try_new(100_000, 1000)
            .unwrap()
            .into_flow_config(),
        retry_policy: Some(RetryPolicy {
            max_attempts: 3,
            min_delay_seconds: 10,
            backoff_type: RetryBackoffType::Linear,
        }),
    };

    event_store
        .save_events(
            &flow_binding_1,
            None,
            vec![event_1_1.clone().into(), event_1_2.clone().into()],
        )
        .await
        .unwrap();

    let num_events = event_store.total_events_stored().await.unwrap();

    assert_eq!(2, num_events);

    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");
    let flow_binding_2 = ingest_dataset_binding(&dataset_id_2);

    let event_2 = FlowConfigurationEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding_2.clone(),
        rule: FlowConfigRuleCompact::try_new(100_000, 1000)
            .unwrap()
            .into_flow_config(),
        retry_policy: None,
    };

    event_store
        .save_events(&flow_binding_2, None, vec![event_2.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.total_events_stored().await.unwrap();

    assert_eq!(3, num_events);

    let flow_binding_3 = FlowBinding::new(FLOW_TYPE_SYSTEM_GC, FlowScope::make_system_scope());
    let event_3 = FlowConfigurationEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding_3.clone(),
        rule: FlowConfigRuleCompact::try_new(100_000, 1000)
            .unwrap()
            .into_flow_config(),
        retry_policy: Some(RetryPolicy {
            max_attempts: 5,
            min_delay_seconds: 3600,
            backoff_type: RetryBackoffType::Fixed,
        }),
    };

    event_store
        .save_events(&flow_binding_3, None, vec![event_3.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.total_events_stored().await.unwrap();

    assert_eq!(4, num_events);

    let events: Vec<_> = event_store
        .get_events(&flow_binding_1, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_1_1.into(), event_1_2.into()]);

    let events: Vec<_> = event_store
        .get_events(&flow_binding_2, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_2.into()]);

    let events: Vec<_> = event_store
        .get_events(&flow_binding_3, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_3.into()]);

    let dataset_bindings = event_store
        .all_bindings_for_scope(&flow_binding_1.scope)
        .await
        .unwrap();
    assert_eq!(
        dataset_bindings,
        vec![flow_binding_1.clone()],
        "Expected only one binding for dataset 1, found: {dataset_bindings:?}"
    );

    let dataset_bindings = event_store
        .all_bindings_for_scope(&flow_binding_2.scope)
        .await
        .unwrap();
    assert_eq!(
        dataset_bindings,
        vec![flow_binding_2.clone()],
        "Expected only one binding for dataset 2, found: {dataset_bindings:?}"
    );

    let all_bindings = event_store
        .stream_all_existing_flow_bindings()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(all_bindings.len(), 3);
    assert!(all_bindings.contains(&flow_binding_1));
    assert!(all_bindings.contains(&flow_binding_2));
    assert!(all_bindings.contains(&flow_binding_3));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_with_windowing(catalog: &Catalog) {
    let event_store = catalog
        .get_one::<dyn FlowConfigurationEventStore>()
        .unwrap();

    let flow_binding = ingest_dataset_binding(&odf::DatasetID::new_seeded_ed25519(b"foo"));

    let event_1 = FlowConfigurationEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding.clone(),
        rule: FlowConfigRuleCompact::try_new(100_000, 1000)
            .unwrap()
            .into_flow_config(),
        retry_policy: None,
    };
    let event_2 = FlowConfigurationEventModified {
        event_time: Utc::now(),
        flow_binding: flow_binding.clone(),
        rule: FlowConfigRuleCompact::try_new(200_000, 2000)
            .unwrap()
            .into_flow_config(),
        retry_policy: None,
    };
    let event_3 = FlowConfigurationEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding.clone(),
        rule: FlowConfigRuleCompact::try_new(100_000, 1000)
            .unwrap()
            .into_flow_config(),
        retry_policy: None,
    };

    let latest_event_id = event_store
        .save_events(
            &flow_binding,
            None,
            vec![
                event_1.clone().into(),
                event_2.clone().into(),
                event_3.clone().into(),
            ],
        )
        .await
        .unwrap();

    // Use "from" only
    let events: Vec<_> = event_store
        .get_events(
            &flow_binding,
            GetEventsOpts {
                from: Some(EventID::new(
                    latest_event_id.into_inner() - 2, /* last 2 events */
                )),
                to: None,
            },
        )
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_2.clone().into(), event_3.into()]);

    // Use "to" only
    let events: Vec<_> = event_store
        .get_events(
            &flow_binding,
            GetEventsOpts {
                from: None,
                to: Some(EventID::new(
                    latest_event_id.into_inner() - 1, /* first 2 events */
                )),
            },
        )
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_1.into(), event_2.clone().into()]);

    // Use both "from" and "to"
    let events: Vec<_> = event_store
        .get_events(
            &flow_binding,
            GetEventsOpts {
                // From 1 to 2, middle event only
                from: Some(EventID::new(latest_event_id.into_inner() - 2)),
                to: Some(EventID::new(latest_event_id.into_inner() - 1)),
            },
        )
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_2.into()]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_concurrent_modification(catalog: &Catalog) {
    let event_store = catalog
        .get_one::<dyn FlowConfigurationEventStore>()
        .unwrap();

    let flow_binding = ingest_dataset_binding(&odf::DatasetID::new_seeded_ed25519(b"foo"));
    let other_flow_binding = ingest_dataset_binding(&odf::DatasetID::new_seeded_ed25519(b"bar"));

    let rule = || {
        FlowConfigRuleCompact::try_new(100_000, 1000)
            .unwrap()
            .into_flow_config()
    };
    let created = |flow_binding: &FlowBinding| -> FlowConfigurationEvent {
        FlowConfigurationEventCreated {
            event_time: Utc::now(),
            flow_binding: flow_binding.clone(),
            rule: rule(),
            retry_policy: None,
        }
        .into()
    };
    let modified = |flow_binding: &FlowBinding| -> FlowConfigurationEvent {
        FlowConfigurationEventModified {
            event_time: Utc::now(),
            flow_binding: flow_binding.clone(),
            rule: rule(),
            retry_policy: None,
        }
        .into()
    };

    // Nothing stored yet, but a previous event is expected
    let res = event_store
        .save_events(
            &flow_binding,
            Some(EventID::new(15)),
            vec![created(&flow_binding)],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    let created_event_id = event_store
        .save_events(&flow_binding, None, vec![created(&flow_binding)])
        .await
        .unwrap();

    // Events stored, but none expected
    let res = event_store
        .save_events(&flow_binding, None, vec![modified(&flow_binding)])
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Other bindings are independent
    let other_event_id = event_store
        .save_events(
            &other_flow_binding,
            None,
            vec![created(&other_flow_binding)],
        )
        .await
        .unwrap();

    event_store
        .save_events(
            &flow_binding,
            Some(created_event_id),
            vec![modified(&flow_binding)],
        )
        .await
        .unwrap();

    // The expected event is no longer the last one
    let res = event_store
        .save_events(
            &flow_binding,
            Some(created_event_id),
            vec![modified(&flow_binding)],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // The expected event belongs to another binding
    let res = event_store
        .save_events(
            &flow_binding,
            Some(other_event_id),
            vec![modified(&flow_binding)],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    assert_eq!(3, event_store.total_events_stored().await.unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

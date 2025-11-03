// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{Duration, Utc};
use dill::Catalog;
use futures::TryStreamExt;
use kamu_adapter_flow_dataset::{FlowScopeDataset, ingest_dataset_binding};
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn dummy_schedule() -> FlowTriggerRule {
    FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
        every: Duration::seconds(5),
    }))
}

fn dummy_schedule_cron() -> FlowTriggerRule {
    FlowTriggerRule::Schedule(Schedule::try_from_5component_cron_expression("0 * * * *").unwrap())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(0, num_events);

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let flow_binding = ingest_dataset_binding(&dataset_id);
    let events: Vec<_> = event_store
        .get_events(&flow_binding, GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(events, []);

    let bindings = event_store
        .all_trigger_bindings_for_scope(&flow_binding.scope)
        .await
        .unwrap();
    assert_eq!(bindings, []);

    let system_bindings = event_store
        .all_trigger_bindings_for_scope(&FlowScope::make_system_scope())
        .await
        .unwrap();
    assert_eq!(system_bindings, []);

    let all_active_bindings = event_store
        .stream_all_active_flow_bindings()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(all_active_bindings, []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_streams(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();

    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let flow_binding_1 = ingest_dataset_binding(&dataset_id_1);

    let event_1_1 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding_1.clone(),
        paused: false,
        rule: dummy_schedule(),
        stop_policy: FlowTriggerStopPolicy::default(),
    };
    let event_1_2 = FlowTriggerEventModified {
        event_time: Utc::now(),
        flow_binding: flow_binding_1.clone(),
        paused: true,
        rule: dummy_schedule(),
        stop_policy: FlowTriggerStopPolicy::default(),
    };

    event_store
        .save_events(
            &flow_binding_1,
            None,
            vec![event_1_1.clone().into(), event_1_2.clone().into()],
        )
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(2, num_events);

    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");
    let flow_binding_2 = ingest_dataset_binding(&dataset_id_2);

    let event_2 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding_2.clone(),
        paused: false,
        rule: dummy_schedule_cron(),
        stop_policy: FlowTriggerStopPolicy::default(),
    };

    event_store
        .save_events(&flow_binding_2, None, vec![event_2.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(3, num_events);

    let flow_binding_3 = FlowBinding::new(FLOW_TYPE_SYSTEM_GC, FlowScope::make_system_scope());
    let event_3 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding_3.clone(),
        paused: false,
        rule: dummy_schedule(),
        stop_policy: FlowTriggerStopPolicy::default(),
    };

    event_store
        .save_events(&flow_binding_3, None, vec![event_3.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();
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

    let bindings = event_store
        .all_trigger_bindings_for_scope(&flow_binding_1.scope)
        .await
        .unwrap();
    assert_eq!(bindings, vec![flow_binding_1.clone()]);

    let bindings = event_store
        .all_trigger_bindings_for_scope(&flow_binding_2.scope)
        .await
        .unwrap();
    assert_eq!(bindings, vec![flow_binding_2.clone()]);

    let bindings = event_store
        .all_trigger_bindings_for_scope(&flow_binding_3.scope)
        .await
        .unwrap();
    assert_eq!(bindings, vec![flow_binding_3.clone()]);

    let all_active_bindings = event_store
        .stream_all_active_flow_bindings()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(all_active_bindings.len(), 2);
    assert!(all_active_bindings.contains(&flow_binding_2));
    assert!(all_active_bindings.contains(&flow_binding_3));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_with_windowing(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();

    let flow_binding = ingest_dataset_binding(&odf::DatasetID::new_seeded_ed25519(b"foo"));

    let event_1 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding.clone(),
        paused: false,
        rule: dummy_schedule(),
        stop_policy: FlowTriggerStopPolicy::default(),
    };
    let event_2 = FlowTriggerEventModified {
        event_time: Utc::now(),
        flow_binding: flow_binding.clone(),
        paused: false,
        rule: dummy_schedule(),
        stop_policy: FlowTriggerStopPolicy::default(),
    };
    let event_3 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_binding: flow_binding.clone(),
        paused: false,
        rule: dummy_schedule(),
        stop_policy: FlowTriggerStopPolicy::default(),
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

pub async fn test_has_active_trigger_for_datasets(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();

    let dataset_id_active = odf::DatasetID::new_seeded_ed25519(b"active");
    let dataset_id_paused = odf::DatasetID::new_seeded_ed25519(b"paused");
    let dataset_id_modified_paused = odf::DatasetID::new_seeded_ed25519(b"modified_paused");
    let dataset_id_modified_active = odf::DatasetID::new_seeded_ed25519(b"modified_active");
    let dataset_id_removed = odf::DatasetID::new_seeded_ed25519(b"removed");
    let unrelated_dataset_id = odf::DatasetID::new_seeded_ed25519(b"other");

    // Active trigger
    let flow_active = ingest_dataset_binding(&dataset_id_active);
    event_store
        .save_events(
            &flow_active,
            None,
            vec![
                FlowTriggerEventCreated {
                    event_time: Utc::now(),
                    flow_binding: flow_active.clone(),
                    paused: false,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Paused trigger
    let flow_paused = ingest_dataset_binding(&dataset_id_paused);
    event_store
        .save_events(
            &flow_paused,
            None,
            vec![
                FlowTriggerEventCreated {
                    event_time: Utc::now(),
                    flow_binding: flow_paused.clone(),
                    paused: true,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Modified trigger: first active, then paused
    let flow_modified_paused = ingest_dataset_binding(&dataset_id_modified_paused);
    event_store
        .save_events(
            &flow_modified_paused,
            None,
            vec![
                FlowTriggerEventCreated {
                    event_time: Utc::now() - Duration::seconds(10),
                    flow_binding: flow_modified_paused.clone(),
                    paused: false,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
                FlowTriggerEventModified {
                    event_time: Utc::now(),
                    flow_binding: flow_modified_paused.clone(),
                    paused: true,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Modified trigger: first paused, then active
    let flow_modified_active = ingest_dataset_binding(&dataset_id_modified_active);
    event_store
        .save_events(
            &flow_modified_active,
            None,
            vec![
                FlowTriggerEventCreated {
                    event_time: Utc::now() - Duration::seconds(10),
                    flow_binding: flow_modified_active.clone(),
                    paused: true,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
                FlowTriggerEventModified {
                    event_time: Utc::now(),
                    flow_binding: flow_modified_active.clone(),
                    paused: false,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Removed trigger
    let flow_removed = ingest_dataset_binding(&dataset_id_removed);
    event_store
        .save_events(
            &flow_removed,
            None,
            vec![
                FlowTriggerEventCreated {
                    event_time: Utc::now() - Duration::seconds(10),
                    flow_binding: flow_removed.clone(),
                    paused: false,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
                FlowTriggerEventScopeRemoved {
                    event_time: Utc::now(),
                    flow_binding: flow_removed.clone(),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // System trigger (should be ignored)
    let flow_system = FlowBinding::new(FLOW_TYPE_SYSTEM_GC, FlowScope::make_system_scope());
    event_store
        .save_events(
            &flow_system,
            None,
            vec![
                FlowTriggerEventCreated {
                    event_time: Utc::now(),
                    flow_binding: flow_system.clone(),
                    paused: false,
                    rule: dummy_schedule(),
                    stop_policy: FlowTriggerStopPolicy::default(),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    async fn test(store: &dyn FlowTriggerEventStore, ids: &[odf::DatasetID]) -> bool {
        let scopes: Vec<_> = ids.iter().map(FlowScopeDataset::make_scope).collect();
        store.has_active_triggers_for_scopes(&scopes).await.unwrap()
    }

    assert!(
        test(
            event_store.as_ref(),
            std::slice::from_ref(&dataset_id_active)
        )
        .await
    );
    assert!(
        !test(
            event_store.as_ref(),
            std::slice::from_ref(&dataset_id_paused)
        )
        .await
    );
    assert!(
        test(
            event_store.as_ref(),
            std::slice::from_ref(&dataset_id_modified_active)
        )
        .await
    );
    assert!(
        !test(
            event_store.as_ref(),
            std::slice::from_ref(&dataset_id_modified_paused)
        )
        .await
    );
    assert!(
        !test(
            event_store.as_ref(),
            std::slice::from_ref(&dataset_id_removed)
        )
        .await
    );
    assert!(
        !test(
            event_store.as_ref(),
            std::slice::from_ref(&unrelated_dataset_id)
        )
        .await
    );
    assert!(
        test(
            event_store.as_ref(),
            &[dataset_id_active, dataset_id_paused.clone()]
        )
        .await
    );
    assert!(
        !test(
            event_store.as_ref(),
            &[
                dataset_id_modified_paused.clone(),
                dataset_id_paused.clone()
            ]
        )
        .await
    );
    assert!(
        test(
            event_store.as_ref(),
            &[
                dataset_id_modified_active,
                dataset_id_paused,
                dataset_id_modified_paused,
                dataset_id_removed
            ]
        )
        .await
    );
    assert!(!test(event_store.as_ref(), &[]).await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

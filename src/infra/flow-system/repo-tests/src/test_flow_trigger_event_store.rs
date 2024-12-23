// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{Duration, Utc};
use database_common::PaginationOpts;
use dill::Catalog;
use futures::TryStreamExt;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_empty(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(0, num_events);

    let flow_key = FlowKey::dataset(
        odf::DatasetID::new_seeded_ed25519(b"foo"),
        DatasetFlowType::Ingest,
    );
    let events: Vec<_> = event_store
        .get_events(&flow_key, GetEventsOpts::default())
        .try_collect()
        .await
        .unwrap();

    assert_eq!(events, []);

    let dataset_ids: Vec<_> = event_store
        .list_dataset_ids(&PaginationOpts {
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap();

    assert_eq!(dataset_ids, []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_streams(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();

    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"foo");
    let flow_key_1 = FlowKey::dataset(dataset_id_1.clone(), DatasetFlowType::Ingest);

    let event_1_1 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_key: flow_key_1.clone(),
        paused: false,
        rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
            every: Duration::seconds(5),
        })),
    };
    let event_1_2 = FlowTriggerEventModified {
        event_time: Utc::now(),
        flow_key: flow_key_1.clone(),
        paused: true,
        rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
            every: Duration::seconds(5),
        })),
    };

    event_store
        .save_events(
            &flow_key_1,
            None,
            vec![event_1_1.clone().into(), event_1_2.clone().into()],
        )
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(2, num_events);

    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"bar");
    let flow_key_2 = FlowKey::dataset(dataset_id_2.clone(), DatasetFlowType::Ingest);

    let event_2 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_key: flow_key_2.clone(),
        paused: false,
        rule: FlowTriggerRule::Schedule(
            Schedule::try_from_5component_cron_expression("0 * * * *").unwrap(),
        ),
    };

    event_store
        .save_events(&flow_key_2, None, vec![event_2.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(3, num_events);

    let flow_key_3 = FlowKey::system(SystemFlowType::GC);
    let event_3 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_key: flow_key_3.clone(),
        paused: false,
        rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
            every: Duration::seconds(5),
        })),
    };

    event_store
        .save_events(&flow_key_3, None, vec![event_3.clone().into()])
        .await
        .unwrap();

    let num_events = event_store.len().await.unwrap();

    assert_eq!(4, num_events);

    let events: Vec<_> = event_store
        .get_events(&flow_key_1, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_1_1.into(), event_1_2.into()]);

    let events: Vec<_> = event_store
        .get_events(&flow_key_2, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_2.into()]);

    let events: Vec<_> = event_store
        .get_events(&flow_key_3, GetEventsOpts::default())
        .map_ok(|(_, event)| event)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(&events[..], [event_3.into()]);

    let mut dataset_ids: Vec<_> = event_store
        .list_dataset_ids(&PaginationOpts {
            limit: 10,
            offset: 0,
        })
        .await
        .unwrap();

    dataset_ids.sort();

    let mut expected_dataset_ids = vec![dataset_id_1, dataset_id_2];

    expected_dataset_ids.sort();

    assert_eq!(expected_dataset_ids, dataset_ids);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_get_events_with_windowing(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();

    let flow_key = FlowKey::dataset(
        odf::DatasetID::new_seeded_ed25519(b"foo"),
        DatasetFlowType::Ingest,
    );

    let event_1 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_key: flow_key.clone(),
        paused: false,
        rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
            every: Duration::seconds(5),
        })),
    };
    let event_2 = FlowTriggerEventModified {
        event_time: Utc::now(),
        flow_key: flow_key.clone(),
        paused: false,
        rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
            every: Duration::seconds(5),
        })),
    };
    let event_3 = FlowTriggerEventCreated {
        event_time: Utc::now(),
        flow_key: flow_key.clone(),
        paused: false,
        rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
            every: Duration::seconds(5),
        })),
    };

    let latest_event_id = event_store
        .save_events(
            &flow_key,
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
            &flow_key,
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
            &flow_key,
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
            &flow_key,
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

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::FutureExt;
use time_source::{FakeSystemTimeSource, SystemTimeSource};
use tokio::time::timeout;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fake_sleep_stable_order() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = FakeSystemTimeSource::new(t0);

    let mut sleep_futures = vec![
        Some(system_time_source.sleep(Duration::seconds(120))), // 1
        Some(system_time_source.sleep(Duration::seconds(60))),  // 2
        Some(system_time_source.sleep(Duration::seconds(90))),  // 3
        Some(system_time_source.sleep(Duration::seconds(120))), // 4
        Some(system_time_source.sleep(Duration::seconds(150))), // 5
        Some(system_time_source.sleep(Duration::seconds(60))),  // 6
    ];

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [false, false, false, false, false, false]
    );

    let dt = Duration::seconds(150);
    let t = t0 + dt;
    let ready_future_ids = system_time_source.advance(Duration::seconds(150));

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [true, true, true, true, true, true]
    );
    assert_eq!(ready_future_ids, [2, 6, 3, 1, 4, 5]);
    assert_eq!(system_time_source.now(), t);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fake_sleep_without_simulate_time_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = FakeSystemTimeSource::new(t0);

    let wake_after_1_sec_fut = system_time_source.sleep(Duration::seconds(1));
    let sleep_result_or_timeout = timeout(StdDuration::from_millis(50), wake_after_1_sec_fut).await;

    assert_matches!(sleep_result_or_timeout, Err(_));
    assert_eq!(system_time_source.now(), t0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fake_sleep_with_lacking_simulate_time_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = FakeSystemTimeSource::new(t0);

    let mut sleep_futures = vec![Some(system_time_source.sleep(Duration::seconds(60)))];

    assert_eq!(check_futures_for_completion(&mut sleep_futures), [false]);

    let dt = Duration::seconds(30);
    let t = t0 + dt;

    system_time_source.advance(dt);

    assert_eq!(check_futures_for_completion(&mut sleep_futures), [false]);
    assert_eq!(system_time_source.now(), t);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fake_sleep_with_several_simulate_time_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = FakeSystemTimeSource::new(t0);

    // 1. Start waiting 4 parallel futures:
    //    - a)  60 seconds
    //    - b)  90 seconds
    //    - c) 120 seconds
    //    - d) 120 seconds
    let mut sleep_futures = vec![
        Some(system_time_source.sleep(Duration::seconds(60))),
        Some(system_time_source.sleep(Duration::seconds(90))),
        Some(system_time_source.sleep(Duration::seconds(120))),
        Some(system_time_source.sleep(Duration::seconds(120))),
    ];

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [false, false, false, false]
    );

    // 2. Simulate +30 seconds:
    //    - a) +00:30/01:00 - waiting
    //    - b) +00:30/01:30 - waiting
    //    - c) +00:30/02:00 - waiting
    //    - d) +00:30/02:00 - waiting
    let dt = Duration::seconds(30);
    let t = t0 + dt;

    system_time_source.advance(dt);

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [false, false, false, false]
    );
    assert_eq!(system_time_source.now(), t);

    // 3. Simulate +30 seconds:
    //    - a) +01:00/01:00 - done
    //    - b) +01:00/01:30 - waiting
    //    - c) +01:00/02:00 - waiting
    //    - d) +01:00/02:00 - waiting
    let dt = Duration::seconds(30);
    let t = t + dt;

    system_time_source.advance(dt);

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [true, false, false, false]
    );
    assert_eq!(system_time_source.now(), t);

    // 4. Simulate +30 seconds:
    //    - a) +01:30/01:00 - done (before)
    //    - b) +01:30/01:30 - done
    //    - c) +01:30/02:00 - waiting
    //    - d) +01:30/02:00 - waiting
    let dt = Duration::seconds(30);
    let t = t + dt;

    system_time_source.advance(dt);

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [true, true, false, false]
    );
    assert_eq!(system_time_source.now(), t);

    // 5. Simulate +30 seconds:
    //    - a) +01:30/01:00 - done (before)
    //    - b) +01:30/01:30 - done (before)
    //    - c) +01:30/02:00 - done
    //    - d) +01:30/02:00 - done
    let dt = Duration::seconds(30);
    let t = t + dt;

    system_time_source.advance(dt);

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [true, true, true, true]
    );
    assert_eq!(system_time_source.now(), t);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fake_sleep_with_simulate_exceeding_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = FakeSystemTimeSource::new(t0);
    // 1. Start waiting 2 parallel futures:
    //    - a) 60 seconds
    //    - b) 90 seconds
    let mut sleep_futures = vec![
        Some(system_time_source.sleep(Duration::seconds(60))),
        Some(system_time_source.sleep(Duration::seconds(90))),
    ];

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [false, false]
    );

    // 2. Simulate +90 seconds:
    //    - a) +01:30/01:00 - 30-second overrun
    //    - b) +01:30/01:30 - done
    let dt = Duration::seconds(90);
    let t = t0 + dt;

    system_time_source.advance(dt);

    assert_eq!(
        check_futures_for_completion(&mut sleep_futures),
        [true, true]
    );
    assert_eq!(system_time_source.now(), t);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn point_in_time_in_a_parallel_universe() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap()
}

type FutureWithoutReturnValue<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

fn check_futures_for_completion(
    futures: &mut Vec<Option<FutureWithoutReturnValue<'_>>>,
) -> Vec<bool> {
    let mut res = Vec::with_capacity(futures.len());

    for maybe_f in futures {
        let Some(f) = maybe_f else {
            res.push(true);

            continue;
        };

        let has_future_resolved = f.now_or_never().is_some();

        if has_future_resolved {
            *maybe_f = None;
        }

        res.push(has_future_resolved);
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

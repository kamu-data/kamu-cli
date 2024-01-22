// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, TimeZone, Utc};
use kamu_core::{SystemTimeSource, SystemTimeSourceStub};
use tokio::time::timeout;

#[tokio::test]
async fn test_stub_wake_after_without_simulate_time_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = SystemTimeSourceStub::new_set(t0);

    let wake_after_1_sec_fut = system_time_source.wake_after(Duration::seconds(1));
    let wake_up_result_or_timeout =
        timeout(StdDuration::from_millis(50), wake_after_1_sec_fut).await;

    assert_matches!(wake_up_result_or_timeout, Err(_));
    assert_eq!(system_time_source.now(), t0);
}

#[tokio::test]
async fn test_stub_wake_after_with_lacking_simulate_time_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = SystemTimeSourceStub::new_set(t0);

    {
        let dt1 = Duration::seconds(30);
        let t1 = t0 + dt1;

        let wake_up_result_or_timeout = {
            let wake_after_60_sec_fut = system_time_source.wake_after(Duration::seconds(60));
            let simulate_30_sec_passage_fut = Box::pin(async { system_time_source.advance(dt1) });
            let wake_after_fut =
                futures::future::join(wake_after_60_sec_fut, simulate_30_sec_passage_fut);

            timeout(StdDuration::from_millis(50), wake_after_fut).await
        };

        assert_matches!(wake_up_result_or_timeout, Err(_));
        assert_eq!(system_time_source.now(), t1);
    };
}

#[tokio::test]
async fn test_stub_wake_after_with_several_simulate_time_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = SystemTimeSourceStub::new_set(t0);
    // 1. Start waiting 4 parallel futures:
    //    - a)  60 seconds
    //    - b)  90 seconds
    //    - c) 120 seconds
    //    - d) 120 seconds
    let a_wake_after_60_sec_fut = system_time_source.wake_after(Duration::seconds(60));
    let b_wake_after_90_sec_fut = system_time_source.wake_after(Duration::seconds(90));
    let c_wake_after_120_sec_fut = system_time_source.wake_after(Duration::seconds(120));
    let d_wake_after_120_sec_fut = system_time_source.wake_after(Duration::seconds(120));

    let main_inner = async {
        // 2. Simulate +30 seconds:
        //    - a) +00:30/01:00 - waiting
        //    - b) +00:30/01:30 - waiting
        //    - c) +00:30/02:00 - waiting
        //    - d) +00:30/02:00 - waiting
        let t1 = {
            let dt1 = Duration::seconds(30);
            let t1 = t0 + dt1;

            let woke_up_callers_ids = system_time_source.advance(dt1);

            assert!(woke_up_callers_ids.is_empty());
            assert_eq!(system_time_source.now(), t1);

            t1
        };
        // 3. Simulate +30 seconds:
        //    - a) +01:00/01:00 - done
        //    - b) +01:00/01:30 - waiting
        //    - c) +01:00/02:00 - waiting
        //    - d) +01:00/02:00 - waiting
        let t2 = {
            let dt2 = Duration::seconds(30);
            let t2 = t1 + dt2;

            let woke_up_callers_ids = system_time_source.advance(dt2);

            assert_eq!(woke_up_callers_ids, [1]);
            assert_eq!(system_time_source.now(), t2);

            t2
        };
        // 4. Simulate +30 seconds:
        //    - a) +01:30/01:00 - done (before)
        //    - b) +01:30/01:30 - done
        //    - c) +01:30/02:00 - waiting
        //    - d) +01:30/02:00 - waiting
        let t3 = {
            let dt3 = Duration::seconds(30);
            let t3 = t2 + dt3;

            let woke_up_callers_ids = system_time_source.advance(dt3);

            assert_eq!(woke_up_callers_ids, [2]);
            assert_eq!(system_time_source.now(), t3);

            t3
        };
        // 4. Simulate +30 seconds:
        //    - a) +01:30/01:00 - done (before)
        //    - b) +01:30/01:30 - done (before)
        //    - c) +01:30/02:00 - done
        //    - d) +01:30/02:00 - done
        {
            let dt4 = Duration::seconds(30);
            let t2 = t3 + dt4;

            let woke_up_callers_ids = system_time_source.advance(dt4);

            assert_eq!(woke_up_callers_ids, [3, 4]);
            assert_eq!(system_time_source.now(), t2);
        };
    };

    let main = futures::future::join5(
        a_wake_after_60_sec_fut,
        b_wake_after_90_sec_fut,
        c_wake_after_120_sec_fut,
        d_wake_after_120_sec_fut,
        main_inner,
    );
    let main_result_or_timeout = timeout(StdDuration::from_millis(50), main).await;

    assert_matches!(main_result_or_timeout, Ok(_));
}

#[tokio::test]
async fn test_stub_wake_after_with_simulate_exceeding_passage() {
    let t0 = point_in_time_in_a_parallel_universe();
    let system_time_source = SystemTimeSourceStub::new_set(t0);
    // 1. Start waiting 2 parallel futures:
    //    - a) 60 seconds
    //    - b) 90 seconds
    let a_wake_after_60_sec_fut = system_time_source.wake_after(Duration::seconds(60));
    let b_wake_after_90_sec_fut = system_time_source.wake_after(Duration::seconds(90));

    let main_inner = async {
        // 2. Simulate +90 seconds:
        //    - a) +01:30/01:00 - 30-second overrun
        //    - b) +01:30/01:30 - done
        let dt1 = Duration::seconds(90);
        let t1 = t0 + dt1;

        let woke_up_callers_ids = system_time_source.advance(dt1);

        assert_eq!(woke_up_callers_ids, [1, 2]);
        assert_eq!(system_time_source.now(), t1);
    };

    let main = futures::future::join3(a_wake_after_60_sec_fut, b_wake_after_90_sec_fut, main_inner);
    let main_result_or_timeout = timeout(StdDuration::from_millis(50), main).await;

    assert_matches!(main_result_or_timeout, Ok(_));
}

/////////////////////////////////////////////////////////////////////////////////////////

fn point_in_time_in_a_parallel_universe() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2050, 1, 2, 12, 0, 0).unwrap()
}

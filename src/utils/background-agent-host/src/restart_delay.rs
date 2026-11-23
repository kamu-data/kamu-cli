// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use chrono::TimeDelta;
use time_source::SystemTimeSource;

use crate::CancellationToken;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Waits `delay` on the time source before a restart. Returns `false` when
/// `shutdown` cut the wait short, so that nothing is restarted.
pub async fn wait_restart_delay(
    time_source: &dyn SystemTimeSource,
    delay: Duration,
    shutdown: &CancellationToken,
) -> bool {
    let sleep = time_source.sleep(TimeDelta::from_std(delay).unwrap_or(TimeDelta::MAX));
    shutdown.run_until_cancelled(sleep).await.is_some()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

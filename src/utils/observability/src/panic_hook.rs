// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::panic;
use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn extend_panic_hook() {
    let default_hook = Arc::new(panic::take_hook());

    panic::set_hook(Box::new(move |info| {
        default_hook(info);

        let backtrace = Backtrace::force_capture();
        let payload = info.payload();
        let msg = if let Some(s) = payload.downcast_ref::<&str>() {
            *s
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.as_str()
        } else {
            "Unknown panic payload"
        };

        let location = info
            .location()
            .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
            .unwrap_or_else(|| "unknown location".into());

        tracing::error!(
            target: "panic_hook",
            message = %msg,
            location = %location,
            backtrace = %backtrace,
            "panic occurred"
        );
    }));
}

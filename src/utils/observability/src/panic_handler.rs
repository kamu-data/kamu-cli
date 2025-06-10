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

pub fn set_hook_trace_panics(propagate: bool) {
    let default_hook_maybe = if propagate {
        Some(Arc::new(panic::take_hook()))
    } else {
        None
    };

    panic::set_hook(Box::new(move |info| {
        if let Some(default_hook) = default_hook_maybe.as_ref() {
            default_hook(info);
        }

        let backtrace = Backtrace::force_capture();
        let payload = info.payload();
        let error_msg = if let Some(s) = payload.downcast_ref::<&str>() {
            *s
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.as_str()
        } else {
            "Unknown panic payload"
        };

        tracing::error!(error_msg, error_backtrace = %backtrace, "Unhandled panic caught");
    }));
}

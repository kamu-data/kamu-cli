// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::backtrace::Backtrace;
use std::panic;
use std::sync::Arc;

use axum::body::Body;
use http::{Response, StatusCode, header};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn set_hook_capture_panic_backtraces_no_propagate(enable_default_hook: bool) {
    let default_hook = Arc::new(panic::take_hook());

    panic::set_hook(Box::new(move |info| {
        if enable_default_hook {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::needless_pass_by_value)]
pub fn panic_handler(_err: Box<dyn Any + Send + 'static>) -> Response<Body> {
    let body = Body::from(r#"{"error":"Internal Server Error"}"#);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}

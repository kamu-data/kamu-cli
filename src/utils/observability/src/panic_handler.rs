// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::panic;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use http::{header, Response, StatusCode};
use lazy_static::lazy_static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Such as we can't get real backtrace in catch_unwind block
// we need place to store trace if such occurs and then print it in logs
lazy_static! {
    static ref BACKTRACE: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn set_panic_hook() {
    panic::set_hook(Box::new(move |_| {
        *BACKTRACE.lock().unwrap() = Some(std::backtrace::Backtrace::force_capture().to_string());
    }));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::needless_pass_by_value)]
pub fn panic_handler(err: Box<dyn Any + Send + 'static>) -> Response<Body> {
    let panic_msg = if let Some(s) = err.downcast_ref::<&str>() {
        *s
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.as_str()
    } else {
        "Unknown panic payload"
    };

    let backtrace = BACKTRACE
        .lock()
        .unwrap()
        .take()
        .unwrap_or("Backtrace not found".to_string());

    tracing::error!(err = "Unhandled panic caught", panic_msg, %backtrace);

    let body = Body::from(r#"{"error":"Internal Server Error"}"#);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}

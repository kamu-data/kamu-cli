// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;

use tokio::task::JoinError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why a spawned task did not return: the panic's text, or the cancellation
pub fn join_error_message(join_error: JoinError) -> String {
    if join_error.is_panic() {
        panic_message(join_error.into_panic().as_ref())
    } else {
        join_error.to_string()
    }
}

/// The text of a panic payload: `panic!` with a literal yields `&str`, with a
/// format string `String`
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-text panic payload".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

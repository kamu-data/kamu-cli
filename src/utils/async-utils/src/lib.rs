// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait ResultAsync<T, E> {
    async fn and_then_async<F, FFut, U>(self, op: F) -> Result<U, E>
    where
        F: FnOnce(T) -> FFut,
        FFut: std::future::Future<Output = Result<U, E>>;
}

#[async_trait::async_trait(?Send)]
impl<T, E> ResultAsync<T, E> for Result<T, E> {
    async fn and_then_async<F, FFut, U>(self, op: F) -> Result<U, E>
    where
        F: FnOnce(T) -> FFut,
        FFut: std::future::Future<Output = Result<U, E>>,
    {
        match self {
            Ok(val) => op(val).await,
            Err(e) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mod async_read_obj;
pub use async_read_obj::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

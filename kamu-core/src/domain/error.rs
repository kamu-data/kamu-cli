// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;

use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Internal error")]
pub struct InternalError {
    #[source]
    source: BoxedError,
    #[backtrace]
    backtrace: Option<Backtrace>,
}

impl InternalError {
    pub fn new<E: Into<BoxedError>>(e: E) -> Self {
        let source = e.into();
        let backtrace = if source.request_ref::<Backtrace>().is_some() {
            None
        } else {
            Some(Backtrace::capture())
        };

        Self { source, backtrace }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub trait ErrorIntoInternal {
    fn int_err(self) -> InternalError;
}

impl<E> ErrorIntoInternal for E
where
    E: Into<BoxedError>,
{
    fn int_err(self) -> InternalError {
        InternalError::new(self)
    }
}

pub trait ResultIntoInternal<OK> {
    fn int_err(self) -> Result<OK, InternalError>;
}

impl<OK, E> ResultIntoInternal<OK> for Result<OK, E>
where
    E: Into<BoxedError>,
{
    fn int_err(self) -> Result<OK, InternalError> {
        match self {
            Ok(ok) => Ok(ok),
            Err(e) => Err(e.int_err()),
        }
    }
}

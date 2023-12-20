// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

///////////////////////////////////////////////////////////////////////////////

use std::marker::PhantomData;

pub trait Multiformat {
    fn format_name() -> &'static str;
}

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub struct ParseError<T: Multiformat> {
    pub value: String,
    #[source]
    pub source: Option<BoxedError>,
    _phantom: PhantomData<T>,
}

impl<T: Multiformat> ParseError<T> {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            source: None,
            _phantom: PhantomData,
        }
    }

    pub fn new_from(value: impl Into<String>, source: impl Into<BoxedError>) -> Self {
        Self {
            value: value.into(),
            source: Some(source.into()),
            _phantom: PhantomData,
        }
    }
}

impl<T: Multiformat> std::fmt::Display for ParseError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Value '{}' is not a valid {}",
            self.value,
            T::format_name()
        )
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub struct DeserializeError<T: Multiformat> {
    #[source]
    pub source: Option<BoxedError>,
    _phantom: PhantomData<T>,
}

impl<T: Multiformat> DeserializeError<T> {
    pub fn new() -> Self {
        Self {
            source: None,
            _phantom: PhantomData,
        }
    }

    pub fn new_from(source: impl Into<BoxedError>) -> Self {
        Self {
            source: Some(source.into()),
            _phantom: PhantomData,
        }
    }
}

impl<T: Multiformat> std::fmt::Display for DeserializeError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Byte sequence is not a valid {}", T::format_name())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Buffer too small")]
pub struct BufferTooSmall;

impl BufferTooSmall {
    pub fn new() -> Self {
        Self
    }
}

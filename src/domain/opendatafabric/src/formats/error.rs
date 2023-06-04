// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct InvalidValue<T: ?Sized> {
    pub value: String,
    _ph: PhantomData<T>,
}

impl<T: ?Sized> InvalidValue<T> {
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self {
            value: s.into(),
            _ph: PhantomData,
        }
    }
}

macro_rules! impl_invalid_value {
    ($typ:ident) => {
        impl std::fmt::Debug for InvalidValue<$typ> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "InvalidValue<{}>({:?})", stringify!($typ), self.value)
            }
        }

        impl std::fmt::Display for InvalidValue<$typ> {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "Invalid {}: {}", stringify!($typ), self.value)
            }
        }

        impl std::error::Error for InvalidValue<$typ> {}
    };
}

pub(crate) use impl_invalid_value;

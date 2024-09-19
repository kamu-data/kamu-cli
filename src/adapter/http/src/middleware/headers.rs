// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum_extra::typed_header::TypedHeader;
use headers::authorization::{Authorization, Bearer};
use headers::{Error, Header};
use http::{HeaderName, HeaderValue};

pub type BearerHeader = TypedHeader<Authorization<Bearer>>;
pub type OdfSmtpVersionTyped = TypedHeader<OdfSmtpVersion>;

#[derive(Debug)]
pub struct OdfSmtpVersion(pub i32);

impl Header for OdfSmtpVersion {
    fn name() -> &'static HeaderName {
        static NAME: HeaderName = HeaderName::from_static("x-odf-smtp-version");
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, Error>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        if let Some(value) = values.next() {
            let value_str = value.to_str().map_err(|_| Error::invalid())?;
            let value_int = value_str.parse::<i32>().map_err(|_| Error::invalid())?;
            Ok(OdfSmtpVersion(value_int))
        } else {
            Err(Error::invalid())
        }
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let value_str = self.0.to_string();
        let value = HeaderValue::from_str(&value_str).expect("Invalid version header value");
        values.extend(std::iter::once(value));
    }
}

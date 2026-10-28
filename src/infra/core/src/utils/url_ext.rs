// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait UrlExt {
    fn ensure_trailing_slash(&mut self);
    fn is_odf_protocol(&self) -> bool;
    fn as_odf_protocol(&self) -> Result<Url, InternalError>;

    /// Converts from odf+http(s) scheme to plain http(s)
    fn odf_to_transport_protocol(&self) -> Result<Url, InternalError>;
}

impl UrlExt for Url {
    fn ensure_trailing_slash(&mut self) {
        if !self.path().ends_with('/') {
            self.set_path(&format!("{}/", self.path()));
        }
    }

    fn as_odf_protocol(&self) -> Result<Url, InternalError> {
        let url_string = self.as_str().replace("http", "odf+http");
        Url::from_str(&url_string).int_err()
    }

    fn is_odf_protocol(&self) -> bool {
        self.scheme().starts_with("odf+")
    }

    fn odf_to_transport_protocol(&self) -> Result<Url, InternalError> {
        let s = self
            .as_str()
            .strip_prefix("odf+")
            .ok_or_else(|| format!("Expected odf+http(s) URL but got: {self}").int_err())?;
        Url::parse(s).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

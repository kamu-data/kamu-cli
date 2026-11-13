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
    /// Converts an http(s) URL to the odf+http(s) scheme; other URLs are
    /// returned as they are
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
        match self.scheme() {
            "http" | "https" => Url::from_str(&format!("odf+{self}")).int_err(),
            _ => Ok(self.clone()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_odf_protocol_rewrites_only_the_scheme() {
        let url = Url::parse("https://example.com/http-repo/").unwrap();

        assert_eq!(
            url.as_odf_protocol().unwrap().as_str(),
            "odf+https://example.com/http-repo/"
        );
    }

    #[test]
    fn test_as_odf_protocol_keeps_other_schemes() {
        for url in [
            "odf+http://example.com/",
            "file:///tmp/repo/",
            "s3://bucket/repo/",
        ] {
            let url = Url::parse(url).unwrap();

            assert_eq!(url.as_odf_protocol().unwrap(), url);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

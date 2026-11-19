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

pub fn ensure_trailing_slash(url: &mut Url) {
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn is_odf_protocol(url: &Url) -> bool {
    url.scheme().starts_with("odf+")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Converts an http(s) URL to the odf+http(s) scheme; other URLs are
/// returned as they are
pub fn to_odf_protocol(url: &Url) -> Result<Url, InternalError> {
    match url.scheme() {
        "http" | "https" => Url::from_str(&format!("odf+{url}")).int_err(),
        _ => Ok(url.clone()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Converts from odf+http(s) scheme to plain http(s)
pub fn odf_to_transport_protocol(url: &Url) -> Result<Url, InternalError> {
    let s = url
        .as_str()
        .strip_prefix("odf+")
        .ok_or_else(|| format!("Expected odf+http(s) URL but got: {url}").int_err())?;
    Url::parse(s).int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_odf_protocol_rewrites_only_the_scheme() {
        let url = Url::parse("https://example.com/http-repo/").unwrap();

        assert_eq!(
            to_odf_protocol(&url).unwrap().as_str(),
            "odf+https://example.com/http-repo/"
        );
    }

    #[test]
    fn test_to_odf_protocol_keeps_other_schemes() {
        for url in [
            "odf+http://example.com/",
            "file:///tmp/repo/",
            "s3://bucket/repo/",
        ] {
            let url = Url::parse(url).unwrap();

            assert_eq!(to_odf_protocol(&url).unwrap(), url);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

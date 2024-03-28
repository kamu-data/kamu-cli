// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct Config {
    pub protocols: Protocols,
}

impl Config {
    pub fn load() -> Result<Self, InternalError> {
        // TODO: Use value from config not envvar
        //       https://github.com/kamu-data/kamu-node/issues/45
        //
        //       Example:
        //       https://github.com/mehcode/config-rs/blob/master/examples/hierarchical-env/settings.rs

        let base_url_rest = {
            let raw = std::env::var("KAMU_BASE_URL_REST")
                .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
            Url::parse(&raw).int_err()?
        };
        let base_url_flightsql = {
            let raw = std::env::var("KAMU_BASE_URL_FLIGHTSQL")
                .unwrap_or_else(|_| "grpc://localhost:50050".to_string());
            Url::parse(&raw).int_err()?
        };

        Ok(Self {
            protocols: Protocols {
                base_url_rest,
                base_url_flightsql,
            },
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
pub struct Protocols {
    pub base_url_rest: Url,
    pub base_url_flightsql: Url,
}

/////////////////////////////////////////////////////////////////////////////////////////

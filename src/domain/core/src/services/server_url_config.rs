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

pub struct ServerUrlConfig {
    pub protocols: Protocols,
}

impl ServerUrlConfig {
    fn get_url_from_env(env_var: &str, default: &str) -> Result<Url, InternalError> {
        let raw = std::env::var(env_var).unwrap_or_else(|_| default.to_string());

        Url::parse(&raw).int_err()
    }

    pub fn load_from_env() -> Result<Self, InternalError> {
        Ok(Self {
            protocols: Protocols {
                base_url_platform: Self::get_url_from_env(
                    "KAMU_BASE_URL_PLATFORM",
                    "http://localhost:4200",
                )?,
                base_url_rest: Self::get_url_from_env(
                    "KAMU_BASE_URL_REST",
                    "http://localhost:8080",
                )?,
                base_url_flightsql: Self::get_url_from_env(
                    "KAMU_BASE_URL_FLIGHTSQL",
                    "grpc://localhost:50050",
                )?,
            },
        })
    }

    pub fn new(protocols: Protocols) -> Self {
        Self { protocols }
    }

    pub fn new_test(base_url_rest: Option<&str>) -> Self {
        Self {
            protocols: Protocols {
                base_url_platform: Url::parse("http://platform.example.com").unwrap(),
                base_url_rest: Url::parse(base_url_rest.unwrap_or("http://example.com")).unwrap(),
                base_url_flightsql: Url::parse("grpc://example.com:50050").unwrap(),
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct Protocols {
    pub base_url_platform: Url,
    pub base_url_rest: Url,
    pub base_url_flightsql: Url,
}

impl Protocols {
    pub fn odata_base_url(&self) -> String {
        format!("{}odata", self.base_url_rest)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

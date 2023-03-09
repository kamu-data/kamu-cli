// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rusoto_core::Region;
use rusoto_s3::S3Client;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct S3Context {
    pub client: S3Client,
    pub bucket: String,
    pub key_prefix: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl S3Context {
    pub fn new<S1, S2>(client: S3Client, bucket: S1, key_prefix: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            client,
            bucket: bucket.into(),
            key_prefix: key_prefix.into(),
        }
    }

    pub fn from_items(endpoint: Option<String>, bucket: String, key_prefix: String) -> Self {
        let region = match endpoint {
            None => Region::default(),
            Some(endpoint) => Region::Custom {
                name: "custom".to_owned(),
                endpoint: endpoint,
            },
        };
        Self::new(S3Client::new(region), bucket, key_prefix)
    }

    pub fn from_url(url: &Url) -> Self {
        let (endpoint, bucket, key_prefix) = Self::split_url(url);
        Self::from_items(endpoint, bucket, key_prefix)
    }

    pub fn split_url(url: &Url) -> (Option<String>, String, String) {
        // TODO: Support virtual hosted style URLs once rusoto supports them
        // See: https://github.com/rusoto/rusoto/issues/1482
        let (endpoint, path): (Option<String>, String) =
            match (url.scheme(), url.host_str(), url.port(), url.path()) {
                ("s3", Some(host), None, path) => {
                    return (
                        None,
                        host.to_owned(),
                        path.trim_start_matches('/').to_owned(),
                    )
                }
                ("s3+http", Some(host), None, path) => {
                    (Some(format!("http://{}", host)), path.to_owned())
                }
                ("s3+http", Some(host), Some(port), path) => {
                    (Some(format!("http://{}:{}", host, port)), path.to_owned())
                }
                ("s3+https", Some(host), None, path) => {
                    (Some(format!("https://{}", host)), path.to_owned())
                }
                ("s3+https", Some(host), Some(port), path) => {
                    (Some(format!("https://{}:{}", host, port)), path.to_owned())
                }
                _ => panic!("Unsupported S3 url format: {}", url),
            };

        let (bucket, key_prefix) = match path.trim_start_matches('/').split_once('/') {
            Some((b, p)) => (b.to_owned(), p.to_owned()),
            None => (path.trim_start_matches('/').to_owned(), String::new()),
        };

        (endpoint, bucket, key_prefix)
    }

    pub fn get_key(&self, key: &str) -> String {
        if self.key_prefix.is_empty() {
            String::from(key)
        } else {
            format!("{}{}", self.key_prefix, key)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_accounts::{ApiKind, Principal, RequestOrigin};

use crate::domain::{CreationOrigin, CreationOriginApi, CreationProvenance};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The provenance of a resource created by `principal` in a request that came
/// through `origin`. Fails for an anonymous principal, which carries no
/// identity: applies turn it away before creating anything
pub fn request_creation_provenance(
    principal: &Principal,
    origin: &RequestOrigin,
) -> Result<CreationProvenance, InternalError> {
    let principal = principal.handle().map_err(|_| {
        InternalError::new("An anonymous principal reached the creation of a resource")
    })?;

    let origin = match origin {
        RequestOrigin::Api(api) => CreationOrigin::Api {
            api: match api {
                ApiKind::GraphQl => CreationOriginApi::GraphQl,
                ApiKind::Rest => CreationOriginApi::Rest,
                ApiKind::FlightSql => CreationOriginApi::FlightSql,
            },
        },
        RequestOrigin::Cli => CreationOrigin::Cli,
        RequestOrigin::Internal { producer, detail } => CreationOrigin::Internal {
            producer: producer.clone(),
            detail: detail.clone(),
        },
    };

    Ok(CreationProvenance { principal, origin })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_each_request_origin_maps_to_its_creation_origin() {
        let internal = |detail: Option<&str>| {
            (
                RequestOrigin::Internal {
                    producer: "dev.kamu.test.Producer".to_string(),
                    detail: detail.map(str::to_string),
                },
                CreationOrigin::Internal {
                    producer: "dev.kamu.test.Producer".to_string(),
                    detail: detail.map(str::to_string),
                },
            )
        };
        let api = |request_api, creation_api| {
            (
                RequestOrigin::Api(request_api),
                CreationOrigin::Api { api: creation_api },
            )
        };
        let cases = [
            api(ApiKind::GraphQl, CreationOriginApi::GraphQl),
            api(ApiKind::Rest, CreationOriginApi::Rest),
            api(ApiKind::FlightSql, CreationOriginApi::FlightSql),
            (RequestOrigin::Cli, CreationOrigin::Cli),
            internal(None),
            internal(Some("flow-1")),
        ];

        for (request_origin, expected_origin) in cases {
            assert_eq!(
                request_creation_provenance(&Principal::Node, &request_origin).unwrap(),
                CreationProvenance {
                    principal: Principal::Node.handle().unwrap(),
                    origin: expected_origin,
                },
                "{request_origin:?}"
            );
        }
    }

    #[test]
    fn test_anonymous_principal_has_no_creation_provenance() {
        assert!(request_creation_provenance(&Principal::Anonymous, &RequestOrigin::Cli).is_err());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_accounts::Principal;
use kamu_core::{ApiKind, RequestOrigin};

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

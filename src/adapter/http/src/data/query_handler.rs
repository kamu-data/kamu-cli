// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::{Extension, Query};
use axum::response::Json;
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use internal_error::*;
use kamu_core::*;

use super::query_types::{QueryResponse, *};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Execute a batch query
///
/// ### Regular Queries
/// This endpoint lets you execute arbitrary SQL that can access multiple
/// datasets at once.
///
/// Example request body:
/// ```json
/// {
///     "query": "select event_time, from, to, close from \"kamu/eth-to-usd\"",
///     "limit": 3,
///     "queryDialect": "SqlDataFusion",
///     "dataFormat": "JsonAoA",
///     "schemaFormat": "ArrowJson"
/// }
/// ```
///
/// Example response:
/// ```json
/// {
///     "output": {
///         "data": [
///             ["2024-09-02T21:50:00Z", "eth", "usd", 2537.07],
///             ["2024-09-02T21:51:00Z", "eth", "usd", 2541.37],
///             ["2024-09-02T21:52:00Z", "eth", "usd", 2542.66]
///         ],
///         "dataFormat": "JsonAoA",
///         "schema": {"fields": ["..."]},
///         "schemaFormat": "ArrowJson"
///     }
/// }
/// ```
///
/// ### Verifiable Queries
/// [Cryptographic proofs](https://docs.kamu.dev/node/commitments) can be
/// also requested to hold the node **forever accountable** for the provided
/// result.
///
/// Example request body:
/// ```json
/// {
///     "query": "select event_time, from, to, close from \"kamu/eth-to-usd\"",
///     "limit": 3,
///     "queryDialect": "SqlDataFusion",
///     "dataFormat": "JsonAoA",
///     "schemaFormat": "ArrowJson",
///     "include": ["proof"]
/// }
/// ```
///
/// Currently, we support verifiability by ensuring that queries are
/// deterministic and fully reproducible and signing the original response with
/// Node's private key. In future more types of proofs will be supported.
///
/// Example response:
/// ```json
/// {
///     "input": {
///         "query": "select event_time, from, to, close from \"kamu/eth-to-usd\"",
///         "queryDialect": "SqlDataFusion",
///         "dataFormat": "JsonAoA",
///         "include": ["Input", "Proof", "Schema"],
///         "schemaFormat": "ArrowJson",
///         "datasets": [{
///             "id": "did:odf:fed0119d20360650afd3d412c6b11529778b784c697559c0107d37ee5da61465726c4",
///             "alias": "kamu/eth-to-usd",
///             "blockHash": "f1620708557a44c88d23c83f2b915abc10a41cc38d2a278e851e5dc6bb02b7e1f9a1a"
///         }],
///         "skip": 0,
///         "limit": 3
///     },
///     "output": {
///         "data": [
///             ["2024-09-02T21:50:00Z", "eth", "usd", 2537.07],
///             ["2024-09-02T21:51:00Z", "eth", "usd", 2541.37],
///             ["2024-09-02T21:52:00Z", "eth", "usd", 2542.66]
///         ],
///         "dataFormat": "JsonAoA",
///         "schema": {"fields": ["..."]},
///         "schemaFormat": "ArrowJson"
///     },
///     "subQueries": [],
///     "commitment": {
///         "inputHash": "f1620e23f7d8cdde7504eadb86f3cdf34b3b1a7d71f10fe5b54b528dd803387422efc",
///         "outputHash": "f1620e91f4d3fa26bc4ca0c49d681c8b630550239b64d3cbcfd7c6c2d6ff45998b088",
///         "subQueriesHash": "f1620ca4510738395af1429224dd785675309c344b2b549632e20275c69b15ed1d210"
///     },
///     "proof": {
///         "type": "Ed25519Signature2020",
///         "verificationMethod": "did:key:z6MkkhJQPHpA41mTPLFgBeygnjeeADUSwuGDoF9pbGQsfwZp",
///         "proofValue": "uJfY3_g03WbmqlQG8TL-WUxKYU8ZoJaP14MzOzbnJedNiu7jpoKnCTNnDI3TYuaXv89vKlirlGs-5AN06mBseCg"
///     }
/// }
/// ```
///
/// A client that gets a proof in response should
/// perform [a few basic steps](https://docs.kamu.dev/node/commitments#response-validation) to validate
/// the proof integrity. For example making sure that the DID in
/// `proof.verificationMethod` actually corresponds to the node you're querying
/// data from and that the signature in `proof.proofValue` is actually valid.
/// Only after this you can use this proof to hold the node accountable for the
/// result.
///
/// A proof can be stored long-term and then disputed at a later point using
/// your own node or a 3rd party node you can trust via the
/// [`/verify`](#tag/odf-query/POST/verify) endpoint.
///
/// See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.
#[utoipa::path(
    post,
    path = "/query",
    request_body = QueryRequest,
    responses((status = OK, body = QueryResponse)),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn query_handler_post(
    Extension(catalog): Extension<Catalog>,
    Json(body): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    query_handler_impl(catalog, body).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Execute a batch query
///
/// Functions exactly like the [POST version](#tag/odf-query/POST/query) of the
/// endpoint with all parameters passed in the query string instead of the body.
#[utoipa::path(
    get,
    path = "/query",
    params(QueryParams),
    responses((status = OK, body = QueryResponse)),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    ),
)]
#[transactional_handler]
pub async fn query_handler(
    Extension(catalog): Extension<Catalog>,
    Query(params): Query<QueryParams>,
) -> Result<Json<QueryResponse>, ApiError> {
    query_handler_impl(catalog, params.into()).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn query_handler_impl(
    catalog: Catalog,
    mut body: QueryRequest,
) -> Result<Json<QueryResponse>, ApiError> {
    tracing::debug!(request = ?body, "Query");

    // Automatically add `Input` if proof is requested, as proof depends on input
    // for verifiability
    if body.include.contains(&Include::Proof) {
        body.include.insert(Include::Input);
    }
    // Automatically add `Schema` if user specified schema format
    if body.schema_format.is_some() {
        body.include.insert(Include::Schema);
    }

    let identity = catalog.get_one::<IdentityConfig>().ok();
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let res = query_svc
        .sql_statement(&body.query, body.to_options())
        .await
        .map_err(map_query_error)?;

    // Apply pagination limits
    let df = res
        .df
        .limit(
            usize::try_from(body.skip).unwrap(),
            Some(usize::try_from(body.limit).unwrap()),
        )
        .int_err()
        .api_err()?;

    let (schema, schema_format) = if body.include.contains(&Include::Schema) {
        // NOTE: We strip encoding to only leave the logical type
        let odf_schema = odf::schema::DataSchema::new_from_arrow(df.schema().inner())
            .int_err()?
            .strip_encoding();

        let schema_format = body.schema_format.unwrap_or_default();

        (
            Some(Schema::new(&odf_schema, schema_format)?),
            Some(schema_format),
        )
    } else {
        (None, None)
    };

    let record_batches = df.collect().await.int_err().api_err()?;
    let json = serialize_data(&record_batches, body.data_format).api_err()?;

    // TODO: PERF: Avoid re-serializing data
    let data = serde_json::from_str(&json).unwrap();

    let output = Outputs {
        data,
        data_format: body.data_format,
        schema,
        schema_format,
    };

    let include_proof = body.include.contains(&Include::Proof);

    let input =
        if !body.include.contains(&Include::Input) && !body.include.contains(&Include::Proof) {
            None
        } else {
            body.datasets = Some(QueryRequest::query_state_to_datasets(res.state));
            body.schema_format = schema_format;
            Some(body)
        };

    let response = if !include_proof {
        QueryResponse {
            input,
            output,
            sub_queries: None,
            commitment: None,
            proof: None,
        }
    } else if let Some(identity) = identity {
        use ed25519_dalek::Signer;

        let sub_queries = Vec::new();

        // TODO: PERF: There is a large avenue for improvements to avoid
        // re-serialization. We could potentially always serialize signed
        // responses in canonical JSON format to avoid transcoding.
        let commitment = Commitment {
            input_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(&input)),
            output_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(&output)),
            sub_queries_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(
                &sub_queries,
            )),
        };

        let signature = identity.private_key.sign(&to_canonical_json(&commitment));

        QueryResponse {
            input,
            output,
            sub_queries: Some(sub_queries),
            commitment: Some(commitment),
            proof: Some(Proof {
                r#type: ProofType::Ed25519Signature2020,
                verification_method: identity.did(),
                proof_value: signature.into(),
            }),
        }
    } else {
        Err(ApiError::not_implemented(ResponseSigningNotConfigured))?
    };

    Ok(Json(response))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("Response signing is not enabled by the node operator")]
struct ResponseSigningNotConfigured;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

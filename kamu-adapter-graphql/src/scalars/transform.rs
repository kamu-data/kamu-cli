// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::Dataset;
use async_graphql::*;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct TransformInput(odf::TransformInput);

impl From<odf::TransformInput> for TransformInput {
    fn from(v: odf::TransformInput) -> Self {
        Self(v)
    }
}

#[Object]
impl TransformInput {
    async fn dataset(&self, ctx: &Context<'_>) -> Result<Dataset> {
        Dataset::from_ref(ctx, &self.0.id.as_ref().unwrap().as_local_ref())
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub(crate) enum Transform {
    Sql(TransformSql),
}

impl From<odf::Transform> for Transform {
    fn from(t: odf::Transform) -> Self {
        match t {
            odf::Transform::Sql(sql) => Self::Sql(sql.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

// TODO: Avoid excessive cloning
#[derive(Debug, Clone)]
pub(crate) struct TransformSql(odf::TransformSql);

impl From<odf::TransformSql> for TransformSql {
    fn from(v: odf::TransformSql) -> Self {
        Self(v)
    }
}

#[Object]
impl TransformSql {
    async fn engine(&self) -> String {
        self.0.engine.clone()
    }

    async fn queries(&self) -> Vec<SqlQueryStep> {
        if let Some(q) = &self.0.query {
            vec![odf::SqlQueryStep {
                alias: None,
                query: q.clone(),
            }
            .into()]
        } else if let Some(qq) = &self.0.queries {
            qq.iter().cloned().map(|q| q.into()).collect()
        } else {
            Vec::new()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct SqlQueryStep(odf::SqlQueryStep);

impl From<odf::SqlQueryStep> for SqlQueryStep {
    fn from(v: odf::SqlQueryStep) -> Self {
        Self(v)
    }
}

#[Object]
impl SqlQueryStep {
    async fn alias(&self) -> Option<String> {
        self.0.alias.clone()
    }

    async fn query(&self) -> String {
        self.0.query.clone()
    }
}

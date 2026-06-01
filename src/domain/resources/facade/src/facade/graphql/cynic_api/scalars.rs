use chrono::{DateTime, Utc};
use kamu_resources as domain;

use super::schema;

cynic::impl_scalar!(DateTime<Utc>, schema::DateTime);
cynic::impl_scalar!(odf::AccountID, schema::AccountID);
cynic::impl_scalar!(domain::ResourceUID, schema::ResourceID);
cynic::impl_scalar!(serde_json::Value, schema::JSON);

#[derive(cynic::Scalar, Debug, Clone)]
#[cynic(graphql_type = "AccountName")]
pub(crate) struct AccountName(pub(crate) String);

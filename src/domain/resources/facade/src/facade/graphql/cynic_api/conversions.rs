use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources as domain;

use super::{fragments, supported_kinds};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<supported_kinds::ResourceKindDescriptor> for domain::ResourceKindDescriptor {
    type Error = InternalError;

    fn try_from(value: supported_kinds::ResourceKindDescriptor) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            short_names: value.short_names,
            kind: value.kind.value,
            api_version: value.api_version,
            list_columns: value
                .list_columns
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<supported_kinds::ResourceListColumnDescriptor>
    for domain::ResourceListColumnDescriptor
{
    type Error = InternalError;

    fn try_from(value: supported_kinds::ResourceListColumnDescriptor) -> Result<Self, Self::Error> {
        Ok(Self {
            key: value.key,
            header: value.header,
            data_type: super::super::query_builder::parse_enum(
                &value.data_type,
                "list column data type",
            )?,
            visibility: super::super::query_builder::parse_enum(
                &value.visibility,
                "list column visibility",
            )?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<fragments::Resource> for domain::ResourceView {
    type Error = InternalError;

    fn try_from(value: fragments::Resource) -> Result<Self, Self::Error> {
        let labels: std::collections::BTreeMap<String, String> =
            serde_json::from_value(value.metadata.labels).int_err()?;
        let annotations: std::collections::BTreeMap<String, String> =
            serde_json::from_value(value.metadata.annotations).int_err()?;

        Ok(Self {
            kind: value.kind.value,
            api_version: value.api_version,
            account: domain::ResourceViewAccount {
                id: value.metadata.account_id,
                name: None,
            },
            metadata: domain::ResourceViewMetadata {
                uid: value.metadata.id,
                name: value.metadata.name,
                description: value.metadata.description,
                labels,
                annotations,
                generation: u64::try_from(value.metadata.generation).int_err()?,
                created_at: value.metadata.created_at,
                updated_at: value.metadata.updated_at,
                deleted_at: value.metadata.deleted_at,
            },
            last_reconciled_at: value.metadata.last_reconciled_at,
            spec: value.spec,
            status: value.status,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<fragments::ResourceIdentity> for domain::ResourceIdentityView {
    fn from(value: fragments::ResourceIdentity) -> Self {
        Self {
            kind: value.kind.value,
            api_version: value.api_version,
            canonical_kind_name: value.canonical_kind_name,
            uid: value.id,
            name: value.name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<fragments::ResourceRenderManifestResult> for crate::RenderResourceManifestResult {
    fn from(value: fragments::ResourceRenderManifestResult) -> Self {
        Self {
            manifest: value.manifest,
            format: value.format.into(),
        }
    }
}

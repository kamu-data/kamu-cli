// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::auth::AccountRef;
use crate::formats::*;
use crate::resource::{LabelFilter, ResourceID, TypeRef};
use crate::serde::flatbuffers::{
    FlatbuffersDeserializable,
    FlatbuffersSerializable,
    proxies_generated as fb,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Matches zero or many resources using identity and label filters.
///
/// Unlike [`ResourceRef`](crate::resource::ResourceRef), which names exactly
/// one resource, `name` here is a SQL `LIKE` pattern.
///
/// Schema: <https://opendatafabric.org/schemas/resource/v1alpha1/ResourceSelector>
///
/// Excluded from codegen via the schema's `rust.dtoType` hint, so it must stay
/// in sync with the generated serde proxy and flatbuffers table — field order
/// included.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceSelector {
    pub account: Option<AccountRef>,
    pub id: Option<ResourceID>,
    pub did: Option<Did>,
    /// Short type name (`SecretSet`) or full schema URI. `None` spans every
    /// type — the spec made this optional so a listing need not invent a magic
    /// wildcard token for "any type".
    pub r#type: Option<TypeRef>,
    /// SQL `LIKE` pattern. A `String`, not a `ResourceName`, since wildcards
    /// are not valid in a name.
    pub name: Option<String>,
    pub labels: Option<LabelFilter>,
}

impl ResourceSelector {
    pub fn account(&self) -> Option<&AccountRef> {
        self.account.as_ref()
    }

    /// Every resource of `r#type`.
    pub fn of_type(r#type: TypeRef) -> Self {
        Self {
            r#type: Some(r#type),
            ..Default::default()
        }
    }

    /// Resources of `r#type` whose names match a SQL `LIKE` pattern.
    pub fn name_pattern(r#type: TypeRef, pattern: impl Into<String>) -> Self {
        Self {
            name: Some(pattern.into()),
            ..Self::of_type(r#type)
        }
    }

    /// One resource of `r#type`, by id.
    fn id_of_type(r#type: TypeRef, id: ResourceID) -> Self {
        Self {
            id: Some(id),
            ..Self::of_type(r#type)
        }
    }

    /// One resource of any type, by id.
    pub fn any_type_id(id: ResourceID) -> Self {
        Self {
            id: Some(id),
            ..Default::default()
        }
    }

    /// Resources of any type whose names match a SQL `LIKE` pattern.
    pub fn any_type_name_pattern(pattern: impl Into<String>) -> Self {
        Self {
            name: Some(pattern.into()),
            ..Default::default()
        }
    }

    /// One selector per id, all of `r#type`.
    ///
    /// The wire is scalar, so a batch of ids fans out here and is folded back
    /// into a single `ExactIds` row by the facade's coalescer.
    pub fn ids_of_type(r#type: &TypeRef, ids: impl IntoIterator<Item = ResourceID>) -> Vec<Self> {
        ids.into_iter()
            .map(|id| Self::id_of_type(r#type.clone(), id))
            .collect()
    }

    /// One selector per id, spanning every type.
    pub fn any_type_ids(ids: impl IntoIterator<Item = ResourceID>) -> Vec<Self> {
        ids.into_iter().map(Self::any_type_id).collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for ResourceSelector {
    type Err = ::multiformats::ParseError<Self>;

    /// Parses the short form `Type:[account/]namePattern`.
    ///
    /// Mirrors `ResourceRef`'s short form, but the name position accepts a
    /// `LIKE` pattern. Cannot express `id`, `did` or `labels`.
    ///
    /// The type position stays mandatory in the grammar even though the field
    /// is now optional: a type-less selector is reachable by construction, not
    /// by short form, since dropping the type would leave nothing to anchor
    /// the `Type:` separator.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((typ, account, name)) = Grammar::match_resource_selector(s) else {
            return Err(::multiformats::ParseError::<Self>::new(s));
        };

        Ok(Self {
            account: account.map(|s| crate::auth::AccountName::new_unchecked(s).into()),
            id: None,
            did: None,
            r#type: Some(crate::resource::TypeName::new_unchecked(typ).into()),
            name: Some(name.to_string()),
            labels: None,
        })
    }
}

impl_parse_error!(ResourceSelector);
impl_try_from_str!(ResourceSelector);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'fb> FlatbuffersSerializable<'fb> for ResourceSelector {
    type OffsetT = WIPOffset<fb::ResourceSelector<'fb>>;

    fn serialize(&self, fb: &mut FlatBufferBuilder<'fb>) -> Self::OffsetT {
        let account_offset = self.account.as_ref().map(|v| v.serialize(fb));
        let labels_offset = self.labels.as_ref().map(|v| v.serialize(fb));
        let id_offset = self.id.as_ref().map(|v| fb.create_vector(v.as_bytes()));
        let did_offset = self.did.as_ref().map(|v| fb.create_vector(v.as_bytes()));
        let type_offset = self.r#type.as_ref().map(|v| fb.create_string(v.as_str()));
        let name_offset = self.name.as_ref().map(|v| fb.create_string(v));

        let mut builder = fb::ResourceSelectorBuilder::new(fb);
        if let Some(off) = account_offset {
            builder.add_account(off);
        }
        if let Some(off) = id_offset {
            builder.add_id(off);
        }
        if let Some(off) = did_offset {
            builder.add_did(off);
        }
        if let Some(off) = type_offset {
            builder.add_type_(off);
        }
        if let Some(off) = name_offset {
            builder.add_name(off);
        }
        if let Some(off) = labels_offset {
            builder.add_labels(off);
        }
        builder.finish()
    }
}

impl<'fb> FlatbuffersDeserializable<fb::ResourceSelector<'fb>> for ResourceSelector {
    fn deserialize(proxy: fb::ResourceSelector<'fb>) -> Self {
        Self {
            account: proxy.account().map(AccountRef::deserialize),
            id: proxy
                .id()
                .map(|v| ResourceID::from_bytes(v.bytes()).unwrap()),
            did: proxy.did().map(|v| Did::from_bytes(v.bytes()).unwrap()),
            r#type: proxy.type_().map(|v| TypeRef::try_from(v).unwrap()),
            name: proxy.name().map(ToString::to_string),
            labels: proxy.labels().map(LabelFilter::deserialize),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use pretty_assertions::assert_eq;

    use super::*;

    fn parse(s: &str) -> ResourceSelector {
        s.parse().unwrap()
    }

    #[test]
    fn test_parse_type_and_exact_name() {
        let sel = parse("SecretSet:my-secrets");
        assert_eq!(sel.r#type.as_ref().map(TypeRef::as_str), Some("SecretSet"));
        assert_eq!(sel.name.as_deref(), Some("my-secrets"));
        assert!(sel.account.is_none());
        assert!(sel.id.is_none());
        assert!(sel.did.is_none());
        assert!(sel.labels.is_none());
    }

    // The name position is what separates a selector from a ref.
    #[test]
    fn test_parse_name_patterns() {
        assert_eq!(parse("SecretSet:%").name.as_deref(), Some("%"));
        assert_eq!(
            parse("SecretSet:my-resource-%").name.as_deref(),
            Some("my-resource-%")
        );
        assert_eq!(parse("SecretSet:%-prod").name.as_deref(), Some("%-prod"));
    }

    #[test]
    fn test_parse_with_account() {
        let sel = parse("SecretSet:alice/app-%");
        assert_eq!(sel.r#type.as_ref().map(TypeRef::as_str), Some("SecretSet"));
        assert_eq!(sel.name.as_deref(), Some("app-%"));
        assert_matches!(
            sel.account,
            Some(AccountRef { name: Some(ref n), .. }) if n.as_str() == "alice"
        );
    }

    #[test]
    fn test_parse_rejects_malformed() {
        for input in [
            "",
            "SecretSet",            // no `:` separator
            ":my-secrets",          // no type
            "SecretSet:",           // no name
            "Secret Set:my-secret", // space in type
            "SecretSet:a/b/c",      // too many separators
        ] {
            assert_matches!(
                input.parse::<ResourceSelector>(),
                Err(_),
                "expected `{input}` to be rejected"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

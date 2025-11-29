// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::dtos::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum DataSchemaCmp<'a> {
    /// Schemas are identical twins in both types and attributes
    Identical,
    /// Schemas are equivalent according to specified options
    Equivalent(DataSchemaDiff<'a>),
    /// Schemas are different - diff is provided
    NonEquivalent(DataSchemaDiff<'a>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct DataSchemaCmpOptions {
    /// Differences in field order will be treated as equivalent
    pub ignore_order: bool,

    /// Differences in type optionality will be treated as equivalent
    pub ignore_optionality: bool,

    /// Differences in attributes will be treated as equivalent
    pub ignore_attributes: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DataSchemaDiff<'a> {
    pub lhs: &'a DataSchema,
    pub rhs: &'a DataSchema,
    pub items: Vec<DataSchemaDiffItem<'a>>,
}

#[derive(Debug)]
pub enum DataSchemaDiffItem<'a> {
    SchemaAttributesChanged,
    FieldAttributesChanged,
    FieldAdded {
        field_rhs: &'a DataField,
    },
    FieldRemoved,
    FieldReordered {
        index_lhs: usize,
        index_rhs: usize,
        field_lhs: &'a DataField,
        field_rhs: &'a DataField,
    },
    FieldTypeChanged,
    FieldBecameOptional,
    FieldBecameRequired,
    NestedFieldDiff {
        field_lhs: &'a DataField,
        field_rhs: &'a DataField,
        items: Vec<DataSchemaDiffItem<'a>>,
    },
}

impl<'a> DataSchemaDiff<'a> {
    fn compare(self, opts: &DataSchemaCmpOptions) -> DataSchemaCmp<'a> {
        if self.items.is_empty() {
            DataSchemaCmp::Identical
        } else if Self::is_equivalent_rec(&self.items, opts) {
            DataSchemaCmp::Equivalent(self)
        } else {
            DataSchemaCmp::NonEquivalent(self)
        }
    }

    fn is_equivalent_rec(items: &Vec<DataSchemaDiffItem<'a>>, opts: &DataSchemaCmpOptions) -> bool {
        for item in items {
            #[allow(clippy::match_same_arms)]
            match item {
                DataSchemaDiffItem::SchemaAttributesChanged if !opts.ignore_attributes => {
                    return false
                }
                DataSchemaDiffItem::FieldAttributesChanged if !opts.ignore_attributes => {
                    return false
                }
                DataSchemaDiffItem::FieldAdded { .. } => return false,
                DataSchemaDiffItem::FieldRemoved => return false,
                DataSchemaDiffItem::FieldReordered { .. } if !opts.ignore_order => return false,
                DataSchemaDiffItem::FieldTypeChanged => return false,
                DataSchemaDiffItem::FieldBecameOptional if !opts.ignore_optionality => {
                    return false
                }
                DataSchemaDiffItem::FieldBecameRequired if !opts.ignore_optionality => {
                    return false
                }
                DataSchemaDiffItem::NestedFieldDiff {
                    items: items_nested,
                    ..
                } => {
                    if !Self::is_equivalent_rec(items_nested, opts) {
                        return false;
                    }
                }
                _ => {}
            }
        }

        true
    }

    fn is_superset(&self, opts: &DataSchemaCmpOptions) -> bool {
        Self::is_superset_rec(&self.items, opts)
    }

    fn is_superset_rec(items: &Vec<DataSchemaDiffItem<'a>>, opts: &DataSchemaCmpOptions) -> bool {
        for item in items {
            #[expect(clippy::match_same_arms)]
            match item {
                DataSchemaDiffItem::SchemaAttributesChanged if !opts.ignore_attributes => {
                    return false
                }
                DataSchemaDiffItem::FieldAttributesChanged if !opts.ignore_attributes => {
                    return false
                }
                DataSchemaDiffItem::FieldAdded { .. } => return false,
                DataSchemaDiffItem::FieldRemoved => {}
                DataSchemaDiffItem::FieldReordered { .. } if !opts.ignore_order => return false,
                DataSchemaDiffItem::FieldTypeChanged => return false,
                DataSchemaDiffItem::FieldBecameOptional if !opts.ignore_optionality => {
                    return false
                }
                DataSchemaDiffItem::FieldBecameRequired if !opts.ignore_optionality => {
                    return false
                }
                DataSchemaDiffItem::NestedFieldDiff {
                    items: items_nested,
                    ..
                } => {
                    if !Self::is_superset_rec(items_nested, opts) {
                        return false;
                    }
                }
                _ => {}
            }
        }

        true
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataSchema {
    #[expect(clippy::needless_pass_by_value)]
    pub fn compare<'a>(&'a self, other: &'a Self, opts: DataSchemaCmpOptions) -> DataSchemaCmp<'a> {
        self.diff(other).compare(&opts)
    }

    pub fn diff<'a>(&'a self, other: &'a Self) -> DataSchemaDiff<'a> {
        let (
            DataSchema {
                fields: fields_lhs,
                extra: extra_lhs,
            },
            DataSchema {
                fields: fields_rhs,
                extra: extra_rhs,
            },
        ) = (self, other);

        let items = Self::diff_impl(
            fields_lhs,
            extra_lhs.as_ref(),
            fields_rhs,
            extra_rhs.as_ref(),
        );

        DataSchemaDiff {
            lhs: self,
            rhs: other,
            items,
        }
    }

    fn diff_impl<'a>(
        fields_lhs: &'a [DataField],
        extra_lhs: Option<&'a ExtraAttributes>,
        fields_rhs: &'a [DataField],
        extra_rhs: Option<&'a ExtraAttributes>,
    ) -> Vec<DataSchemaDiffItem<'a>> {
        let mut items = Vec::new();

        if extra_lhs != extra_rhs {
            items.push(DataSchemaDiffItem::SchemaAttributesChanged);
        }

        // Check fast case where number of fields is same and names match
        let fields_cmp: Vec<(&DataField, &DataField)> = if fields_lhs.len() == fields_rhs.len()
            && fields_lhs
                .iter()
                .zip(fields_rhs)
                .all(|(lf, rf)| lf.name == rf.name)
        {
            fields_lhs.iter().zip(fields_rhs).collect()
        } else {
            // Fall back to complex case
            let mut fields_cmp = Vec::new();

            let mut fields_rhs_map: std::collections::BTreeMap<&str, (usize, &DataField)> =
                fields_rhs
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name.as_str(), (i, f)))
                    .collect();

            for (index_lhs, field_lhs) in fields_lhs.iter().enumerate() {
                if let Some((index_rhs, field_rhs)) = fields_rhs_map.remove(field_lhs.name.as_str())
                {
                    if index_lhs != index_rhs {
                        items.push(DataSchemaDiffItem::FieldReordered {
                            index_lhs,
                            index_rhs,
                            field_lhs,
                            field_rhs,
                        });
                    }
                    fields_cmp.push((field_lhs, field_rhs));
                } else {
                    items.push(DataSchemaDiffItem::FieldRemoved);
                }
            }
            for (_, field_rhs) in fields_rhs_map.values() {
                items.push(DataSchemaDiffItem::FieldAdded { field_rhs });
            }

            Vec::new()
        };

        for (field_lhs, field_rhs) in fields_cmp {
            assert_eq!(field_lhs.name, field_rhs.name);

            // Compare data types
            let diff = match (&field_lhs.r#type, &field_rhs.r#type) {
                (
                    DataType::Struct(DataTypeStruct { fields: fields_lhs }),
                    DataType::Struct(DataTypeStruct { fields: fields_rhs }),
                ) => {
                    let items = Self::diff_impl(fields_lhs, None, fields_rhs, None);
                    if items.is_empty() {
                        None
                    } else {
                        Some(DataSchemaDiffItem::NestedFieldDiff {
                            field_lhs,
                            field_rhs,
                            items,
                        })
                    }
                }
                (type_lhs, type_rhs) if *type_lhs == *type_rhs => None,
                (DataType::Option(DataTypeOption { inner: inner_lhs }), rhs)
                    if **inner_lhs == *rhs =>
                {
                    Some(DataSchemaDiffItem::FieldBecameRequired)
                }
                (lhs, DataType::Option(DataTypeOption { inner: inner_rhs }))
                    if *lhs == **inner_rhs =>
                {
                    Some(DataSchemaDiffItem::FieldBecameOptional)
                }
                (_, _) => Some(DataSchemaDiffItem::FieldTypeChanged),
            };

            if let Some(diff) = diff {
                items.push(diff);
            }

            if field_lhs.extra != field_rhs.extra {
                items.push(DataSchemaDiffItem::FieldAttributesChanged);
            }
        }

        items
    }

    /// Checks whether current schema contains all fields of another
    #[expect(clippy::needless_pass_by_value)]
    pub fn is_superset_of(&self, other: &Self, opts: DataSchemaCmpOptions) -> bool {
        Self::diff(self, other).is_superset(&opts)
    }

    /// Checks whether all fields of the current schema are fully contained in
    /// another
    pub fn is_subset_of(&self, other: &Self, opts: DataSchemaCmpOptions) -> bool {
        other.is_superset_of(self, opts)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;

    use serde_json::json;

    use crate::*;

    #[test]
    fn test_schema_cmp_compare() {
        // Empty
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![]),
                &DataSchema::new(vec![]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: schema attrs
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![]),
                &DataSchema::new_with_attrs(
                    vec![],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "a"})).unwrap()
                ),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![]),
                &DataSchema::new_with_attrs(
                    vec![],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "a"})).unwrap()
                ),
                DataSchemaCmpOptions {
                    ignore_attributes: true,
                    ..Default::default()
                },
            ),
            DataSchemaCmp::Equivalent(_)
        );

        // Identical: types
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: types
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u64("foo")]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );

        // Different: names
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("bar")]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );

        // Identical: attrs
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new_with_attrs(
                    vec![DataField::u32("foo")],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "a"})).unwrap()
                ),
                &DataSchema::new_with_attrs(
                    vec![DataField::u32("foo")],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "a"})).unwrap()
                ),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: attrs
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new_with_attrs(
                    vec![DataField::u32("foo")],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "a"})).unwrap()
                ),
                &DataSchema::new_with_attrs(
                    vec![DataField::u32("foo")],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "b"})).unwrap()
                ),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new_with_attrs(
                    vec![DataField::u32("foo")],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "a"})).unwrap()
                ),
                &DataSchema::new_with_attrs(
                    vec![DataField::u32("foo")],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "b"})).unwrap()
                ),
                DataSchemaCmpOptions {
                    ignore_attributes: true,
                    ..Default::default()
                },
            ),
            DataSchemaCmp::Equivalent(_)
        );

        // Identical: nested types
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: nested types
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u64("a")])]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );

        // Different: nested names
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("b")])]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );

        // Different: nested attrs
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure(
                    "foo",
                    vec![DataField::u32("a").extra_json(json!({"foo.com/bar": "a"}))]
                )]),
                &DataSchema::new(vec![DataField::structure(
                    "foo",
                    vec![DataField::u32("a").extra_json(json!({"foo.com/bar": "b"}))]
                )]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure(
                    "foo",
                    vec![DataField::u32("a").extra_json(json!({"foo.com/bar": "a"}))]
                )]),
                &DataSchema::new(vec![DataField::structure(
                    "foo",
                    vec![DataField::u32("a").extra_json(json!({"foo.com/bar": "b"}))]
                )]),
                DataSchemaCmpOptions {
                    ignore_attributes: true,
                    ..Default::default()
                },
            ),
            DataSchemaCmp::Equivalent(_)
        );

        // Identical: optionality
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: optionality
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent(_)
        );
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                DataSchemaCmpOptions {
                    ignore_optionality: true,
                    ..Default::default()
                },
            ),
            DataSchemaCmp::Equivalent(_)
        );
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions {
                    ignore_optionality: true,
                    ..Default::default()
                },
            ),
            DataSchemaCmp::Equivalent(_)
        );
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn test_schema_cmp_is_superset() {
        // Empty
        assert_eq!(
            DataSchema::is_superset_of(
                &DataSchema::new(vec![]),
                &DataSchema::new(vec![]),
                DataSchemaCmpOptions::default(),
            ),
            true
        );

        // Identical
        assert_eq!(
            DataSchema::is_superset_of(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions::default(),
            ),
            true
        );

        // Different attributes (check attributes)
        assert_eq!(
            DataSchema::is_superset_of(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![
                    DataField::u32("foo").extra_json(json!({"foo.com/bar": "a"}))
                ]),
                DataSchemaCmpOptions::default(),
            ),
            false
        );

        // Different attributes (ignore attributes)
        assert_eq!(
            DataSchema::is_superset_of(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![
                    DataField::u32("foo").extra_json(json!({"foo.com/bar": "a"}))
                ]),
                DataSchemaCmpOptions {
                    ignore_attributes: true,
                    ..Default::default()
                },
            ),
            true
        );

        // One new field at the end
        assert_eq!(
            DataSchema::is_superset_of(
                &DataSchema::new(vec![DataField::u32("foo"), DataField::u32("bar")]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions::default(),
            ),
            true
        );

        // One new field at the start (check order)
        assert_eq!(
            DataSchema::is_superset_of(
                &DataSchema::new(vec![DataField::u32("bar"), DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions::default(),
            ),
            false
        );

        // One new field at the start (ignore order)
        assert_eq!(
            DataSchema::is_superset_of(
                &DataSchema::new(vec![DataField::u32("bar"), DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions {
                    ignore_order: true,
                    ..Default::default()
                },
            ),
            true
        );
    }

    #[test]
    fn test_schema_cmp_diff() {
        // One new field at the end
        let lhs = DataSchema::new(vec![DataField::u32("foo")]);
        let rhs = DataSchema::new(vec![DataField::u32("foo"), DataField::u32("bar")]);
        let diff = DataSchema::diff(&lhs, &rhs);

        assert_matches!(
            diff.items[..],
            [
                DataSchemaDiffItem::FieldAdded { field_rhs }
            ] if field_rhs.name == "bar"
        );

        // One new field at the start
        let lhs = DataSchema::new(vec![DataField::u32("foo")]);
        let rhs = DataSchema::new(vec![DataField::u32("bar"), DataField::u32("foo")]);
        let diff = DataSchema::diff(&lhs, &rhs);

        assert_matches!(
            diff.items[..],
            [
                DataSchemaDiffItem::FieldReordered { index_lhs: 0, index_rhs: 1, .. },
                DataSchemaDiffItem::FieldAdded { field_rhs },
            ] if field_rhs.name == "bar"
        );
    }
}

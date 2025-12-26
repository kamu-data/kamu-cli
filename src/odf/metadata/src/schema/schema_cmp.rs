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

#[derive(Debug, Clone)]
pub struct DataTypeDiff<'a> {
    pub items: Vec<DataTypeDiffItem<'a>>,
}

#[derive(Debug, Clone)]
pub enum DataTypeDiffItem<'a> {
    Changed {
        lhs: &'a DataType,
        rhs: &'a DataType,
    },
    BecameOptional {
        lhs: &'a DataType,
        rhs: &'a DataType,
    },
    BecameRequired {
        lhs: &'a DataType,
        rhs: &'a DataType,
    },
    StructDiff {
        lhs: &'a DataTypeStruct,
        rhs: &'a DataTypeStruct,
        fields_diff: Vec<DataSchemaDiffItem<'a>>,
    },
    ListDiff {
        lhs: &'a DataTypeList,
        rhs: &'a DataTypeList,
        item_type_diff: Vec<DataTypeDiffItem<'a>>,
    },
}

impl DataType {
    pub fn diff<'a>(&'a self, other: &'a Self) -> DataTypeDiff<'a> {
        let mut items = Vec::new();
        Self::diff_impl(self, other, &mut items);
        DataTypeDiff { items }
    }

    fn diff_impl<'a>(lhs: &'a Self, rhs: &'a Self, diff: &mut Vec<DataTypeDiffItem<'a>>) {
        match (lhs, rhs) {
            (lhs, rhs) if *lhs == *rhs => {}
            (
                DataType::Option(DataTypeOption { inner: inner_lhs }),
                DataType::Option(DataTypeOption { inner: inner_rhs }),
            ) => {
                Self::diff_impl(inner_lhs, inner_rhs, diff);
            }
            (lhs, DataType::Option(DataTypeOption { inner: inner_rhs })) => {
                diff.push(DataTypeDiffItem::BecameOptional { lhs, rhs });
                Self::diff_impl(lhs, inner_rhs, diff);
            }
            (DataType::Option(DataTypeOption { inner: inner_lhs }), rhs) => {
                diff.push(DataTypeDiffItem::BecameRequired { lhs, rhs });
                Self::diff_impl(inner_lhs, rhs, diff);
            }
            (
                DataType::Struct(lhs @ DataTypeStruct { fields: fields_lhs }),
                DataType::Struct(rhs @ DataTypeStruct { fields: fields_rhs }),
            ) => {
                let fields_diff = DataSchema::diff_impl(fields_lhs, None, fields_rhs, None);
                if !fields_diff.is_empty() {
                    diff.push(DataTypeDiffItem::StructDiff {
                        lhs,
                        rhs,
                        fields_diff,
                    });
                }
            }
            (
                DataType::List(
                    lhs_t @ DataTypeList {
                        item_type: item_type_lhs,
                        fixed_length: fixed_length_lhs,
                    },
                ),
                DataType::List(
                    rhs_t @ DataTypeList {
                        item_type: item_type_rhs,
                        fixed_length: fixed_length_rhs,
                    },
                ),
            ) => {
                let item_type_diff = Self::diff(item_type_lhs, item_type_rhs).items;

                if item_type_diff.is_empty() {
                    assert_ne!(fixed_length_lhs, fixed_length_rhs);
                    diff.push(DataTypeDiffItem::Changed { lhs, rhs });
                } else {
                    diff.push(DataTypeDiffItem::ListDiff {
                        lhs: lhs_t,
                        rhs: rhs_t,
                        item_type_diff,
                    });
                }
            }
            (_, _) => diff.push(DataTypeDiffItem::Changed { lhs, rhs }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum DataSchemaCmp<'a> {
    /// Schemas are identical twins in both types and attributes
    Identical,
    /// Schemas are equivalent according to specified options
    Equivalent { diff_orig: DataSchemaDiff<'a> },
    /// Schemas are different
    NonEquivalent {
        diff_orig: DataSchemaDiff<'a>,
        diff_non_equivalent: DataSchemaDiff<'a>,
    },
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

#[derive(Debug, Clone)]
pub struct DataSchemaDiff<'a> {
    pub items: Vec<DataSchemaDiffItem<'a>>,
}

#[derive(Debug, Clone)]
pub enum DataSchemaDiffItem<'a> {
    SchemaAttributesChanged,
    FieldAttributesChanged {
        field_lhs: &'a DataField,
        field_rhs: &'a DataField,
    },
    FieldAdded {
        field_rhs: &'a DataField,
    },
    FieldRemoved {
        field_lhs: &'a DataField,
    },
    FieldReordered {
        index_lhs: usize,
        index_rhs: usize,
        field_lhs: &'a DataField,
        field_rhs: &'a DataField,
    },
    FieldTypeChanged {
        field_lhs: &'a DataField,
        field_rhs: &'a DataField,
        type_diff: Vec<DataTypeDiffItem<'a>>,
    },
}

impl<'a> DataSchemaDiff<'a> {
    fn compare(self, opts: &DataSchemaCmpOptions) -> DataSchemaCmp<'a> {
        if self.items.is_empty() {
            DataSchemaCmp::Identical
        } else {
            let non_eq = Self::filter_equivalent(&self.items, opts);

            if non_eq.is_empty() {
                DataSchemaCmp::Equivalent { diff_orig: self }
            } else {
                DataSchemaCmp::NonEquivalent {
                    diff_non_equivalent: DataSchemaDiff { items: non_eq },
                    diff_orig: self,
                }
            }
        }
    }

    fn filter_equivalent(
        items: &Vec<DataSchemaDiffItem<'a>>,
        opts: &DataSchemaCmpOptions,
    ) -> Vec<DataSchemaDiffItem<'a>> {
        items
            .iter()
            .filter_map(|i| Self::filter_equivalent_item(i, opts))
            .collect()
    }

    fn filter_equivalent_item(
        item: &DataSchemaDiffItem<'a>,
        opts: &DataSchemaCmpOptions,
    ) -> Option<DataSchemaDiffItem<'a>> {
        #[expect(clippy::match_same_arms)]
        let eq = match item {
            DataSchemaDiffItem::SchemaAttributesChanged => opts.ignore_attributes,
            DataSchemaDiffItem::FieldAttributesChanged { .. } => opts.ignore_attributes,
            DataSchemaDiffItem::FieldAdded { .. } => false,
            DataSchemaDiffItem::FieldRemoved { .. } => false,
            DataSchemaDiffItem::FieldReordered { .. } => opts.ignore_order,
            DataSchemaDiffItem::FieldTypeChanged {
                type_diff,
                field_lhs,
                field_rhs,
            } => {
                let type_diff = Self::filter_equivalent_type_items(type_diff, opts);
                return if type_diff.is_empty() {
                    None
                } else {
                    Some(DataSchemaDiffItem::FieldTypeChanged {
                        field_lhs,
                        field_rhs,
                        type_diff,
                    })
                };
            }
        };

        if eq { None } else { Some(item.clone()) }
    }

    fn filter_equivalent_type_items(
        type_diff: &Vec<DataTypeDiffItem<'a>>,
        opts: &DataSchemaCmpOptions,
    ) -> Vec<DataTypeDiffItem<'a>> {
        type_diff
            .iter()
            .filter_map(|i| Self::filter_equivalent_type(i, opts))
            .collect()
    }

    fn filter_equivalent_type(
        type_diff: &DataTypeDiffItem<'a>,
        opts: &DataSchemaCmpOptions,
    ) -> Option<DataTypeDiffItem<'a>> {
        #[expect(clippy::match_same_arms)]
        let eq = match type_diff {
            DataTypeDiffItem::Changed { .. } => false,
            DataTypeDiffItem::BecameOptional { .. } => opts.ignore_optionality,
            DataTypeDiffItem::BecameRequired { .. } => opts.ignore_optionality,
            DataTypeDiffItem::StructDiff {
                fields_diff,
                lhs,
                rhs,
            } => {
                let fields_diff = Self::filter_equivalent(fields_diff, opts);
                return if fields_diff.is_empty() {
                    None
                } else {
                    Some(DataTypeDiffItem::StructDiff {
                        lhs,
                        rhs,
                        fields_diff,
                    })
                };
            }
            DataTypeDiffItem::ListDiff {
                item_type_diff,
                lhs,
                rhs,
            } => {
                let item_type_diff = Self::filter_equivalent_type_items(item_type_diff, opts);
                return if item_type_diff.is_empty() {
                    None
                } else {
                    Some(DataTypeDiffItem::ListDiff {
                        lhs,
                        rhs,
                        item_type_diff,
                    })
                };
            }
        };

        if eq { None } else { Some(type_diff.clone()) }
    }

    fn is_superset(&self, opts: &DataSchemaCmpOptions) -> bool {
        Self::is_superset_rec(&self.items, opts)
    }

    fn is_superset_rec(items: &Vec<DataSchemaDiffItem<'a>>, opts: &DataSchemaCmpOptions) -> bool {
        items.iter().all(|i| Self::is_superset_schema_diff(i, opts))
    }

    fn is_superset_schema_diff(item: &DataSchemaDiffItem<'a>, opts: &DataSchemaCmpOptions) -> bool {
        #[expect(clippy::match_same_arms)]
        match item {
            DataSchemaDiffItem::SchemaAttributesChanged => opts.ignore_attributes,
            DataSchemaDiffItem::FieldAttributesChanged { .. } => opts.ignore_attributes,
            DataSchemaDiffItem::FieldAdded { .. } => false,
            DataSchemaDiffItem::FieldRemoved { .. } => true, // !
            DataSchemaDiffItem::FieldReordered { .. } => opts.ignore_order,
            DataSchemaDiffItem::FieldTypeChanged { type_diff, .. } => type_diff
                .iter()
                .all(|i| Self::is_superset_type_diff(i, opts)),
        }
    }

    fn is_superset_type_diff(
        type_diff: &DataTypeDiffItem<'a>,
        opts: &DataSchemaCmpOptions,
    ) -> bool {
        #[expect(clippy::match_same_arms)]
        match type_diff {
            DataTypeDiffItem::Changed { .. } => false,
            DataTypeDiffItem::BecameOptional { .. } => opts.ignore_optionality,
            DataTypeDiffItem::BecameRequired { .. } => opts.ignore_optionality,
            DataTypeDiffItem::StructDiff { fields_diff, .. } => {
                Self::is_superset_rec(fields_diff, opts)
            }
            DataTypeDiffItem::ListDiff { item_type_diff, .. } => item_type_diff
                .iter()
                .all(|i| Self::is_superset_type_diff(i, opts)),
        }
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

        DataSchemaDiff { items }
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
                    items.push(DataSchemaDiffItem::FieldRemoved { field_lhs });
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
            let type_diff = DataType::diff(&field_lhs.r#type, &field_rhs.r#type).items;

            if !type_diff.is_empty() {
                items.push(DataSchemaDiffItem::FieldTypeChanged {
                    field_lhs,
                    field_rhs,
                    type_diff,
                });
            }

            if field_lhs.extra != field_rhs.extra {
                items.push(DataSchemaDiffItem::FieldAttributesChanged {
                    field_lhs,
                    field_rhs,
                });
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
            DataSchemaCmp::NonEquivalent { .. }
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
            DataSchemaCmp::Equivalent { .. }
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
            DataSchemaCmp::NonEquivalent { .. }
        );

        // Different: names
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("bar")]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent { .. }
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
            DataSchemaCmp::NonEquivalent { .. }
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
            DataSchemaCmp::Equivalent { .. }
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
            DataSchemaCmp::NonEquivalent { .. }
        );

        // Different: nested names
        assert_matches!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("b")])]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::NonEquivalent { .. }
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
            DataSchemaCmp::NonEquivalent { .. }
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
            DataSchemaCmp::Equivalent { .. }
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
            DataSchemaCmp::NonEquivalent { .. }
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
            DataSchemaCmp::Equivalent { .. }
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
            DataSchemaCmp::Equivalent { .. }
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

        // Struct field optionality changed
        let lhs = DataSchema::new(vec![DataField::structure(
            "foo",
            vec![DataField::u32("a").optional()],
        )]);
        let rhs = DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]);
        let diff = DataSchema::diff(&lhs, &rhs);

        assert_matches!(
            &diff.items[..],
            [
                DataSchemaDiffItem::FieldTypeChanged {
                    field_lhs,
                    field_rhs,
                    type_diff,
                }
            ] if field_lhs.name == "foo"
                && field_rhs.name == "foo"
                && matches!(
                    &type_diff[..],
                    [DataTypeDiffItem::StructDiff { fields_diff, .. }]
                    if matches!(
                        &fields_diff[..],
                        [
                            DataSchemaDiffItem::FieldTypeChanged {
                                field_lhs,
                                field_rhs,
                                type_diff,
                            }
                        ] if field_lhs.name == "a"
                            && field_rhs.name == "a"
                            && matches!(
                                &type_diff[..],
                                [DataTypeDiffItem::BecameRequired { .. }]
                            )
                    )
                )
        );

        // List items optionality changed
        let lhs = DataSchema::new(vec![DataField::list("foo", DataType::u32().optional())]);
        let rhs = DataSchema::new(vec![DataField::list("foo", DataType::u32())]);
        let diff = DataSchema::diff(&lhs, &rhs);

        assert_matches!(
            &diff.items[..],
            [
                DataSchemaDiffItem::FieldTypeChanged {
                    field_lhs,
                    field_rhs,
                    type_diff,
                }
            ] if field_lhs.name == "foo"
                && field_rhs.name == "foo"
                && matches!(
                    &type_diff[..],
                    [
                        DataTypeDiffItem::ListDiff { item_type_diff, .. }
                    ] if matches!(
                        &item_type_diff[..],
                        [DataTypeDiffItem::BecameRequired { .. }]
                    )
                )
        );

        // Both list items optionality and optionality of list field itself changed
        let lhs = DataSchema::new(vec![
            DataField::list("foo", DataType::u32().optional()).optional(),
        ]);
        let rhs = DataSchema::new(vec![DataField::list("foo", DataType::u32())]);
        let diff = DataSchema::diff(&lhs, &rhs);

        assert_matches!(
            &diff.items[..],
            [
                DataSchemaDiffItem::FieldTypeChanged {
                    field_lhs,
                    field_rhs,
                    type_diff,
                }
            ] if field_lhs.name == "foo"
                && field_rhs.name == "foo"
                && matches!(
                    &type_diff[..],
                    [
                        DataTypeDiffItem::BecameRequired { .. },
                        DataTypeDiffItem::ListDiff { item_type_diff, .. },
                    ] if matches!(
                        &item_type_diff[..],
                        [DataTypeDiffItem::BecameRequired { .. }]
                    )
                )
        );
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataSchemaCmp {
    /// Schemas are identical twins in both types and attributes
    Identical,
    /// Schemas are equivalent according to specified options
    Equivalent,
    Incompatible,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, Default)]
pub struct DataSchemaCmpOptions {
    /// Differences in type optionality will be treated as equivalent
    pub ignore_optionality: bool,

    /// Differences in attributes will be treated as equivalent
    pub ignore_attributes: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataSchema {
    pub fn compare(&self, other: &Self, opts: DataSchemaCmpOptions) -> DataSchemaCmp {
        let mut res = DataSchemaCmp::Identical;

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

        if *extra_lhs != *extra_rhs {
            if opts.ignore_attributes {
                res = DataSchemaCmp::Equivalent;
            } else {
                return DataSchemaCmp::Incompatible;
            }
        }

        if fields_lhs.len() != fields_rhs.len() {
            return DataSchemaCmp::Incompatible;
        }

        for (field_lhs, field_rhs) in fields_lhs.iter().zip(fields_rhs) {
            match DataField::compare(field_lhs, field_rhs, opts) {
                DataSchemaCmp::Identical => (),
                DataSchemaCmp::Equivalent => res = DataSchemaCmp::Equivalent,
                DataSchemaCmp::Incompatible => return DataSchemaCmp::Incompatible,
            }
        }

        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataField {
    pub fn compare(&self, other: &Self, opts: DataSchemaCmpOptions) -> DataSchemaCmp {
        let mut res = DataSchemaCmp::Identical;

        let (
            DataField {
                name: name_lhs,
                r#type: typ_lhs,
                extra: extra_lhs,
            },
            DataField {
                name: name_rhs,
                r#type: typ_rhs,
                extra: extra_rhs,
            },
        ) = (self, other);

        if name_lhs != name_rhs {
            return DataSchemaCmp::Incompatible;
        }

        match DataType::compare(typ_lhs, typ_rhs, opts) {
            DataSchemaCmp::Identical => (),
            DataSchemaCmp::Equivalent => res = DataSchemaCmp::Equivalent,
            DataSchemaCmp::Incompatible => return DataSchemaCmp::Incompatible,
        }

        if *extra_lhs != *extra_rhs {
            if opts.ignore_attributes {
                res = DataSchemaCmp::Equivalent;
            } else {
                return DataSchemaCmp::Incompatible;
            }
        }

        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataType {
    pub fn compare(&self, other: &Self, opts: DataSchemaCmpOptions) -> DataSchemaCmp {
        match (self, other) {
            (
                DataType::Struct(DataTypeStruct { fields: fields_lhs }),
                DataType::Struct(DataTypeStruct { fields: fields_rhs }),
            ) => {
                let mut res = DataSchemaCmp::Identical;

                if fields_lhs.len() != fields_rhs.len() {
                    return DataSchemaCmp::Incompatible;
                }

                for (field_lhs, field_rhs) in fields_lhs.iter().zip(fields_rhs) {
                    match DataField::compare(field_lhs, field_rhs, opts) {
                        DataSchemaCmp::Identical => (),
                        DataSchemaCmp::Equivalent => res = DataSchemaCmp::Equivalent,
                        DataSchemaCmp::Incompatible => return DataSchemaCmp::Incompatible,
                    }
                }

                res
            }
            (_, _) if *self == *other => DataSchemaCmp::Identical,
            (DataType::Option(DataTypeOption { inner: inner_lhs }), _)
                if opts.ignore_optionality =>
            {
                match DataType::compare(inner_lhs, other, opts) {
                    DataSchemaCmp::Identical | DataSchemaCmp::Equivalent => {
                        DataSchemaCmp::Equivalent
                    }
                    DataSchemaCmp::Incompatible => DataSchemaCmp::Incompatible,
                }
            }
            (_, DataType::Option(DataTypeOption { inner: inner_rhs }))
                if opts.ignore_optionality =>
            {
                match DataType::compare(self, inner_rhs, opts) {
                    DataSchemaCmp::Identical | DataSchemaCmp::Equivalent => {
                        DataSchemaCmp::Equivalent
                    }
                    DataSchemaCmp::Incompatible => DataSchemaCmp::Incompatible,
                }
            }
            (_, _) => DataSchemaCmp::Incompatible,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use serde_json::json;

    use crate::*;

    #[test]
    fn test_schema_cmp() {
        // Empty
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![]),
                &DataSchema::new(vec![]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: schema attrs
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![]),
                &DataSchema::new_with_attrs(
                    vec![],
                    ExtraAttributes::new_from_json(json!({"foo.com/bar": "a"})).unwrap()
                ),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Incompatible
        );
        assert_eq!(
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
            DataSchemaCmp::Equivalent
        );

        // Identical: types
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: types
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u64("foo")]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Incompatible
        );

        // Different: names
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("bar")]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Incompatible
        );

        // Identical: attrs
        assert_eq!(
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
        assert_eq!(
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
            DataSchemaCmp::Incompatible
        );
        assert_eq!(
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
            DataSchemaCmp::Equivalent
        );

        // Identical: nested types
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: nested types
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u64("a")])]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Incompatible
        );

        // Different: nested names
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("a")])]),
                &DataSchema::new(vec![DataField::structure("foo", vec![DataField::u32("b")])]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Incompatible
        );

        // Different: nested attrs
        assert_eq!(
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
            DataSchemaCmp::Incompatible
        );
        assert_eq!(
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
            DataSchemaCmp::Equivalent
        );

        // Identical: optionality
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Identical
        );

        // Different: optionality
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                DataSchemaCmpOptions::default(),
            ),
            DataSchemaCmp::Incompatible
        );
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo")]),
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                DataSchemaCmpOptions {
                    ignore_optionality: true,
                    ..Default::default()
                },
            ),
            DataSchemaCmp::Equivalent
        );
        assert_eq!(
            DataSchema::compare(
                &DataSchema::new(vec![DataField::u32("foo").optional()]),
                &DataSchema::new(vec![DataField::u32("foo")]),
                DataSchemaCmpOptions {
                    ignore_optionality: true,
                    ..Default::default()
                },
            ),
            DataSchemaCmp::Equivalent
        );
    }
}

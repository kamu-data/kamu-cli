// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::array::{Array, AsArray, FixedSizeListArray, GenericListArray, GenericListViewArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static ASSERT_NOT_NULL: std::sync::LazyLock<Arc<ScalarUDF>> =
    std::sync::LazyLock::new(|| Arc::new(AssertNotNull::new().into()));

pub fn assert_not_null() -> Arc<ScalarUDF> {
    Arc::clone(&*ASSERT_NOT_NULL)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static ASSERT_LIST_ELEMENTS_NOT_NULL: std::sync::LazyLock<Arc<ScalarUDF>> =
    std::sync::LazyLock::new(|| Arc::new(AssertListElementsNotNull::new().into()));

pub fn assert_list_elements_not_null() -> Arc<ScalarUDF> {
    Arc::clone(&*ASSERT_LIST_ELEMENTS_NOT_NULL)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AssertNotNull
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AssertNotNull {
    signature: Signature,
}

impl AssertNotNull {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AssertNotNull {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "assert_not_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> datafusion::error::Result<DataType> {
        unreachable!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return datafusion::common::plan_err!("assert_not_null accepts 1 argument");
        }

        let f = &args.arg_fields[0];

        if !f.is_nullable() {
            Ok(Arc::clone(f))
        } else {
            Ok(Arc::new(Field::new(
                self.name(),
                f.data_type().clone(),
                false,
            )))
        }
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let ColumnarValue::Array(arr) = &args.args[0] else {
            unreachable!();
        };

        if arr.null_count() != 0 {
            return datafusion::common::exec_err!(
                "Column `{}` contains {} null values while none were expected",
                args.arg_fields[0].name(),
                arr.null_count()
            );
        }

        Ok(ColumnarValue::Array(arr.clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AssertListElementsNotNull
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AssertListElementsNotNull {
    signature: Signature,
}

impl AssertListElementsNotNull {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn field_to_required(f: &Arc<Field>) -> Arc<Field> {
        if f.is_nullable() {
            Arc::new(f.as_ref().clone().with_nullable(false))
        } else {
            f.clone()
        }
    }
}

impl ScalarUDFImpl for AssertListElementsNotNull {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "assert_list_elements_not_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> datafusion::error::Result<DataType> {
        unreachable!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return datafusion::common::plan_err!(
                "assert_list_elements_not_null accepts 1 argument"
            );
        }

        let f = &args.arg_fields[0];

        let data_type = match f.data_type() {
            DataType::List(items) => DataType::List(Self::field_to_required(items)),
            DataType::ListView(items) => DataType::ListView(Self::field_to_required(items)),
            DataType::FixedSizeList(items, len) => {
                DataType::FixedSizeList(Self::field_to_required(items), *len)
            }
            DataType::LargeList(items) => DataType::LargeList(Self::field_to_required(items)),
            DataType::LargeListView(items) => {
                DataType::LargeListView(Self::field_to_required(items))
            }
            _ => {
                return datafusion::common::plan_err!(
                    "assert_list_elements_not_null expects argument must be a list"
                );
            }
        };

        Ok(Arc::new(f.as_ref().clone().with_data_type(data_type)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let ColumnarValue::Array(arr) = &args.args[0] else {
            unreachable!();
        };

        let check_null_count = |null_count: usize| {
            if null_count == 0 {
                Ok(())
            } else {
                datafusion::common::exec_err!(
                    "List column `{}` contains {} null elements while none were expected",
                    args.arg_fields[0].name(),
                    null_count
                )
            }
        };

        let new_arr: Arc<dyn Array> = match arr.data_type() {
            DataType::List(_) => {
                let arr = arr.as_list::<i32>();
                let (field, offsets, values, nulls) = arr.clone().into_parts();
                check_null_count(values.null_count())?;

                Arc::new(GenericListArray::new(
                    Self::field_to_required(&field),
                    offsets,
                    values,
                    nulls,
                ))
            }
            DataType::ListView(_) => {
                let arr = arr.as_list_view::<i32>();
                let (field, offsets, sizes, values, nulls) = arr.clone().into_parts();
                check_null_count(values.null_count())?;

                Arc::new(GenericListViewArray::new(
                    Self::field_to_required(&field),
                    offsets,
                    sizes,
                    values,
                    nulls,
                ))
            }
            DataType::FixedSizeList(_, _) => {
                let arr = arr.as_fixed_size_list();
                let (field, size, values, nulls) = arr.clone().into_parts();
                check_null_count(values.null_count())?;

                Arc::new(FixedSizeListArray::new(
                    Self::field_to_required(&field),
                    size,
                    values,
                    nulls,
                ))
            }
            DataType::LargeList(_) => {
                let arr = arr.as_list::<i64>();
                let (field, offsets, values, nulls) = arr.clone().into_parts();
                check_null_count(values.null_count())?;

                Arc::new(GenericListArray::new(
                    Self::field_to_required(&field),
                    offsets,
                    values,
                    nulls,
                ))
            }
            DataType::LargeListView(_) => {
                let arr = arr.as_list_view::<i64>();
                let (field, offsets, sizes, values, nulls) = arr.clone().into_parts();
                check_null_count(values.null_count())?;

                Arc::new(GenericListViewArray::new(
                    Self::field_to_required(&field),
                    offsets,
                    sizes,
                    values,
                    nulls,
                ))
            }
            _ => unreachable!(),
        };

        Ok(ColumnarValue::Array(new_arr))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static ASSERT_NOT_NULL: std::sync::LazyLock<Arc<ScalarUDF>> =
    std::sync::LazyLock::new(|| Arc::new(AssertNotNull::new().into()));

pub fn assert_not_null() -> Arc<ScalarUDF> {
    Arc::clone(&*ASSERT_NOT_NULL)
}

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
            return datafusion::common::exec_err!("assert_not_null expects a column argument");
        };

        if arr.null_count() != 0 {
            return datafusion::common::exec_err!(
                "Column {} contains {} null values while none were expected",
                args.arg_fields[0].name(),
                arr.null_count()
            );
        }

        Ok(ColumnarValue::Array(arr.clone()))
    }
}

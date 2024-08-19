// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

macro_rules! impl_sqlx {
    ($typ:ident) => {
        impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for $typ
        where
            &'r str: sqlx::Decode<'r, DB>,
        {
            fn decode(
                value: <DB as sqlx::database::Database>::ValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let value = <&str as sqlx::Decode<DB>>::decode(value)?;
                let value = $typ::from_did_str(value)?;
                Ok(value)
            }
        }

        #[cfg(feature = "sqlx-postgres")]
        impl sqlx::Type<sqlx::Postgres> for $typ {
            fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
                <&str as sqlx::Type<sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &<sqlx::Postgres as sqlx::Database>::TypeInfo) -> bool {
                <&str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        #[cfg(feature = "sqlx-mysql")]
        impl sqlx::Type<sqlx::MySql> for $typ {
            fn type_info() -> <sqlx::MySql as sqlx::Database>::TypeInfo {
                <&str as sqlx::Type<sqlx::MySql>>::type_info()
            }

            fn compatible(ty: &<sqlx::MySql as sqlx::Database>::TypeInfo) -> bool {
                <&str as sqlx::Type<sqlx::MySql>>::compatible(ty)
            }
        }

        #[cfg(feature = "sqlx-sqlite")]
        impl sqlx::Type<sqlx::Sqlite> for $typ {
            fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
                <&str as sqlx::Type<sqlx::Sqlite>>::type_info()
            }

            fn compatible(ty: &<sqlx::Sqlite as sqlx::Database>::TypeInfo) -> bool {
                <&str as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }
    };
}

pub(crate) use impl_sqlx;

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]

use std::backtrace::Backtrace;

use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Internal error")]
pub struct InternalError {
    #[source]
    source: BoxedError,
    context: Option<String>,
    #[backtrace]
    backtrace: Option<Backtrace>,
}

impl InternalError {
    pub fn new<E: Into<BoxedError>>(e: E) -> Self {
        let source = e.into();
        let backtrace = if core::error::request_ref::<Backtrace>(source.as_ref()).is_some() {
            None
        } else {
            Some(Backtrace::capture())
        };

        Self {
            source,
            backtrace,
            context: None,
        }
    }

    pub fn bail<T>(reason: impl Into<String>) -> Result<T, Self> {
        Err(reason.into().int_err())
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());

        self
    }

    pub fn reason(&self) -> String {
        use std::fmt::Write;

        let mut reason = format!("{self}: {}", self.source);

        if let Some(context) = &self.context {
            write!(&mut reason, " (context: {context})").unwrap();
        }

        reason
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ErrorIntoInternal {
    fn int_err(self) -> InternalError;
}

impl<E> ErrorIntoInternal for E
where
    E: Into<BoxedError>,
{
    fn int_err(self) -> InternalError {
        InternalError::new(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResultIntoInternal<OK> {
    fn int_err(self) -> Result<OK, InternalError>;

    fn map_int_err<E, F>(self, error_mapper: F) -> Result<OK, E>
    where
        F: FnOnce(InternalError) -> E;

    fn context_int_err(self, context: impl Into<String>) -> Result<OK, InternalError>;
}

impl<OK, E> ResultIntoInternal<OK> for Result<OK, E>
where
    E: Into<BoxedError>,
{
    /// Method whose purpose is to convert a dynamic error
    /// into an [`InternalError`].
    ///
    /// If you need to add additional context to the error,
    /// [`context_int_err()`](ResultIntoInternal::context_int_err())
    /// method will help.
    fn int_err(self) -> Result<OK, InternalError> {
        match self {
            Ok(ok) => Ok(ok),
            Err(e) => Err(e.int_err()),
        }
    }

    /// Shortcut for [`int_err()`](ResultIntoInternal::int_err())
    /// followed by a call to [`Result::map_err()`]:
    /// ```
    /// auth_svc
    ///     .login(...)
    ///     .map_int_err(CLIError::critical)
    /// ```
    ///
    /// This function is equivalent to calling:
    /// ```
    /// auth_svc
    ///     .login(...)
    ///     .int_err()
    ///     .map_err(CLIError::critical)
    /// ```
    #[inline]
    fn map_int_err<R, F>(self, error_mapper: F) -> Result<OK, R>
    where
        F: FnOnce(InternalError) -> R,
    {
        self.int_err().map_err(error_mapper)
    }

    /// Version of method [`int_err()`](ResultIntoInternal::int_err())
    /// with additional context.
    ///
    /// # Examples
    /// ```
    /// // Adding context to a parsing error:
    /// let account_property = property_name
    ///     .parse::<AccountPropertyName>()
    ///     .context_int_err(format!("group '{group}', property_name '{property_name}'"))?;
    /// //   ^^^^^^^^^^^^^^^
    /// ```
    #[inline]
    fn context_int_err(self, context: impl Into<String>) -> Result<OK, InternalError> {
        self.int_err().map_err(|e| e.with_context(context))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

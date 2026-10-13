// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Yields `prefix`, then fails, the way a network stream cut off midway does
pub(crate) struct InterruptedStream {
    prefix: &'static [u8],
}

impl InterruptedStream {
    pub(crate) fn new(prefix: &'static [u8]) -> Self {
        Self { prefix }
    }
}

impl AsyncRead for InterruptedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.prefix.is_empty() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "stream interrupted",
            )));
        }

        let len = self.prefix.len().min(buf.remaining());
        buf.put_slice(&self.prefix[..len]);
        self.prefix = &self.prefix[len..];
        Poll::Ready(Ok(()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Yields `prefix`, then never yields again, the way a stalled network stream
/// does; lets a test drop a write midway
pub(crate) struct StalledStream {
    prefix: &'static [u8],
}

impl StalledStream {
    pub(crate) fn new(prefix: &'static [u8]) -> Self {
        Self { prefix }
    }
}

impl AsyncRead for StalledStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.prefix.is_empty() {
            return Poll::Pending;
        }

        let len = self.prefix.len().min(buf.remaining());
        buf.put_slice(&self.prefix[..len]);
        self.prefix = &self.prefix[len..];
        Poll::Ready(Ok(()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

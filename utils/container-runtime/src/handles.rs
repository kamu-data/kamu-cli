// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::Command;

use crate::ContainerRuntimeError;

///////////////////////////////////////////////////////////////////////////////
// NetworkHandle
///////////////////////////////////////////////////////////////////////////////

#[must_use]
#[derive(Debug)]
pub struct NetworkHandle {
    remove: Option<Command>,
}

impl NetworkHandle {
    const DROP_TIMEOUT: Duration = Duration::from_millis(500);
    const DROP_TICK_TIMEOUT: Duration = Duration::from_millis(10);

    pub fn new(remove: Command) -> Self {
        Self {
            remove: Some(remove),
        }
    }

    pub async fn free(mut self) -> Result<(), ContainerRuntimeError> {
        if let Some(mut cmd) = self.remove.take() {
            cmd.stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
        }
        Ok(())
    }
}

impl Drop for NetworkHandle {
    fn drop(&mut self) {
        if let Some(mut cmd) = self.remove.take() {
            tracing::warn!(
                ?cmd,
                "Network handle was not freed - cleaning up synchronously"
            );

            if let Ok(mut child) = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn() {
                // TODO: Ideally we would reach to inner handle and block on it, but tokio makes
                // it hard, so we do a busy loop.
                let start = Instant::now();
                while (Instant::now() - start) < Self::DROP_TIMEOUT {
                    match child.try_wait() {
                        Ok(None) => std::thread::sleep(Self::DROP_TICK_TIMEOUT),
                        Ok(Some(_)) | Err(_) => break,
                    }
                }
            }
        }
    }
}

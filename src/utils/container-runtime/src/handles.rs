// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::process::Stdio;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NetworkHandle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use]
#[derive(Debug)]
pub struct NetworkHandle {
    runtime: ContainerRuntime,
    network_name: Option<String>,
}

impl NetworkHandle {
    pub fn new(runtime: ContainerRuntime, network_name: String) -> Self {
        Self {
            runtime,
            network_name: Some(network_name),
        }
    }

    pub fn name(&self) -> &str {
        self.network_name
            .as_ref()
            .map(|v| -> &str { v.as_ref() })
            .unwrap()
    }

    pub async fn free(mut self) -> Result<(), ContainerRuntimeError> {
        if let Some(network_name) = self.network_name.take() {
            self.runtime
                .remove_network_cmd(&network_name)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
        }
        Ok(())
    }
}

impl Drop for NetworkHandle {
    fn drop(&mut self) {
        if let Some(network_name) = self.network_name.take() {
            tracing::warn!(
                network_name,
                "Network handle was not freed - cleaning up synchronously"
            );
            let _ = self
                .runtime
                .remove_network_cmd_std(&network_name)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

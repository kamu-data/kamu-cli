// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerRuntimeConfig {
    pub runtime: ContainerRuntimeType,
    pub network_ns: NetworkNamespaceType,
}

impl Default for ContainerRuntimeConfig {
    fn default() -> Self {
        let runtime = std::env::var("KAMU_CONTAINER_RUNTIME_TYPE")
            .map(|val| match val.as_str() {
                "docker" => ContainerRuntimeType::Docker,
                "podman" => ContainerRuntimeType::Podman,
                _ => panic!("Unrecognized runtime type: {}", val),
            })
            .unwrap_or(ContainerRuntimeType::Podman);

        Self {
            runtime,
            network_ns: NetworkNamespaceType::Private,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ContainerRuntimeType {
    Docker,
    Podman,
}

/// Corresponds to podman's containers.conf::netns
/// We podman is used inside containers (e.g. podman-in-docker or podman-in-k8s)
/// it usually runs uses host network namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NetworkNamespaceType {
    Private,
    Host,
}

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub const SPARK: &str = "ghcr.io/kamu-data/engine-spark:0.23.1-spark_3.5.0";
pub const FLINK: &str = "ghcr.io/kamu-data/engine-flink:0.18.2-flink_1.16.0-scala_2.12-java8";
pub const DATAFUSION: &str = "ghcr.io/kamu-data/engine-datafusion:0.8.1";
pub const RISINGWAVE: &str = "ghcr.io/kamu-data/engine-risingwave:0.2.0-risingwave_1.7.0-alpha";

pub const LIVY: &str = SPARK;
pub const JUPYTER: &str = "ghcr.io/kamu-data/jupyter:0.7.1";
pub const QDRANT: &str = "docker.io/qdrant/qdrant:v1.13.4";
pub const BUSYBOX: &str = "docker.io/busybox:latest";

#[cfg(feature = "ingest-mqtt")]
pub const RUMQTTD: &str = "docker.io/bytebeamio/rumqttd:0.19.0";

#[cfg(feature = "ingest-ftp")]
pub const FTP: &str = "docker.io/bogem/ftp";

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub const SPARK: &str = "ghcr.io/kamu-data/engine-spark:0.22.1-spark_3.1.2";
pub const FLINK: &str = "ghcr.io/kamu-data/engine-flink:0.18.0-flink_1.16.0-scala_2.12-java8";
pub const DATAFUSION: &str = "ghcr.io/kamu-data/engine-datafusion:0.7.2";

pub const LIVY: &str = SPARK;
pub const JUPYTER: &str = "ghcr.io/kamu-data/jupyter:0.5.2";

// Test Images
pub const HTTPD: &str = "docker.io/httpd:2.4";
pub const MINIO: &str = "docker.io/minio/minio:RELEASE.2021-08-31T05-46-54Z";
pub const BUSYBOX: &str = "docker.io/busybox:latest";

#[cfg(feature = "ftp")]
pub const FTP: &'static str = "docker.io/bogem/ftp";

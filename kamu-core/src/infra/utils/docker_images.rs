// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub const SPARK: &str = "docker.io/kamudata/engine-spark:0.16.0-spark_3.1.2";
pub const LIVY: &str = SPARK;
pub const FLINK: &str = "docker.io/kamudata/engine-flink:0.14.0-flink_1.13.1-scala_2.12-java8";
pub const JUPYTER: &str = "docker.io/kamudata/jupyter:0.3.0";

// Test Images
pub const HTTPD: &str = "docker.io/httpd:2.4";
pub const FTP: &str = "docker.io/bogem/ftp";
pub const MINIO: &str = "docker.io/minio/minio:RELEASE.2021-08-31T05-46-54Z";

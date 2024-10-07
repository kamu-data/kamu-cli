// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_set_value(kamu: KamuCliPuppet) {
    // 1. Set flow for the "engine.runtime" key
    {
        let assert = kamu.execute(["config", "list"]).await.success();
        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::indoc!(
                r#"
                {}

                "#
            )
        );
    }
    {
        let assert = kamu
            .execute(["config", "set", "engine.runtime", "podman"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Set engine.runtime to podman in workspace scope"),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["config", "get", "engine.runtime"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::indoc!(
                r#"
                podman

                "#
            )
        );
    }
    {
        let assert = kamu.execute(["config", "list"]).await.success();
        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::indoc!(
                r#"
                engine:
                  runtime: podman

                "#
            )
        );
    }
    // 2. Set flow for the "uploads.maxFileSizeInMb" key
    {
        let assert = kamu
            .execute(["config", "set", "uploads.maxFileSizeInMb", "42"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Set uploads.maxFileSizeInMb to 42 in workspace scope"),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["config", "get", "uploads.maxFileSizeInMb"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(stdout, "42\n\n");
    }
    {
        let assert = kamu.execute(["config", "list"]).await.success();
        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::indoc!(
                r#"
                engine:
                  runtime: podman
                uploads:
                  maxFileSizeInMb: 42

                "#
            )
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_reset_key(kamu: KamuCliPuppet) {
    {
        let assert = kamu
            .execute(["config", "set", "engine.runtime", "podman"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Set engine.runtime to podman in workspace scope"),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["config", "set", "engine.runtime"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Removed engine.runtime from workspace scope"),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["config", "get", "engine.runtime"])
            .await
            .failure();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Error: Key engine.runtime not found"),
            "Unexpected output:\n{stderr}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_get_with_default(kamu: KamuCliPuppet) {
    {
        let assert = kamu
            .execute(["config", "get", "engine.runtime"])
            .await
            .failure();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Error: Key engine.runtime not found"),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["config", "get", "engine.runtime", "--with-defaults"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::indoc!(
                r#"
                docker

                "#
            )
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_get_from_config(kamu: KamuCliPuppet) {
    let assert = kamu.execute(["config", "list"]).await.success();
    let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

    pretty_assertions::assert_eq!(
        stdout,
        indoc::indoc!(
            r#"
            engine:
              runtime: podman
            uploads:
              maxFileSizeInMb: 42

            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

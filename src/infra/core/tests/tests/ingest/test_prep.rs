// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::io::prelude::*;

use indoc::indoc;
use kamu::domain::PollingIngestError;
use kamu::ingest::*;

#[test]
fn test_prep_pipe() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.json");
    let target_path = tempdir.path().join("prepared.bin");
    let run_info_dir = tempdir.path();

    let prep_steps = vec![odf::metadata::PrepStep::Pipe(odf::metadata::PrepStepPipe {
        command: ["jq", "-c", ".[]"]
            .iter()
            .map(|s| (*s).to_string())
            .collect(),
    })];

    let prep_svc = PrepService::new();

    std::fs::write(
        &src_path,
        indoc!(
            "
            [
                {
                    \"city\": \"A\",
                    \"population\": 100
                },
                {
                    \"city\": \"B\",
                    \"population\": 200
                },
                {
                    \"city\": \"A\",
                    \"population\": 110
                }
            ]
            "
        ),
    )
    .unwrap();

    prep_svc
        .prepare(&prep_steps, &src_path, &target_path, run_info_dir)
        .unwrap();

    assert!(target_path.exists());
    assert_eq!(
        std::fs::read_to_string(&target_path)
            .unwrap()
            .replace('\r', ""),
        "{\"city\":\"A\",\"population\":100}\n{\"city\":\"B\",\"population\":200}\n{\"city\":\"A\"\
         ,\"population\":110}\n"
    );
}

#[test]
fn test_prep_decompress_zip_single_file() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.zip");
    let target_path = tempdir.path().join("prepared.bin");
    let run_info_dir = tempdir.path();

    let prep_steps = vec![odf::metadata::PrepStep::Decompress(
        odf::metadata::PrepStepDecompress {
            format: odf::metadata::CompressionFormat::Zip,
            sub_path: None,
        },
    )];

    let content = indoc!(
        "
        city,population
        A,1000
        B,2000
        C,3000
        "
    );

    {
        // Create archive
        use zip::write::*;
        let mut zip = ZipWriter::new(std::fs::File::create(&src_path).unwrap());

        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("data.csv", options).unwrap();
        zip.write_all(content.as_bytes()).unwrap();
        zip.finish().unwrap();
    }

    let prep_svc = PrepService::new();

    prep_svc
        .prepare(&prep_steps, &src_path, &target_path, run_info_dir)
        .unwrap();

    assert!(target_path.exists());
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
}

#[test]
fn test_prep_decompress_zip_bad_file() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.zip");
    let target_path = tempdir.path().join("prepared.bin");
    let run_info_dir = tempdir.path();

    let prep_steps = vec![odf::metadata::PrepStep::Decompress(
        odf::metadata::PrepStepDecompress {
            format: odf::metadata::CompressionFormat::Zip,
            sub_path: None,
        },
    )];

    std::fs::write(&src_path, "garbage").unwrap();

    let prep_svc = PrepService::new();

    let res = prep_svc.prepare(&prep_steps, &src_path, &target_path, run_info_dir);
    assert_matches!(res, Err(PollingIngestError::Internal(_)));
}

#[test]
fn test_prep_decompress_gzip() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.gz");
    let target_path = tempdir.path().join("prepared.bin");
    let run_info_dir = tempdir.path();

    let prep_steps = vec![odf::metadata::PrepStep::Decompress(
        odf::metadata::PrepStepDecompress {
            format: odf::metadata::CompressionFormat::Gzip,
            sub_path: None,
        },
    )];

    let content = indoc!(
        "
        city,population
        A,1000
        B,2000
        C,3000
        "
    );

    {
        // Create archive
        use flate2::write::GzEncoder;
        use flate2::Compression;
        let mut gzip = GzEncoder::new(
            std::fs::File::create(&src_path).unwrap(),
            Compression::fast(),
        );
        gzip.write_all(content.as_bytes()).unwrap();
    }

    let prep_svc = PrepService::new();

    prep_svc
        .prepare(&prep_steps, &src_path, &target_path, run_info_dir)
        .unwrap();

    assert!(target_path.exists());
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
}

use indoc::indoc;
use kamu::domain::IngestError;
use kamu::infra::ingest::*;
use kamu_test::assert_err;
use opendatafabric::*;

use chrono::Utc;
use std::io::prelude::*;

#[test]
fn test_prep_pipe() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.json");
    let target_path = tempdir.path().join("prepared.bin");

    let prep_steps = vec![PrepStep::Pipe(PrepStepPipe {
        command: ["jq", "-c", ".[]"].iter().map(|s| s.to_string()).collect(),
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

    let res = prep_svc
        .prepare(&prep_steps, Utc::now(), None, &src_path, &target_path)
        .unwrap();
    assert_eq!(res.was_up_to_date, false);
    assert!(target_path.exists());

    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap().replace("\r", ""),
        "{\"city\":\"A\",\"population\":100}\n{\"city\":\"B\",\"population\":200}\n{\"city\":\"A\",\"population\":110}\n"
    );
}

#[test]
fn test_prep_decompress_zip_single_file() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.zip");
    let target_path = tempdir.path().join("prepared.bin");

    let prep_steps = vec![PrepStep::Decompress(PrepStepDecompress {
        format: CompressionFormat::Zip,
        sub_path: None,
    })];

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

        let options = FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("data.csv", options).unwrap();
        zip.write(content.as_bytes()).unwrap();
        zip.finish().unwrap();
    }

    let prep_svc = PrepService::new();

    let res = prep_svc
        .prepare(&prep_steps, Utc::now(), None, &src_path, &target_path)
        .unwrap();
    assert_eq!(res.was_up_to_date, false);
    assert!(target_path.exists());

    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
}

#[test]
fn test_prep_decompress_zip_bad_file() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.zip");
    let target_path = tempdir.path().join("prepared.bin");

    let prep_steps = vec![PrepStep::Decompress(PrepStepDecompress {
        format: CompressionFormat::Zip,
        sub_path: None,
    })];

    std::fs::write(&src_path, "garbage").unwrap();

    let prep_svc = PrepService::new();

    let res = prep_svc.prepare(&prep_steps, Utc::now(), None, &src_path, &target_path);
    assert_err!(res, IngestError::InternalError { .. });
}

#[test]
fn test_prep_decompress_gzip() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.gz");
    let target_path = tempdir.path().join("prepared.bin");

    let prep_steps = vec![PrepStep::Decompress(PrepStepDecompress {
        format: CompressionFormat::Gzip,
        sub_path: None,
    })];

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
        gzip.write(content.as_bytes()).unwrap();
    }

    let prep_svc = PrepService::new();

    let res = prep_svc
        .prepare(&prep_steps, Utc::now(), None, &src_path, &target_path)
        .unwrap();
    assert_eq!(res.was_up_to_date, false);
    assert!(target_path.exists());

    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
}

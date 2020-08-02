use indoc::indoc;
use kamu::infra::ingest::*;
use kamu::infra::serde::yaml::*;

use chrono::Utc;

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
        std::fs::read_to_string(&target_path).unwrap(),
        "{\"city\":\"A\",\"population\":100}\n{\"city\":\"B\",\"population\":200}\n{\"city\":\"A\",\"population\":110}\n"
    );
}

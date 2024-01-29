use assert_cmd::Command;

#[test]
fn test_datafusion_cli() {
    let expected_output =
        "+----------+\n| Int64(1) |\n+----------+\n| 1        |\n+----------+\n1 row in set.";

    let mut cmd = Command::cargo_bin("kamu-cli").unwrap();

    let assert = cmd.arg("sql").write_stdin("select 1;").assert();

    let output = assert.get_output();

    //not using assert.stdout("expected text") as time of query execution may differ
    let string_result = String::from_utf8_lossy(&output.stdout);

    assert!(
        string_result.contains(expected_output),
        "Output does not contain expected text"
    );
    assert.success();
}

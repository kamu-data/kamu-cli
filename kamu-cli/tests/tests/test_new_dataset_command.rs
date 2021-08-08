use kamu::domain::*;
use kamu::infra::ResourceLoaderImpl;
use kamu_cli::commands::*;
use kamu_cli::CLIError;

#[test]
fn test_ambiguity_is_punished() {
    let mut cmd = NewDatasetCommand::new("foo", false, false, None::<&str>);
    assert!(matches!(cmd.run(), Err(CLIError::UsageError { .. })));
}

#[test]
fn test_root_dataset_parses() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("ds.yaml");
    let mut cmd = NewDatasetCommand::new("foo", true, false, Some(&path));
    cmd.run().unwrap();

    let loader = ResourceLoaderImpl::new();
    loader
        .load_dataset_snapshot_from_path(&path)
        .expect("Failed to parse template");
}

#[test]
fn test_derivative_dataset_parses() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("ds.yaml");
    let mut cmd = NewDatasetCommand::new("foo", false, true, Some(&path));
    cmd.run().unwrap();

    let loader = ResourceLoaderImpl::new();
    loader
        .load_dataset_snapshot_from_path(&path)
        .expect("Failed to parse template");
}

use opendatafabric::*;

use std::convert::TryFrom;

#[test]
fn datasetid_newtype() {
    let s = "valid.dataset.id";
    let id = DatasetID::try_from(s).unwrap();

    fn needs_str(_: &str) {}
    fn needs_id(_: &DatasetID) {}

    needs_id(id);
    needs_str(id); // implicit converts to str
    assert!(id.starts_with("valid")); // str methods are still accessible
}

#[test]
fn datasetid_fmt() {
    let id = DatasetID::try_from("valid.dataset.id").unwrap();

    assert_eq!(format!("{}", id), "valid.dataset.id");
    assert_eq!(format!("{:?}", id), "DatasetID(\"valid.dataset.id\")");
}

#[test]
fn datasetid_equality() {
    assert_eq!(DatasetID::new_unchecked("a"), DatasetID::new_unchecked("a"));
    assert_ne!(DatasetID::new_unchecked("a"), DatasetID::new_unchecked("b"));
}

#[test]
fn datasetid_validation() {
    match DatasetID::try_from("valid.dataset-id") {
        Ok(act) => assert_eq!(act, "valid.dataset-id"),
        Err(e) => assert!(false, "{}", e),
    }

    assert!(DatasetID::try_from(".invalid").is_err());
    assert!(DatasetID::try_from("invalid-").is_err());
    assert!(DatasetID::try_from("invalid--id").is_err());
    assert!(DatasetID::try_from("invalid..id").is_err());
    assert!(DatasetID::try_from("in^valid").is_err());
}

#[test]
fn datasetidbuf_newtype() {
    let buf = DatasetIDBuf::try_from("test").unwrap();

    fn needs_str(_: &str) {}
    fn needs_id(_: &DatasetID) {}
    fn needs_idbuf(_: &DatasetIDBuf) {}

    needs_idbuf(&buf);
    needs_id(&buf); // implicitly converts to str
    needs_str(&buf); // implicitly converts to ref type

    let buf2 = DatasetID::new_unchecked("asdf").to_owned();
    needs_id(&buf2);
}

#[test]
fn datasetidbuf_fmt() {
    let id = DatasetIDBuf::try_from("valid.dataset.id").unwrap();

    assert_eq!(format!("{}", id), "valid.dataset.id");
    assert_eq!(format!("{:?}", id), "DatasetIDBuf(\"valid.dataset.id\")");
}

#[test]
fn datasetidbuf_equality() {
    let buf1 = DatasetIDBuf::try_from("test1").unwrap();
    let buf2 = DatasetIDBuf::try_from("test2").unwrap();
    let buf22 = DatasetIDBuf::try_from("test2").unwrap();

    assert_eq!(buf2, buf22);

    assert_ne!(buf1, *DatasetID::new_unchecked("test2"));
    assert_eq!(buf1, *DatasetID::new_unchecked("test1"));
}

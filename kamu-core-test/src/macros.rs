#[macro_export]
macro_rules! assert_err {
    ($actual:expr, $expected:pat) => {
        match $actual {
            Err($expected) => (),
            Err(e) => assert!(
                false,
                format!("expected {} but got {:?} instead", stringify!($expected), e)
            ),
            _ => assert!(false, "Expected an error but got Ok"),
        }
    };
}

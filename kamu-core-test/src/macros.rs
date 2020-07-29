#[macro_export]
macro_rules! assert_ok {
    ($actual:expr, $expected:pat) => {
        match $actual {
            Ok($expected) => (),
            ref res => panic!(
                "Expected {} but got {:?} instead",
                stringify!($expected),
                res
            ),
        }
    };
}

#[macro_export]
macro_rules! assert_err {
    ($actual:expr, $expected:pat) => {
        match $actual {
            Err($expected) => (),
            Err(e) => panic!("Expected {} but got {:?} instead", stringify!($expected), e),
            _ => panic!("Expected an error but got Ok"),
        }
    };
}

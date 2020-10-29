#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest<T> {
    pub api_version: i32,
    pub kind: String,
    pub content: T,
}

#[cynic::schema("kamu")]
pub(crate) mod schema {}

pub(crate) mod conversions;
pub(crate) mod fragments;
pub(crate) mod get_resource;
pub(crate) mod get_resources;
pub(crate) mod identity;
pub(crate) mod inputs;
pub(crate) mod render_manifest;
pub(crate) mod scalars;
pub(crate) mod supported_kinds;
pub(crate) mod variables;

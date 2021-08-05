use opendatafabric::{DatasetRef, DatasetRefBuf};

use super::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteAliasKind {
    Pull,
    Push,
}

pub trait RemoteAliases {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a DatasetRef> + 'a>;

    fn contains(&self, remore_ref: &DatasetRef, kind: RemoteAliasKind) -> bool;

    fn is_empty(&self, kind: RemoteAliasKind) -> bool;

    fn add(
        &mut self,
        remote_ref: DatasetRefBuf,
        kind: RemoteAliasKind,
    ) -> Result<bool, DomainError>;

    fn delete(
        &mut self,
        remote_ref: &DatasetRef,
        kind: RemoteAliasKind,
    ) -> Result<bool, DomainError>;

    fn clear(&mut self, kind: RemoteAliasKind) -> Result<usize, DomainError>;
}

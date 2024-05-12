use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::{d_store::DStoreId, Error};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum InnerDFut {
    DStore(DStoreId),
    Error(Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DFut<T> {
    // Inner dfut state.
    pub(crate) inner: InnerDFut,

    // Track the type!
    #[serde(skip)]
    _phantom_data: PhantomData<T>,
}

impl<T> DFut<T> {
    pub(crate) fn new(inner: InnerDFut) -> Self {
        Self {
            inner,
            _phantom_data: PhantomData::default(),
        }
    }
}

impl<T> From<DStoreId> for DFut<T> {
    fn from(value: DStoreId) -> Self {
        DFut::new(InnerDFut::DStore(value))
    }
}

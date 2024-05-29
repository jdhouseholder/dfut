// TODO: Once we have a nice way to make DFut Try (use ? operator to change control flow) we
// can use inner dfut error rather than DResult.
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::d_store::DStoreId;

#[derive(Debug, Serialize, Deserialize)]
pub struct DFut<T> {
    // Inner dfut state.
    pub(crate) d_store_id: DStoreId,

    // Track the type!
    #[serde(skip)]
    _phantom_data: PhantomData<T>,
}

impl<T> DFut<T> {
    pub(crate) fn new(d_store_id: DStoreId) -> Self {
        Self {
            d_store_id,
            _phantom_data: PhantomData::default(),
        }
    }

    pub(crate) fn share(&self) -> Self {
        Self {
            d_store_id: self.d_store_id.clone(),
            _phantom_data: PhantomData::default(),
        }
    }
}

impl<T> From<DStoreId> for DFut<T> {
    fn from(value: DStoreId) -> Self {
        DFut::new(value)
    }
}

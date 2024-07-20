use std::ptr;

use log::error;

use fdb_c::FDBTenant;

use crate::{Error, FdbErrorCode};
use crate::transaction::{CreateTransaction, Transaction};


pub struct Tenant(FDBTenant);

impl From<FDBTenant> for Tenant {
    fn from(value: FDBTenant) -> Self {
        Tenant(value)
    }
}


impl CreateTransaction for Tenant {
    fn create_transaction(&mut self) -> Result<Transaction, Error> {
        let mut trx = ptr::null_mut();
        let result = unsafe { fdb_c::fdb_tenant_create_transaction(&mut self.0, &mut trx) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        Ok(trx.into())
    }
}


impl Drop for Tenant {
    /// Destroys an FDBTenant object. It must be called exactly once for each successful call to
    /// fdb_database_create_tenant(). This function only destroys a handle to the tenant – the
    /// tenant and its data will be fine!
    fn drop(&mut self) {
        unsafe { fdb_c::fdb_tenant_destroy(&mut self.0) };
    }
}

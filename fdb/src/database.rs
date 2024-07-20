use std::ptr;
use std::ptr::NonNull;

use log::error;

use fdb_c::FDBDatabase;

use crate::tenant::Tenant;
use crate::transaction::{CreateTransaction, Transaction};

pub struct Database(*mut FDBDatabase);

impl From<*mut FDBDatabase> for Database {
    fn from(value: *mut FDBDatabase) -> Self {
        Self(value)
    }
}


impl Database {
    fn set_option() -> Result<(), crate::Error> {
        todo!()
    }

    fn tenant(&mut self, name: &str) -> Result<Tenant, crate::Error> {
        let tenant_name = name.as_bytes();
        let mut tenant = ptr::null_mut();

        let result = unsafe {
            fdb_c::fdb_database_open_tenant(
                self.0,
                tenant_name.as_ptr(),
                tenant_name.len() as i32,
                &mut tenant,
            )
        };

        if result != 0 {
            error!("{result}");
            return Err(crate::FdbErrorCode(result).into());
        }

        Ok(unsafe { *tenant }.into())
    }

    fn reboot_worker() {
        todo!()
    }

    fn force_recovery_with_data_loss() {
        todo!()
    }

    fn create_snapshot() {
        todo!()
    }

    /// Returns a value where 0 indicates that the client is idle and 1 (or larger) indicates
    /// that the client is saturated. By default, this value is updated every second.
    fn get_main_thread_busyness(&mut self) -> f64 {
        unsafe { fdb_c::fdb_database_get_main_thread_busyness(self.0) }
    }
}


impl CreateTransaction for Database {
    fn create_transaction(&mut self) -> Result<Transaction, crate::Error> {
        let mut trx: *mut fdb_c::FDBTransaction = std::ptr::null_mut();
        let result = unsafe { fdb_c::fdb_database_create_transaction(self.0, &mut trx) };

        if result != 0 {
            error!("{result}");
            return Err(crate::FdbErrorCode(result).into());
        }

        Ok(trx.into())
    }
}

impl Drop for Database {
    /// Destroys an FDBDatabase object. It must be called exactly once for each successful call to
    /// fdb_create_database(). This function only destroys a handle to the database – your database will be fine!
    fn drop(&mut self) {
        unsafe { fdb_c::fdb_database_destroy(self.0) };
    }
}


use std::ffi::CStr;
use std::fmt::{Debug, Display, Formatter};

use log::error;
use thiserror::Error;

pub use client::Client;
pub use database::Database;
use fdb_c::fdb_error_t;
pub use transaction::{CreateTransaction, Transaction};

mod client;
mod transaction;
mod database;
#[cfg(any(feature = "730", feature = "710"))]
mod tenant;
mod future;
mod types;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Error {0}")]
    Generic(FdbErrorCode),
    #[error("API Version not supported")]
    APIVersionNotSupported,
    #[error("API version may be set only once")]
    APIVersionSingletonViolated,
    #[error("THe network must only be initialized once")]
    NetworkSingletonViolated,
    #[error("Action not possible before the network is configured")]
    ActionInvalidBeforeNetworkConfig,
    #[error("Key not found")]
    KeyNotFound,
}

#[derive(Eq, PartialEq)]
pub struct FdbErrorCode(fdb_error_t);

impl Debug for FdbErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for FdbErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let msg = unsafe { fdb_c::fdb_get_error(self.0) };
        let msg = unsafe { CStr::from_ptr(msg) };
        let msg = msg.to_str().unwrap();

        write!(f, "{:?} ({})", msg, self.0)
    }
}

// https://apple.github.io/foundationdb/api-error-codes.html
impl From<FdbErrorCode> for Error {
    fn from(value: FdbErrorCode) -> Self {
        match value.0 {
            2203 => Error::APIVersionNotSupported,
            2201 => Error::APIVersionSingletonViolated,
            2009 => Error::NetworkSingletonViolated,
            2008 => Error::ActionInvalidBeforeNetworkConfig,
            _ => Error::Generic(FdbErrorCode(value.0)),
        }
    }
}

impl From<&Error> for FdbErrorCode {
    fn from(value: &Error) -> Self {
        FdbErrorCode(match value {
            Error::APIVersionNotSupported => 2203,
            Error::APIVersionSingletonViolated => 2201,
            Error::NetworkSingletonViolated => 2009,
            Error::ActionInvalidBeforeNetworkConfig => 2008,
            Error::Generic(i) => i.0,
            _ => -1,
        })
    }
}



#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_simple_transaction() {
        // let client = Client::new().await.unwrap();
        // let db = client.database().unwrap();
        // let tx = db.create_transaction().unwrap();
        //
        // let empty_get = tx.get("hello").await;
        // assert_eq!(empty_get, Err(Error::KeyNotFound));
        //
        //
        // tx.set("hello", "world").await;
        // let existing_get = tx.get("hello").await;
        // assert_eq!(existing_get, Ok("world".into()));
        //
        // tx.clear("hello").await;
        // tx.commit().await.unwrap();
    }
}

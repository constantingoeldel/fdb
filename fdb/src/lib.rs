use std::ffi::CStr;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::task::{Context, Poll};
use futures::poll;
use thiserror::Error;
use fdb_c::{FDB_API_VERSION, fdb_error_t, fdb_network_set_option, FDBNetworkOption};
use log::{error, info, log, warn};
use tokio::task;


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
    ActionInvalidBeforeNetworkConfig

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
        let msg = unsafe {fdb_c::fdb_get_error(self.0)};
        let msg = unsafe { CStr::from_ptr(msg)};
        let msg = msg.to_str().unwrap();

        write!(f, "{:?} ({})", msg, self.0)
    }
}

impl From<FdbErrorCode> for Error {
    fn from(value: FdbErrorCode) -> Self {
        match value.0 {
            2203 => Error::APIVersionNotSupported,
            2201 => Error::APIVersionSingletonViolated,
            2009 => Error::NetworkSingletonViolated,
            2008 => Error::ActionInvalidBeforeNetworkConfig,
            _ => Error::Generic(FdbErrorCode(value.0))
        }
    }
}

const API_VERSION_SET : AtomicBool = AtomicBool::new(false);

fn select_api_version(version: i32) -> Result<(), Error> {

    let first_time = API_VERSION_SET.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok();

    if !first_time {
        return Err(Error::APIVersionSingletonViolated)
    }

    if version > FDB_API_VERSION as i32 || version > get_max_api_version() {
        return Err(Error::APIVersionNotSupported);
    }

    if version != FDB_API_VERSION as i32 {
        warn!("Selected API version should almost always be equal to the foundation db version feature")
    }

    let result = unsafe {fdb_c::fdb_select_api_version_impl(version, FDB_API_VERSION as i32) };

    if result != 0 {
        error!("{result}");
        return Err(FdbErrorCode(result).into());

    }
    Ok(())

}

fn get_max_api_version() -> i32 {
    unsafe { fdb_c::fdb_get_max_api_version() }
}

struct FDBNetworkOptions;

/// Singleton Client Instance
/// TODO: Maybe cheaply clone using ARC? Is this needed?
#[derive(Debug)]
struct Client;


const NETWORK_SETUP: AtomicBool = AtomicBool::new(false);
const NETWORK_STARTED : AtomicBool = AtomicBool::new(false);
impl Client {
    async fn new() -> Result<Self, Error> {

        // Init network
        Self::setup_network(FDBNetworkOptions)?;

        let handle: task::JoinHandle<_> = task::spawn_blocking(|| {
            Self::run_network()
        });

        // Poll the network once to check if it errored on initialization
        let poll = poll!(handle);
        if let Poll::Ready(join) = poll {
            join.expect("Could not join network init thread")?;
        }

        Ok(Self)
    }
    /// Idempotent singleton network setup
    fn setup_network(options: FDBNetworkOptions) -> Result<() , Error> {
        // TODO: Options
        // Set options
        // let options: Vec<FDBNetworkOption> = options.into();
        //
        // for (option, value) in options {
        //     let res = fdb_network_set_option(o, value, mem::length(value));
        // }

        // Setup network
        let first_time = NETWORK_SETUP.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok();
        if !first_time {
            // Network already setup
            return Ok(())
        }

        let result = unsafe  { fdb_c::fdb_setup_network() };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());

        }

        Ok(())

    }
    /// Initializes the network.
    /// Will not return until stop_network() is called by you or a serious error occurs.
    /// Should therefore be called from an auxiliary thread
    ///
    /// Idempotent & Singleton.
    fn run_network() -> Result<(), Error> {

        let first_time = NETWORK_STARTED.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok();
        if !first_time {
            // Network is already running
            return Ok(())
        }

        info!("Starting network...");

        // Must be called after fdb_setup_network() before any asynchronous functions in this API can be
        // expected to complete. Unless your program is entirely event-driven based on results of
        // asynchronous functions in this API and has no event loop of its own, you will want to invoke
        // this function on an auxiliary thread (which it is your responsibility to create).
        //
        // This function will not return until fdb_stop_network() is called by you or a serious error occurs.
        // It is not possible to run more than one network thread, and the network thread cannot be restarted
        // once it has been stopped. This means that once fdb_run_network has been called, it is not legal
        // to call it again for the lifetime of the running program.

        let result = unsafe { fdb_c::fdb_run_network() };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into())
        }

        Ok(())

    }

    fn stop_network() -> Result<(), Error> {
        let result = unsafe { fdb_c::fdb_stop_network()};

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());

        }

        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        info!("Stopping foundation db network...");
        Self::stop_network().expect("Stopping the network failed")
    }
}
//
// struct Database {
//     fn new() {
//     // fdb_create_database
//     }
// }
//
// impl Drop for Database {
//     // call fdb_database_destroy
// }




struct FDBFuture;

impl Future for FDBFuture {
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = unsafe { fdb_c::fdb_future_is_ready(*self) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());

        }


    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_version() {
        #[cfg(feature = "fdb_730" )]
        assert_eq!(fdb_c::FDB_API_VERSION, 730);
        #[cfg(feature = "fdb_710" )]
        assert_eq!(fdb_c::FDB_API_VERSION, 710);
    }

    #[test]
    fn test_select_invalid_api_version() {
        let result = select_api_version(99999);

        dbg!(&result);
        assert!(result.is_err());
        assert_eq!(result, Err(Error::APIVersionNotSupported))
    }

    #[test]
    fn test_select_api_version() {
        let result = select_api_version(get_max_api_version());
        dbg!(&result);
        assert!(result.is_ok());
    }

    #[test]
    fn test_select_api_version_again() {
        let result = select_api_version(2);

        dbg!(&result);
        assert!(result.is_err());
        assert_eq!(result, Err(Error::APIVersionSingletonViolated))
    }

    #[test]
    fn test_max_api_version() {
        let version = get_max_api_version();
        assert_eq!(version, 710);
    }

    #[tokio::test]
    async fn init_client_idempotent() {
        let client = Client::new().await;
        let client2 = Client::new().await;
        dbg!(&client);
        dbg!(&client2);
        assert!(client.is_ok());
        assert!(client.is_ok());
    }
    // #[tokio::test]
    // async fn init_client_again() {
    //     let client = Client::new().await;
    //     dbg!(&client);
    //     assert!(client.is_ok())
    // }

}

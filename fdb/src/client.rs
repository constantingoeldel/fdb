use std::ffi::c_char;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::task::Poll;

use futures::poll;
use log::{error, info, warn};
use tokio::task;

use fdb_c::FDB_API_VERSION;

use crate::{Error, FdbErrorCode};
use crate::database::Database;

/// Singleton Client Instance
/// TODO: Maybe cheaply clone using ARC? Is this needed?
#[derive(Debug)]
pub struct Client;

static NETWORK_SETUP: AtomicBool = AtomicBool::new(false);
static NETWORK_STARTED: AtomicBool = AtomicBool::new(false);
static API_VERSION_SET: AtomicI32 = AtomicI32::new(0);

/// Must be called before any other API functions. version must be less than or equal to FDB_API_VERSION (and should almost always be equal).
///
/// Passing a version less than FDB_API_VERSION will cause the API to behave as it did in the older version.


struct FDBNetworkOptions;


impl Client {
    pub async fn new() -> Result<Self, Error> {
        Self::select_api_version(Self::get_max_api_version()).expect("Invalid API version");

        // Init network
        Self::setup_network(FDBNetworkOptions)?;

        let handle: task::JoinHandle<_> = task::spawn_blocking(Self::run_network);

        // Poll the network once to check if it errored on initialization
        let poll = poll!(handle);
        if let Poll::Ready(join) = poll {
            join.expect("Could not join network init thread")?;
        }

        Ok(Self)
    }

    fn select_api_version(version: i32) -> Result<(), Error> {

        if version > FDB_API_VERSION as i32 || version > Self::get_max_api_version() || ![510, 520, 600, 610, 620, 630, 700, 710, 730].contains(&version) {
            return Err(Error::APIVersionNotSupported);
        }

        if version != FDB_API_VERSION as i32 {
            warn!("Selected API version should almost always be equal to the foundation db version feature")
        }

        let atomic_version_update = API_VERSION_SET
            .compare_exchange(0, version, Ordering::Acquire, Ordering::Relaxed);


        if let Err(previous_version)  = atomic_version_update {
            if previous_version == version {
                // API Version already set to the same version as requested
                return Ok(());
            }
            println!("API Version already set to a different version: {previous_version}");
            return Err(Error::APIVersionSingletonViolated);
        }


        let result = unsafe { fdb_c::fdb_select_api_version_impl(version, FDB_API_VERSION as i32) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }
        Ok(())
    }

    fn get_max_api_version() -> i32 {
        unsafe { fdb_c::fdb_get_max_api_version() }
    }

    /// Idempotent singleton network setup
    fn setup_network(_options: FDBNetworkOptions) -> Result<(), Error> {
        // TODO: Options
        // Set options
        // let options: Vec<FDBNetworkOption> = options.into();
        //
        // for (option, value) in options {
        //     let res = fdb_network_set_option(o, value, mem::length(value));
        // }

        // Setup network
        let first_time = NETWORK_SETUP
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok();
        if !first_time {
            // Network already setup
            return Ok(());
        }

        let result = unsafe { fdb_c::fdb_setup_network() };

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
        let first_time = NETWORK_STARTED
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok();
        if !first_time {
            // Network is already running
            return Ok(());
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
            return Err(FdbErrorCode(result).into());
        }

        Ok(())
    }

    fn stop_network() -> Result<(), Error> {
        let result = unsafe { fdb_c::fdb_stop_network() };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        Ok(())
    }

    /// Connects to a database on the specified cluster. The caller assumes ownership of the
    /// FDBDatabase object and must destroy it with fdb_database_destroy() (Implemented to automatically happen on Drop).
    ///
    /// --- TODO: Not implemented yet, always uses the default cluster file ---
    /// A single client can use this function multiple times to connect to different clusters
    /// simultaneously, with each invocation requiring its own cluster file.
    /// To connect to multiple clusters running at different, incompatible versions, the multi-version client API must be used.
    pub fn database(&self) -> Result<Database, Error> {
        let mut db = ptr::null_mut();
        let cluster_file_path: *const c_char = ptr::null();

        let result = unsafe { fdb_c::fdb_create_database(cluster_file_path, &mut db) };
        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        };

        Ok(db.into())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        info!("Stopping foundation db network...");
        Self::stop_network().expect("Stopping the network failed")
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_version() {
        #[cfg(feature = "730")]
        assert_eq!(fdb_c::FDB_API_VERSION, 730);
        #[cfg(feature = "710")]
        assert_eq!(fdb_c::FDB_API_VERSION, 710);
        #[cfg(feature = "700")]
        assert_eq!(fdb_c::FDB_API_VERSION, 700);
        #[cfg(feature = "630")]
        assert_eq!(fdb_c::FDB_API_VERSION, 630);
        #[cfg(feature = "620")]
        assert_eq!(fdb_c::FDB_API_VERSION, 620);
        #[cfg(feature = "610")]
        assert_eq!(fdb_c::FDB_API_VERSION, 610);
        #[cfg(feature = "600")]
        assert_eq!(fdb_c::FDB_API_VERSION, 600);
        #[cfg(feature = "520")]
        assert_eq!(fdb_c::FDB_API_VERSION, 520);
        #[cfg(feature = "510")]
        assert_eq!(fdb_c::FDB_API_VERSION, 510);
    }

    #[test]
    fn test_select_invalid_api_version() {
        let result = Client::select_api_version(99999);
        
        assert!(result.is_err());
        assert_eq!(result, Err(Error::APIVersionNotSupported))
    }

    #[test]
    fn test_select_api_version() {
        let result = Client::select_api_version(Client::get_max_api_version());
        assert!(result.is_ok());

        let result = Client::select_api_version(510);
        #[cfg(feature = "510")]
        assert!(result.is_ok());
        #[cfg(not(feature = "510"))]
        assert_eq!(result, Err(Error::APIVersionSingletonViolated));
    }

    #[tokio::test]
    async fn init_client_idempotent() {
        let client = Client::new().await;
        let client2 = Client::new().await;
      
        assert!(client.is_ok());
        assert!(client.is_ok());
    }
}
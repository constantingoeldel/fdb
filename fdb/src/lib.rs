use std::ffi::{CStr, CString};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::poll;
use log::{error, info, warn};
use thiserror::Error;
use tokio::task;

use fdb_c::{FDB_API_VERSION, FDB_database, fdb_error_t, FDB_future, fdb_network_set_option, FDBDatabase, FDBKey, FDBKeyValue, FDBNetworkOption, FDBStreamingMode, FDBTenant, FDBTransaction};

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

static API_VERSION_SET: AtomicBool = AtomicBool::new(false);

fn select_api_version(version: i32) -> Result<(), Error> {
    let first_time = API_VERSION_SET
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok();

    if !first_time {
        return Err(Error::APIVersionSingletonViolated);
    }

    if version > FDB_API_VERSION as i32 || version > get_max_api_version() {
        return Err(Error::APIVersionNotSupported);
    }

    if version != FDB_API_VERSION as i32 {
        warn!("Selected API version should almost always be equal to the foundation db version feature")
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

struct FDBNetworkOptions;

/// Singleton Client Instance
/// TODO: Maybe cheaply clone using ARC? Is this needed?
#[derive(Debug)]
struct Client;

static NETWORK_SETUP: AtomicBool = AtomicBool::new(false);
static NETWORK_STARTED: AtomicBool = AtomicBool::new(false);
impl Client {
    async fn new() -> Result<Self, Error> {
        // Init network
        Self::setup_network(FDBNetworkOptions)?;

        let handle: task::JoinHandle<_> = task::spawn_blocking(|| Self::run_network());

        // Poll the network once to check if it errored on initialization
        let poll = poll!(handle);
        if let Poll::Ready(join) = poll {
            join.expect("Could not join network init thread")?;
        }

        Ok(Self)
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

trait FDBResult: Sized {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error>;
}

struct FDBFuture<T> {
    future: FDB_future,
    target: PhantomData<T>,
}

impl<T> Deref for FDBFuture<T> {
    type Target = FDB_future;
    fn deref(&self) -> &Self::Target {
        &self.future
    }
}

impl<T> DerefMut for FDBFuture<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.future
    }
}

impl<T: FDBResult> Future for FDBFuture<T> {
    type Output = Result<T, Error>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut future = self.future;
        let ready = unsafe { fdb_c::fdb_future_is_ready(&mut future) } == 1;

        if !ready {
            return Poll::Pending;
        }

        let error = unsafe { fdb_c::fdb_future_get_error(&mut future) };

        if error != 0 {
            error!("{error}");
            return Poll::Ready(Err(FdbErrorCode(error).into()));
        }

        let result = Poll::Ready(T::from_future(&mut future));

        // The memory referenced by the result is owned by the FDBFuture object and will be valid until fdb_future_destroy(future) is called.
        // All the types implementing FDBResult must have ownership of their content at this point.
        unsafe { fdb_c::fdb_future_destroy(&mut future) }

        result
    }
}

// enum FDBResultTypes {
//     Int64(Int64),
//     KeyArray(KeyArray),
//     Key(Key),
//     Value(Value),
//     StringArray(StringArray),
//     KeyValueArray(KeyValueArray),
// }


struct Int64(i64);

impl FDBResult for Int64 {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error> {
        // Dummy init value
        let mut out: i64 = i64::MIN;
        let result = unsafe { fdb_c::fdb_future_get_int64(future, &mut out) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }
        // Check that dummy value has been overwritten
        // TODO: Is this stupid to check?
        assert_ne!(out, i64::MIN);

        Ok(Int64(out))
    }
}

struct Key(Vec<u8>);

impl Deref for Key {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Key {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<FDBKey> for Key {
    fn from(value: FDBKey) -> Self {
        Key(from_raw_fdb_slice(value.key, value.key_length as usize).to_owned())
    }
}

fn from_raw_fdb_slice<T, U: Into<usize>>(ptr: *const T, len: U) -> &'static [T] {
    if ptr.is_null() {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(ptr, len.into()) }
}

impl FDBResult for Key {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error> {
        let mut key = ptr::null();
        let mut key_length = i32::MIN;
        let result = unsafe { fdb_c::fdb_future_get_key(future, &mut key, &mut key_length) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }
        // Check that dummy value has been overwritten
        assert_ne!(key_length, i32::MIN);

        let key: Key = FDBKey { key, key_length }.into();

        assert_eq!(key.len(), key_length as usize);

        Ok(key)
    }
}

struct KeyArray(Vec<Key>);

impl FDBResult for KeyArray {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error> {
        let mut keys = ptr::null();
        let mut key_count = i32::MIN;
        let result = unsafe { fdb_c::fdb_future_get_key_array(future, &mut keys, &mut key_count) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }
        // Check that dummy value has been overwritten
        // TODO: Is this stupid to check?
        assert_ne!(key_count, i32::MIN);

        let keys: Vec<FDBKey> = from_raw_fdb_slice(keys, key_count as usize).to_owned();
        let keys: Vec<Key> = keys.into_iter().map(|k| k.into()).collect();

        assert_eq!(keys.len(), key_count as usize);

        Ok(KeyArray(keys))
    }
}

struct Value(Option<Vec<u8>>);

impl FDBResult for Value {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error> {
        let mut present = i32::MIN;
        let mut value = ptr::null();
        let mut value_length = i32::MIN;
        let result = unsafe {
            fdb_c::fdb_future_get_value(future, &mut present, &mut value, &mut value_length)
        };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        // Value is not present in the database
        if present == 0 {
            return Ok(Value(None));
        }

        // Check that dummy value has been overwritten
        // TODO: Is this stupid to check?
        assert_ne!(value_length, i32::MIN);
        assert_ne!(present, i32::MIN);

        let value = from_raw_fdb_slice(value, value_length as usize).to_owned();

        assert_eq!(value.len(), value_length as usize);

        Ok(Value(Some(value)))
    }
}

struct StringArray(Vec<String>);

impl FDBResult for StringArray {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error> {
        let mut strings: *mut *const c_char = ptr::null_mut();
        let mut count = i32::MIN;

        let result =
            unsafe { fdb_c::fdb_future_get_string_array(future, &mut strings, &mut count) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        // Check that dummy value has been overwritten
        // TODO: Is this stupid to check?
        assert_ne!(count, i32::MIN);

        let strings = from_raw_fdb_slice(strings, count as usize);
        let strings = strings
            .iter()
            .map(|s| {
                unsafe { CStr::from_ptr(*s) }
                    .to_str()
                    .expect("Could not convert C String to String")
                    .to_owned()
            })
            .collect();

        Ok(StringArray(strings))
    }
}

struct KeyValueArray(Vec<(Key, Value)>);

impl FDBResult for KeyValueArray {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error> {
        let mut kvs = ptr::null();
        let mut count = i32::MIN;
        let mut more_remaining = i32::MIN;

        let result = unsafe {
            fdb_c::fdb_future_get_keyvalue_array(future, &mut kvs, &mut count, &mut more_remaining)
        };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        // Check that dummy value has been overwritten
        // TODO: Is this stupid to check?
        assert_ne!(count, i32::MIN);
        assert_ne!(more_remaining, i32::MIN);

        // TODO: Was mit more_remaining anstellen?

        let kvs = from_raw_fdb_slice(kvs, count as usize);
        let kvs = kvs
            .iter()
            .map(|kv| {
                let key = Key(from_raw_fdb_slice(kv.key, kv.key_length as usize).to_owned());
                let value = Value(Some(
                    from_raw_fdb_slice(kv.value, kv.value_length as usize).to_owned(),
                ));
                (key, value)
            })
            .collect();

        Ok(KeyValueArray(kvs))
    }
}

struct Database(FDBDatabase);
struct Tenant(FDBTenant);

impl Database {
    /// Creates a new database connected the specified cluster. The caller assumes ownership of the
    /// FDBDatabase object and must destroy it with fdb_database_destroy() (Implemented to automatically happen on Drop).
    ///
    /// --- TODO: Not implemented yet, always uses the default cluster file ---
    /// A single client can use this function multiple times to connect to different clusters
    /// simultaneously, with each invocation requiring its own cluster file.
    /// To connect to multiple clusters running at different, incompatible versions, the multi-version client API must be used.
    fn new() -> Result<Self, Error> {
        let mut db = ptr::null_mut();
        let cluster_file_path = c"";

        let result = unsafe { fdb_c::fdb_create_database(cluster_file_path.as_ptr(), db) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        };

        Ok(Database(unsafe {**db}))
    }

    fn set_option() -> Result<(), Error> {
        todo!()
    }

    fn tenant(&mut self, name: &str) -> Result<Tenant, Error> {
        let tenant_name = name.as_bytes();
        let mut tenant = ptr::null_mut();

        let result = unsafe { fdb_c::fdb_database_open_tenant(&mut self.0, tenant_name.as_ptr(), tenant_name.len() as i32, &mut tenant) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        Ok(Tenant(unsafe {*tenant}))
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
        unsafe { fdb_c::fdb_database_get_main_thread_busyness(&mut self.0) }
    }

}

impl Drop for Database {
    /// Destroys an FDBDatabase object. It must be called exactly once for each successful call to
    /// fdb_create_database(). This function only destroys a handle to the database – your database will be fine!
    fn drop(&mut self) {
        unsafe { fdb_c::fdb_database_destroy(&mut self.0) };
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

struct Transaction(FDBTransaction);

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe { fdb_c::fdb_transaction_destroy(&mut self.0) };
    }
}

trait CreateTransaction {
    fn create_transaction(&mut self) -> Result<Transaction, Error>;
}

impl CreateTransaction for Database {
    fn create_transaction(&mut self) -> Result<Transaction, Error> {
        let mut trx = ptr::null_mut();
        let result = unsafe { fdb_c::fdb_database_create_transaction(&mut self.0, &mut trx) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        Ok(Transaction(unsafe {*trx}))
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

        Ok(Transaction(unsafe {*trx}))

    }
}

impl Transaction {
    fn set_option() -> Result<(), Error> {
        todo!()
    }

    fn set_read_version() -> Result<(), Error> {
        todo!()
    }

    /// Reads a value from the database
    async  fn get(&mut self, key: Key, snapshot: bool) -> Result<Value, Error> {

        let future = unsafe { fdb_c::fdb_transaction_get(&mut self.0, key.as_ptr(), key.len() as i32, snapshot as i32) };
        // TODO: make this conversion a method of future
        let future = FDBFuture { future: unsafe { *future} , target: PhantomData };

        future.await
    }

    /// Returns an estimated byte size of the key range.
    /// 
    /// The estimated size is calculated based on the sampling done by FDB server.
    /// The sampling algorithm works roughly in this way: the larger the key-value pair is,
    /// the more likely it would be sampled and the more accurate its sampled size would be.
    /// And due to that reason it is recommended to use this API to query against large ranges for
    /// accuracy considerations.
    /// For a rough reference, if the returned size is larger than 3MB, one can consider the size to be accurate.
    async fn get_estimated_range_size(&mut self, start: Key, end: Key) -> Result<Int64, Error> {

        let future = unsafe { fdb_c::fdb_transaction_get_estimated_range_size_bytes(&mut self.0, start.as_ptr(), start.len() as i32, end.as_ptr(), end.len() as i32) };

        let future = FDBFuture { future: unsafe { *future }, target: PhantomData };

        future.await
        
    }
    /// Returns a list of keys that can split the given range into (roughly) equally sized chunks based on chunk_size.
    async fn get_range_split_points(&mut self, start: Key, end: Key, chunk_size: i64) -> Result<KeyArray, Error> {
        let future = unsafe { fdb_c::fdb_transaction_get_range_split_points(&mut self.0, start.as_ptr(), start.len() as i32, end.as_ptr(), end.len() as i32, chunk_size) };

        let future = FDBFuture { future: unsafe { *future }, target: PhantomData };

        future.await
    }
    
    /// Resolves a key selector against the keys in the database snapshot represented by transaction.
    async fn get_key(&mut self, key: Key, offset: i32, inclusive: bool, snapshot: bool )  -> Result<Key, Error> {
        
        let future = unsafe { fdb_c::fdb_transaction_get_key(&mut self.0, key.as_ptr(), key.len() as i32, inclusive as i32, offset,  snapshot as i32) };
        
        let future = FDBFuture { future: unsafe { *future }, target: PhantomData };
        
        future.await
    }
    
    async fn get_first_key_greater_or_equal_than(&mut self, key: Key, offset: i32, snapshot: bool) ->Result<Key, Error> {
        self.get_key(key, 1 + offset, true, snapshot).await
    }

    async fn get_first_key_greater_than(&mut self, key: Key, offset: i32, snapshot: bool) ->Result<Key, Error> {
        self.get_key(key, 1 + offset, false, snapshot).await
    }

    async fn get_last_key_less_or_equal_than(&mut self, key: Key, offset: i32, snapshot: bool) ->Result<Key, Error> {
        self.get_key(key, -1 + offset, true, snapshot).await
    }

    async fn get_last_key_less_than(&mut self, key: Key, offset: i32, snapshot: bool) ->Result<Key, Error> {
        self.get_key(key, -1 + offset, false, snapshot).await
    }

    /// Returns a list of public network addresses as strings, one for each of the storage servers
    /// responsible for storing the key and its associated value.
    async fn get_key_addresses(&mut self, key: Key) -> Result<StringArray, Error> {
        let future = unsafe { fdb_c::fdb_transaction_get_addresses_for_key(&mut self.0, key.as_ptr(), key.len() as i32) };

        let future = FDBFuture { future: unsafe { *future }, target: PhantomData };

        future.await
    }
    
    async fn get_range(&mut self, start: Key, begin_inclusive: bool, begin_offset: i32, end: Key, end_inclusive: bool, end_offset: i32, limit: Option<i32>, target_bytes: Option<i32>, mode: FDBStreamingMode, iteration: i32, snapshot: bool, reverse: bool ) -> Result<KeyValueArray, Error> {
        
    }
    
    
    
}




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_version() {
        #[cfg(feature = "fdb_730")]
        assert_eq!(fdb_c::FDB_API_VERSION, 730);
        #[cfg(feature = "fdb_710")]
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
}

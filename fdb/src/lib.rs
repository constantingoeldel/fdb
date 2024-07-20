use std::ffi::{CStr, CString};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{BitAnd, Deref, DerefMut};
use std::os::fd::IntoRawFd;
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use async_stream::try_stream;
use futures::{poll, Stream};
use log::{error, info, warn};
use thiserror::Error;
use tokio::task;

use fdb_c::{
    fdb_error_t, fdb_network_set_option, FDBConflictRangeType_FDB_CONFLICT_RANGE_TYPE_READ,
    FDBConflictRangeType_FDB_CONFLICT_RANGE_TYPE_WRITE, FDBDatabase, FDBKey, FDBKeyValue,
    FDBMutationType_FDB_MUTATION_TYPE_ADD, FDBMutationType_FDB_MUTATION_TYPE_AND,
    FDBMutationType_FDB_MUTATION_TYPE_BYTE_MAX, FDBMutationType_FDB_MUTATION_TYPE_BYTE_MIN,
    FDBMutationType_FDB_MUTATION_TYPE_COMPARE_AND_CLEAR, FDBMutationType_FDB_MUTATION_TYPE_MAX,
    FDBMutationType_FDB_MUTATION_TYPE_MIN, FDBMutationType_FDB_MUTATION_TYPE_OR,
    FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY,
    FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE,
    FDBMutationType_FDB_MUTATION_TYPE_XOR, FDBNetworkOption, FDBStreamingMode, FDBTenant,
    FDBTransaction, FDB_database, FDB_future, FDB_API_VERSION,
};

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

struct Empty(());

impl FDBResult for Empty {
    fn from_future(future: &mut FDB_future) -> Result<Self, Error> {
        return Ok(Empty(()));
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

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        let bytes = value.as_bytes();
        Self(bytes.to_vec())
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

struct Value(Vec<u8>);

impl Deref for Value {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
            return Err(Error::KeyNotFound);
        }

        // Check that dummy value has been overwritten
        // TODO: Is this stupid to check?
        assert_ne!(value_length, i32::MIN);
        assert_ne!(present, i32::MIN);

        let value = from_raw_fdb_slice(value, value_length as usize).to_owned();

        assert_eq!(value.len(), value_length as usize);

        Ok(Value(value))
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
                let value =
                    Value(from_raw_fdb_slice(kv.value, kv.value_length as usize).to_owned());
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

        Ok(Database(unsafe { **db }))
    }

    fn set_option() -> Result<(), Error> {
        todo!()
    }

    fn tenant(&mut self, name: &str) -> Result<Tenant, Error> {
        let tenant_name = name.as_bytes();
        let mut tenant = ptr::null_mut();

        let result = unsafe {
            fdb_c::fdb_database_open_tenant(
                &mut self.0,
                tenant_name.as_ptr(),
                tenant_name.len() as i32,
                &mut tenant,
            )
        };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        Ok(Tenant(unsafe { *tenant }))
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

        Ok(Transaction(unsafe { *trx }))
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

        Ok(Transaction(unsafe { *trx }))
    }
}

type TransactionLogic<R> = fn(&mut Transaction) -> R;

async fn exec(
    mut tx: Transaction,
    f: TransactionLogic<impl Future<Output = Result<(), Error>>>,
) -> Result<(), Error> {
    let result = f(&mut tx).await;

    match result {
        Ok(r) => Ok(r),
        Err(e) => {
            let error_code = FdbErrorCode::from(&e);
            let error_handling_fut =
                unsafe { fdb_c::fdb_transaction_on_error(&mut tx.0, error_code.0) };
            let error_handling_fut: FDBFuture<Empty> = FDBFuture {
                future: unsafe { *error_handling_fut },
                target: PhantomData,
            };

            let should_be_retried = error_handling_fut.await.is_ok();

            if should_be_retried {
                // Recursion in async functions requires boxing
                Box::pin(exec(tx, f)).await
            } else {
                Err(e)
            }
        }
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
    async fn _get(&mut self, key: Key, snapshot: bool) -> Result<Value, Error> {
        let future = unsafe {
            fdb_c::fdb_transaction_get(&mut self.0, key.as_ptr(), key.len() as i32, snapshot as i32)
        };
        // TODO: make this conversion a method of future
        let future = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        future.await
    }

    async fn get<K: Into<Key>>(&mut self, key: K) -> Result<Value, Error> {
        self._get(key.into(), false).await
    }
    async fn snapshot_get<K: Into<Key>>(&mut self, key: K) -> Result<Value, Error> {
        self._get(key.into(), true).await
    }

    /// Returns an estimated byte size of the key range.
    ///
    /// The estimated size is calculated based on the sampling done by FDB server.
    /// The sampling algorithm works roughly in this way: the larger the key-value pair is,
    /// the more likely it would be sampled and the more accurate its sampled size would be.
    /// And due to that reason it is recommended to use this API to query against large ranges for
    /// accuracy considerations.
    /// For a rough reference, if the returned size is larger than 3MB, one can consider the size to be accurate.
    ///
    /// TODO: Does this include the size of the keys as well?
    async fn get_estimated_range_size<K: Into<Key>>(
        &mut self,
        start: K,
        end: K,
    ) -> Result<i64, Error> {
        let start = start.into();
        let end = end.into();
        let future = unsafe {
            fdb_c::fdb_transaction_get_estimated_range_size_bytes(
                &mut self.0,
                start.as_ptr(),
                start.len() as i32,
                end.as_ptr(),
                end.len() as i32,
            )
        };

        let future = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        let size: Int64 = future.await?;

        Ok(size.0)
    }
    /// Returns a list of keys that can split the given range into (roughly) equally sized chunks based on chunk_size.
    async fn get_range_split_points<K: Into<Key>>(
        &mut self,
        start: K,
        end: K,
        chunk_size: i64,
    ) -> Result<KeyArray, Error> {
        let start = start.into();
        let end = end.into();
        let future = unsafe {
            fdb_c::fdb_transaction_get_range_split_points(
                &mut self.0,
                start.as_ptr(),
                start.len() as i32,
                end.as_ptr(),
                end.len() as i32,
                chunk_size,
            )
        };

        let future = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        future.await
    }

    /// Resolves a key selector against the keys in the database snapshot represented by transaction.
    async fn _get_key(
        &mut self,
        key: Key,
        offset: i32,
        inclusive: bool,
        snapshot: bool,
    ) -> Result<Key, Error> {
        let future = unsafe {
            fdb_c::fdb_transaction_get_key(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                inclusive as i32,
                offset,
                snapshot as i32,
            )
        };

        let future = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        future.await
    }

    async fn get_key<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
        inclusive: bool,
    ) -> Result<Key, Error> {
        self._get_key(key.into(), offset, inclusive, false).await
    }

    async fn snapshot_get_key<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
        inclusive: bool,
    ) -> Result<Key, Error> {
        self._get_key(key.into(), offset, inclusive, true).await
    }

    async fn _get_first_key_greater_or_equal_than(
        &mut self,
        key: Key,
        offset: i32,
        snapshot: bool,
    ) -> Result<Key, Error> {
        self._get_key(key, 1 + offset, true, snapshot).await
    }

    async fn get_first_key_greater_or_equal_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_first_key_greater_or_equal_than(key.into(), offset, false)
            .await
    }
    async fn snapshot_get_first_key_greater_or_equal_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_first_key_greater_or_equal_than(key.into(), offset, true)
            .await
    }

    async fn _get_first_key_greater_than(
        &mut self,
        key: Key,
        offset: i32,
        snapshot: bool,
    ) -> Result<Key, Error> {
        self._get_key(key, 1 + offset, false, snapshot).await
    }

    async fn get_first_key_greater_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_first_key_greater_than(key.into(), offset, false)
            .await
    }
    async fn snapshot_get_first_key_greater_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_first_key_greater_than(key.into(), offset, true)
            .await
    }

    async fn _get_last_key_less_or_equal_than(
        &mut self,
        key: Key,
        offset: i32,
        snapshot: bool,
    ) -> Result<Key, Error> {
        self._get_key(key, -1 + offset, true, snapshot).await
    }

    async fn get_last_key_less_or_equal_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_last_key_less_or_equal_than(key.into(), offset, false)
            .await
    }
    async fn snapshot_get_last_key_less_or_equal_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_last_key_less_or_equal_than(key.into(), offset, true)
            .await
    }

    async fn _get_last_key_less_than(
        &mut self,
        key: Key,
        offset: i32,
        snapshot: bool,
    ) -> Result<Key, Error> {
        self._get_key(key, -1 + offset, false, snapshot).await
    }

    async fn get_last_key_less_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_last_key_less_than(key.into(), offset, false)
            .await
    }
    async fn snapshot_get_last_key_less_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_last_key_less_than(key.into(), offset, true).await
    }

    /// Returns a list of public network addresses as strings, one for each of the storage servers
    /// responsible for storing the key and its associated value.
    async fn get_key_addresses<K: Into<Key>>(&mut self, key: K) -> Result<StringArray, Error> {
        let key = key.into();
        let future = unsafe {
            fdb_c::fdb_transaction_get_addresses_for_key(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
            )
        };

        let future = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        future.await
    }

    /// Return Keys and Values within a given range as a stream of `(Key, Value)` tuples.
    ///
    /// TODO: Check Lifetime of returned tuples corresponds to lifetime of transaction
    async fn get_range(
        &mut self,
        start: KeySelector,
        end: KeySelector,
        limit: Option<i32>,
        target_bytes: Option<i32>,
        snapshot: bool,
        reverse: bool,
    ) -> impl Stream<Item = Result<(Key, Value), Error>> + '_ {
        try_stream! {
                let mut iteration = 0;
                let mode = fdb_c::FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR;

                 loop {
                    let future = unsafe { fdb_c::fdb_transaction_get_range(&mut self.0, start.key.as_ptr(), start.key.len() as i32, start.inclusive as i32, start.offset, end.key.as_ptr(), end.key.len() as i32, end.inclusive as i32, end.offset, limit.unwrap_or(0), target_bytes.unwrap_or(0), mode, iteration, snapshot as i32, reverse as i32) };

                    let future = FDBFuture { future: unsafe { *future }, target: PhantomData };

                    let mut result: KeyValueArray = future.await?;

                    if result.0.is_empty() {
                        // All range items have been returned
                        break;
                    }

                    iteration += 1;
                        for r in result.0 {
                            yield r;
                        }
                }
        }
    }

    /// Infallible because setting happens client-side until commiting the transaction
    async fn set<K: Into<Key>, V: Into<Value>>(&mut self, key: K, value: V) {
        let key = key.into();
        let value = value.into();
        unsafe {
            fdb_c::fdb_transaction_set(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
            )
        };
    }

    /// Infallible because clearing stays client-side until commiting the transaction
    async fn clear<K: Into<Key>>(&mut self, key: K) {
        let key = key.into();
        unsafe { fdb_c::fdb_transaction_clear(&mut self.0, key.as_ptr(), key.len() as i32) }
    }

    async fn clear_range<K: Into<Key>>(&mut self, start: K, end: K) {
        let start = start.into();
        let end = end.into();
        unsafe {
            fdb_c::fdb_transaction_clear_range(
                &mut self.0,
                start.as_ptr(),
                start.len() as i32,
                end.as_ptr(),
                end.len() as i32,
            )
        }
    }

    async fn atomic_add<K: Into<Key>>(&mut self, key: K, other: i32) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_ADD;
        let addend = other.to_le_bytes();

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                addend.as_ptr(),
                32 / 8,
                operation_type,
            )
        }
    }

    /// Performs a bitwise “and” operation
    ///
    /// TODO: better datatype for other (Maybe something like impl BitAnd?)
    async fn atomic_and<K: Into<Key>>(&mut self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_AND;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    async fn atomic_or<K: Into<Key>>(&mut self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_OR;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    async fn atomic_xor<K: Into<Key>>(&mut self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_XOR;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    /// Performs an atomic compare and clear operation. If the existing value in the database is equal to the given value, then given key is cleared.
    async fn atomic_compare_and_clear<K: Into<Key>>(&mut self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_COMPARE_AND_CLEAR;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    /// Sets the value in the database to the larger of the existing value and other. If the key is not present, other is stored
    async fn atomic_max<K: Into<Key>>(&mut self, key: K, other: u32) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_MAX;

        let other = other.to_le_bytes();

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                32 / 8,
                operation_type,
            )
        }
    }

    /// Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then other is stored.
    /// Otherwise, the larger of the two values is then stored in the database.
    async fn atomic_byte_max<K: Into<Key>>(&mut self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_BYTE_MAX;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    /// Sets the value in the database to the smaller of the existing value and other. If the key is not present, other is stored
    async fn atomic_min<K: Into<Key>>(&mut self, key: K, other: u32) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_MIN;

        let other = other.to_le_bytes();

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                32 / 8,
                operation_type,
            )
        }
    }

    /// Performs lexicographic comparison of byte strings. If the existing value in the database is not present, then other is stored.
    /// Otherwise, the smaller of the two values is then stored in the database.
    async fn atomic_byte_min<K: Into<Key>>(&mut self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_BYTE_MIN;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    /// Atomic version of set()
    ///
    /// Collisions with other transactions are circumvented by replacing part of the key with the versionstamp of this transaction, ensuring uniqueness.
    ///
    /// Replacement mechanism:
    ///
    /// The final 4 bytes of the key will be interpreted as a 32-bit little-endian integer denoting
    /// an index into the key at which to perform the transformation, and then trimmed off the key.
    /// The 10 bytes in the key beginning at the index will be overwritten with the versionstamp.
    /// If the index plus 10 bytes points past the end of the key, the result will be an error.
    ///
    /// TODO: Use special versionstamped Key type for this
    ///
    /// A transaction is not permitted to read any transformed key or value previously set within
    /// that transaction, and an attempt to do so will result in an accessed_unreadable error.
    /// The range of keys marked unreadable when setting a versionstamped key begins at the
    /// transactions’s read version if it is known, otherwise a versionstamp of all 0x00 bytes
    /// is conservatively assumed. The upper bound of the unreadable range is a versionstamp of all 0xFF bytes
    async fn atomic_set_versionstamped_key<K: Into<Key>>(&mut self, key: K, value: Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
                operation_type,
            )
        }
    }

    /// Another Atomic version of set()
    ///
    /// Collisions with other transactions are ignored and part of the value of this transaction is overwritten with the versionstamp, ensuring global ordering.
    ///
    /// Replacement mechanism:
    ///
    /// The final 4 bytes of the value will be interpreted as a 32-bit little-endian integer denoting
    /// an index into the value at which to perform the transformation, and then trimmed off the key.
    /// The 10 bytes in the value beginning at the index will be overwritten with the versionstamp.
    /// If the index plus 10 bytes points past the end of the value, the result will be an error.
    ///
    /// TODO: Use special versionstamped Value type for this
    ///
    /// A transaction is not permitted to read any transformed key or value previously set within
    /// that transaction, and an attempt to do so will result in an accessed_unreadable error.
    /// The range of keys marked unreadable when setting a versionstamped key begins at the
    /// transactions’s read version if it is known, otherwise a versionstamp of all 0x00 bytes is
    /// conservatively assumed. The upper bound of the unreadable range is a versionstamp of all 0xFF bytes
    async fn atomic_set_versionstamped_value<K: Into<Key>>(&mut self, key: K, value: Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                &mut self.0,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
                operation_type,
            )
        }
    }

    /// Returns the approximate transaction size so far in the returned future, which is the summation
    /// of the estimated size of mutations, read conflict ranges, and write conflict ranges.
    ///
    /// This can be called multiple times before the transaction is committed.
    ///
    /// The maximum allowed transaction size is 10MB.
    async fn get_approximate_size(&mut self) -> Result<Int64, Error> {
        let future = unsafe { fdb_c::fdb_transaction_get_approximate_size(&mut self.0) };

        let future = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        future.await
    }

    /// TODO: perhaps make this a method of Key? How to implement cancelling watches?
    async fn watch<K: Into<Key>>(&mut self, key: K) -> Result<Empty, Error> {
        let key = key.into();
        let future =
            unsafe { fdb_c::fdb_transaction_watch(&mut self.0, key.as_ptr(), key.len() as i32) };

        let future = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        future.await
    }

    /// Adds a conflict range to a transaction without performing the associated read or write.
    async fn add_conflict_range<K: Into<Key>>(
        &mut self,
        start: K,
        end: K,
        conflict_type: ConflictType,
    ) -> Result<(), Error> {
        let start = start.into();
        let end = end.into();
        let t = match conflict_type {
            ConflictType::Read => FDBConflictRangeType_FDB_CONFLICT_RANGE_TYPE_READ,
            ConflictType::Write => FDBConflictRangeType_FDB_CONFLICT_RANGE_TYPE_WRITE,
        };
        let result = unsafe {
            fdb_c::fdb_transaction_add_conflict_range(
                &mut self.0,
                start.as_ptr(),
                start.len() as i32,
                end.as_ptr(),
                end.len() as i32,
                t,
            )
        };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }

        Ok(())
    }

    /// Cancels the transaction.
    async fn cancel(mut self) {
        unsafe { fdb_c::fdb_transaction_cancel(&mut self.0) }
    }

    /// Consume a readonly transaction, thereby destroying it (readonly transactions don't need to be committed)
    async fn commit_readonly(self) {
        drop(self)
    }

    async fn commit(mut self) -> Result<(), Error> {
        let future = unsafe { fdb_c::fdb_transaction_commit(&mut self.0) };

        let commit_fut = FDBFuture {
            future: unsafe { *future },
            target: PhantomData,
        };

        let _commited: Empty = commit_fut.await?;

        Ok(())
    }

    // Not implemented: (Because not deemed necessary)
    // -fdb_transaction_get_committed_version
    // - fdb_transaction_get_versionstamp
    // - reset (just create a new one)
}

enum ConflictType {
    Read,
    Write,
}

struct KeySelector {
    // Key the selector starts from
    key: Key,
    inclusive: bool,
    offset: i32,
}

impl KeySelector {
    fn set_inclusive(mut self, to: bool) -> Self {
        self.inclusive = to;
        self
    }

    fn set_offset(mut self, to: i32) -> Self {
        self.offset = to;
        self
    }
}

/// Default Conversion
impl From<Key> for KeySelector {
    fn from(value: Key) -> Self {
        KeySelector {
            key: value,
            inclusive: true,
            offset: 0,
        }
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

    #[tokio::test]
    async fn test_simple_transaction() {
        let mut db = Database::new().unwrap();
        let mut tx = db.create_transaction().unwrap();

        let empty_get = tx.get("hello").await;
        assert_eq!(empty_get, Error::KeyNotFound);

        tx.set("hello", "world").await;

        let existing_get = tx.get("hello").await;
        assert_eq!(existing_get, "world");
    }
}

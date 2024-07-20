use std::future::Future;

use async_stream::try_stream;
use futures::Stream;
use log::error;

use fdb_c::{FDBConflictRangeType_FDB_CONFLICT_RANGE_TYPE_READ, FDBConflictRangeType_FDB_CONFLICT_RANGE_TYPE_WRITE, FDBMutationType_FDB_MUTATION_TYPE_ADD, FDBMutationType_FDB_MUTATION_TYPE_AND, FDBMutationType_FDB_MUTATION_TYPE_BYTE_MAX, FDBMutationType_FDB_MUTATION_TYPE_BYTE_MIN, FDBMutationType_FDB_MUTATION_TYPE_COMPARE_AND_CLEAR, FDBMutationType_FDB_MUTATION_TYPE_MAX, FDBMutationType_FDB_MUTATION_TYPE_MIN, FDBMutationType_FDB_MUTATION_TYPE_OR, FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY, FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE, FDBMutationType_FDB_MUTATION_TYPE_XOR, FDBTransaction};

use crate::{Error, FdbErrorCode};
use crate::future::FDBFuture;
use crate::types::{Empty, Int64, Key, KeyArray, KeySelector, KeyValueArray, StringArray, Value};

pub struct Transaction(*mut FDBTransaction);

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe { fdb_c::fdb_transaction_destroy(self.0) };
    }
}

impl From<*mut FDBTransaction> for Transaction {
    fn from(value: *mut FDBTransaction) -> Self {
        Transaction(value)
    }
}

pub trait CreateTransaction {
    fn create_transaction(&self) -> Result<Transaction, Error>;
}


type TransactionLogic<R> = fn(&mut Transaction) -> R;

async fn exec(
    mut tx: Transaction,
    f: TransactionLogic<impl Future<Output=Result<(), Error>>>,
) -> Result<(), Error> {
    let result = f(&mut tx).await;

    match result {
        Ok(r) => Ok(r),
        Err(e) => {
            let error_code = FdbErrorCode::from(&e);
            let error_handling_fut: FDBFuture<Empty> =
                unsafe { fdb_c::fdb_transaction_on_error(tx.0, error_code.0) }.into();

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
    async fn _get(&self, key: Key, snapshot: bool) -> Result<Value, Error> {
        let future: FDBFuture<Value> = unsafe {
            fdb_c::fdb_transaction_get(self.0, key.as_ptr(), key.len() as i32, snapshot as i32)
        }.into();
        let handle = tokio::spawn(future);
        handle.await.unwrap()
    }

    pub async fn get<K: Into<Key>>(&self, key: K) -> Result<Value, Error> {
        self._get(key.into(), false).await
    }
    pub async fn snapshot_get<K: Into<Key>>(&self, key: K) -> Result<Value, Error> {
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
    pub async fn get_estimated_range_size<K: Into<Key>>(
        &mut self,
        start: K,
        end: K,
    ) -> Result<i64, Error> {
        let start = start.into();
        let end = end.into();
        let future: FDBFuture<Int64> = unsafe {
            fdb_c::fdb_transaction_get_estimated_range_size_bytes(
                self.0,
                start.as_ptr(),
                start.len() as i32,
                end.as_ptr(),
                end.len() as i32,
            )
        }.into();


        let size = future.await?;

        Ok(size.0)
    }
    /// Returns a list of keys that can split the given range into (roughly) equally sized chunks based on chunk_size.
    pub async fn get_range_split_points<K: Into<Key>>(
        &mut self,
        start: K,
        end: K,
        chunk_size: i64,
    ) -> Result<KeyArray, Error> {
        let start = start.into();
        let end = end.into();
        let future: FDBFuture<KeyArray> = unsafe {
            fdb_c::fdb_transaction_get_range_split_points(
                self.0,
                start.as_ptr(),
                start.len() as i32,
                end.as_ptr(),
                end.len() as i32,
                chunk_size,
            )
        }.into();


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
        let future: FDBFuture<Key> = unsafe {
            fdb_c::fdb_transaction_get_key(
                self.0,
                key.as_ptr(),
                key.len() as i32,
                inclusive as i32,
                offset,
                snapshot as i32,
            )
        }.into();


        future.await
    }

    pub async fn get_key<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
        inclusive: bool,
    ) -> Result<Key, Error> {
        self._get_key(key.into(), offset, inclusive, false).await
    }

    pub async fn snapshot_get_key<K: Into<Key>>(
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

    pub async fn get_first_key_greater_or_equal_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_first_key_greater_or_equal_than(key.into(), offset, false)
            .await
    }
    pub async fn snapshot_get_first_key_greater_or_equal_than<K: Into<Key>>(
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

    pub async fn get_first_key_greater_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_first_key_greater_than(key.into(), offset, false)
            .await
    }
    pub async fn snapshot_get_first_key_greater_than<K: Into<Key>>(
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

    pub async fn get_last_key_less_or_equal_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_last_key_less_or_equal_than(key.into(), offset, false)
            .await
    }
    pub async fn snapshot_get_last_key_less_or_equal_than<K: Into<Key>>(
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

    pub async fn get_last_key_less_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_last_key_less_than(key.into(), offset, false)
            .await
    }
    pub async fn snapshot_get_last_key_less_than<K: Into<Key>>(
        &mut self,
        key: K,
        offset: i32,
    ) -> Result<Key, Error> {
        self._get_last_key_less_than(key.into(), offset, true).await
    }

    /// Returns a list of public network addresses as strings, one for each of the storage servers
    /// responsible for storing the key and its associated value.
    pub async fn get_key_addresses<K: Into<Key>>(&self, key: K) -> Result<StringArray, Error> {
        let key = key.into();
        let future: FDBFuture<StringArray> = unsafe {
            fdb_c::fdb_transaction_get_addresses_for_key(
                self.0,
                key.as_ptr(),
                key.len() as i32,
            )
        }.into();

        future.await
    }

    /// Return Keys and Values within a given range as a stream of `(Key, Value)` tuples.
    ///
    /// TODO: Check Lifetime of returned tuples corresponds to lifetime of transaction
    pub async fn get_range(
        &mut self,
        start: KeySelector,
        end: KeySelector,
        limit: Option<i32>,
        target_bytes: Option<i32>,
        snapshot: bool,
        reverse: bool,
    ) -> impl Stream<Item=Result<(Key, Value), Error>> + '_ {
        try_stream! {
                let mut iteration = 0;
                let mode = fdb_c::FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR;

                 loop {
                    let future: FDBFuture<KeyValueArray> = unsafe { fdb_c::fdb_transaction_get_range(self.0, start.key.as_ptr(), start.key.len() as i32, start.inclusive as i32, start.offset, end.key.as_ptr(), end.key.len() as i32, end.inclusive as i32, end.offset, limit.unwrap_or(0), target_bytes.unwrap_or(0), mode, iteration, snapshot as i32, reverse as i32) }.into();

                    let result = future.await?;

                    if result.is_empty() {
                        // All range items have been returned
                        break;
                    }

                    iteration += 1;
                        for r in result.0.into_iter() {
                            yield r;
                        }
                }
        }
    }

    /// Infallible because setting happens client-side until commiting the transaction
    pub async fn set<K: Into<Key>, V: Into<Value>>(&self, key: K, value: V) {
        let key = key.into();
        let value = value.into();
        unsafe {
            fdb_c::fdb_transaction_set(
                self.0,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
            )
        };
    }

    /// Infallible because clearing stays client-side until commiting the transaction
    pub async fn clear<K: Into<Key>>(&self, key: K) {
        let key = key.into();
        unsafe { fdb_c::fdb_transaction_clear(self.0, key.as_ptr(), key.len() as i32) }
    }

    pub async fn clear_range<K: Into<Key>>(&self, start: K, end: K) {
        let start = start.into();
        let end = end.into();
        unsafe {
            fdb_c::fdb_transaction_clear_range(
                self.0,
                start.as_ptr(),
                start.len() as i32,
                end.as_ptr(),
                end.len() as i32,
            )
        }
    }

    pub async fn atomic_add<K: Into<Key>>(&self, key: K, other: i32) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_ADD;
        let addend = other.to_le_bytes();

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
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
    pub async fn atomic_and<K: Into<Key>>(&self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_AND;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    pub async fn atomic_or<K: Into<Key>>(&self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_OR;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    pub async fn atomic_xor<K: Into<Key>>(&self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_XOR;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    /// Performs an atomic compare and clear operation. If the existing value in the database is equal to the given value, then given key is cleared.
    pub async fn atomic_compare_and_clear<K: Into<Key>>(&self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_COMPARE_AND_CLEAR;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    /// Sets the value in the database to the larger of the existing value and other. If the key is not present, other is stored
    pub async fn atomic_max<K: Into<Key>>(&self, key: K, other: u32) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_MAX;

        let other = other.to_le_bytes();

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
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
    pub async fn atomic_byte_max<K: Into<Key>>(&self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_BYTE_MAX;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
                key.as_ptr(),
                key.len() as i32,
                other.as_ptr(),
                other.len() as i32,
                operation_type,
            )
        }
    }

    /// Sets the value in the database to the smaller of the existing value and other. If the key is not present, other is stored
    pub async fn atomic_min<K: Into<Key>>(&self, key: K, other: u32) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_MIN;

        let other = other.to_le_bytes();

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
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
    pub async fn atomic_byte_min<K: Into<Key>>(&self, key: K, other: &Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_BYTE_MIN;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
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
    pub async fn atomic_set_versionstamped_key<K: Into<Key>>(&self, key: K, value: Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
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
    pub async fn atomic_set_versionstamped_value<K: Into<Key>>(&self, key: K, value: Value) {
        let key = key.into();
        let operation_type = FDBMutationType_FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE;

        unsafe {
            fdb_c::fdb_transaction_atomic_op(
                self.0,
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
    pub async fn get_approximate_size(&self) -> Result<Int64, Error> {
        let future: FDBFuture<Int64> = unsafe { fdb_c::fdb_transaction_get_approximate_size(self.0) }.into();

        future.await
    }

    /// TODO: perhaps make this a method of Key? How to implement cancelling watches?
    pub async fn watch<K: Into<Key>>(&self, key: K) -> Result<(), Error> {
        let key = key.into();
        let future: FDBFuture<Empty> =
            unsafe { fdb_c::fdb_transaction_watch(self.0, key.as_ptr(), key.len() as i32) }.into();


        let _changed = future.await?;

        Ok(())
    }

    /// Adds a conflict range to a transaction without performing the associated read or write.
    pub async fn add_conflict_range<K: Into<Key>>(
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
                self.0,
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
    pub async fn cancel(mut self) {
        unsafe { fdb_c::fdb_transaction_cancel(self.0) }
    }

    /// Consume a readonly transaction, thereby destroying it (readonly transactions don't need to be committed)
    pub async fn commit_readonly(self) {
        drop(self)
    }

    pub async fn commit(mut self) -> Result<(), Error> {
        let future: FDBFuture<Empty> = unsafe { fdb_c::fdb_transaction_commit(self.0) }.into();

        let _commited = future.await?;

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

use std::ffi::CStr;
use std::future::Future;
use std::marker::PhantomData;
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, Waker};

use log::error;

use fdb_c::FDB_future;

use crate::{Error, FdbErrorCode};
use crate::types::*;

pub trait FDBResult: Sized {
    fn from_future(future: *mut FDB_future) -> Result<Self, Error>;
}

pub struct FDBFuture<T> {
    future: *mut FDB_future,
    target: PhantomData<T>,
}

unsafe impl<T> Send for FDBFuture<T> {}


impl<T> From<*mut FDB_future> for FDBFuture<T> {
    fn from(value: *mut FDB_future) -> Self {
        FDBFuture {
            future: value,
            target: PhantomData,
        }
    }
}
//
// impl<T> Deref for FDBFuture<T> {
//     type Target = *mut FDB_future;
//     fn deref(&self) -> &Self::Target {
//         &self.future
//     }
// }
//
// impl<T> DerefMut for FDBFuture<T> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.future
//     }
// }


impl<T: FDBResult> Future for FDBFuture<T> {
    type Output = Result<T, Error>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.future;
        let ready = unsafe { fdb_c::fdb_future_is_ready(future) };

        if ready == 0 {
            unsafe extern "C" fn future_ready_callback(_future: *mut FDB_future, callback_parameter: *mut std::os::raw::c_void,
            ) {
                let waker: Box<Waker> = Box::from_raw(callback_parameter.cast());
                waker.wake_by_ref()
            }

            let waker = Box::new(cx.waker().clone());
            let waker_ptr = Box::into_raw(waker).cast();
            unsafe { fdb_c::fdb_future_set_callback(future, Some(future_ready_callback), waker_ptr as *mut _) };
            return Poll::Pending;
        }

        let error = unsafe { fdb_c::fdb_future_get_error(future) };

        if error != 0 {
            error!("{error}");
            return Poll::Ready(Err(FdbErrorCode(error).into()));
        }

        let result = Poll::Ready(T::from_future(future));

        // The memory referenced by the result is owned by the FDBFuture object and will be valid until fdb_future_destroy(future) is called.
        // All the protocol implementing FDBResult must have ownership of their content at this point.
        unsafe { fdb_c::fdb_future_destroy(future) }

        result
    }
}


pub fn from_raw_fdb_slice<T, U: Into<usize>>(ptr: *const T, len: U) -> &'static [T] {
    if ptr.is_null() {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(ptr, len.into()) }
}


impl FDBResult for Empty {
    fn from_future(_future: *mut FDB_future) -> Result<Self, Error> {
        return Ok(Empty::default());
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


impl FDBResult for Int64 {
    fn from_future(future: *mut FDB_future) -> Result<Self, Error> {
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

        Ok(out.into())
    }
}


impl FDBResult for Key {
    fn from_future(future: *mut FDB_future) -> Result<Self, Error> {
        let mut key = ptr::null();
        let mut key_length = i32::MIN;
        let result = unsafe { fdb_c::fdb_future_get_key(future, &mut key, &mut key_length) };

        if result != 0 {
            error!("{result}");
            return Err(FdbErrorCode(result).into());
        }
        // Check that dummy value has been overwritten
        assert_ne!(key_length, i32::MIN);

        let key: Key = from_raw_fdb_slice(key, key_length as usize).to_owned().into();

        assert_eq!(key.len(), key_length as usize);

        Ok(key)
    }
}


impl FDBResult for Value {
    fn from_future(future: *mut FDB_future) -> Result<Self, Error> {
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

        Ok(value.into())
    }
}

#[cfg(any(feature = "730", feature = "710", feature = "700"))]
impl FDBResult for KeyArray {

    fn from_future(future: *mut FDB_future) -> Result<Self, Error> {
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

        let keys: Vec<fdb_c::FDBKey> = from_raw_fdb_slice(keys, key_count as usize).to_owned();
        let keys: Vec<Key> = keys.into_iter().map(|k| from_raw_fdb_slice(k.key, k.key_length as usize).to_owned().into()).collect();

        assert_eq!(keys.len(), key_count as usize);

        Ok(keys.into())
    }
}


impl FDBResult for StringArray {
    fn from_future(future: *mut FDB_future) -> Result<Self, Error> {
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
        let strings: Vec<String> = strings
            .iter()
            .map(|s| {
                unsafe { CStr::from_ptr(*s) }
                    .to_str()
                    .expect("Could not convert C String to String")
                    .to_owned()
            })
            .collect();

        Ok(strings.into())
    }
}


impl FDBResult for KeyValueArray {
    fn from_future(future: *mut FDB_future) -> Result<Self, Error> {
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
        let kvs: Vec<(Key, Value)> = kvs
            .iter()
            .map(|kv| {
                let key = from_raw_fdb_slice(kv.key, kv.key_length as usize).to_owned().into();
                let value =
                    from_raw_fdb_slice(kv.value, kv.value_length as usize).to_owned().into();
                (key, value)
            })
            .collect();

        Ok(kvs.into())
    }
}

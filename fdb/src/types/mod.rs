use std::ops::{Deref, DerefMut};

pub use key::{Key, KeySelector};
pub use value::Value;

mod key;
mod value;

pub struct Empty(());

impl Default for Empty {
    fn default() -> Self {
        Empty(())
    }
}

pub struct Int64(pub i64);

impl From<i64> for Int64 {
    fn from(value: i64) -> Self {
        Int64(value)
    }
}


pub struct KeyArray(Vec<Key>);

impl From<Vec<Key>> for KeyArray {
    fn from(value: Vec<Key>) -> Self {
        KeyArray(value)
    }
}

pub struct StringArray(Vec<String>);

impl From<Vec<String>> for StringArray {
    fn from(value: Vec<String>) -> Self {
        StringArray(value)
    }
}

pub struct KeyValueArray(pub Vec<(Key, Value)>);

impl From<Vec<(Key, Value)>> for KeyValueArray {
    fn from(value: Vec<(Key, Value)>) -> Self {
        KeyValueArray(value)
    }
}

impl Deref for KeyValueArray {
    type Target = Vec<(Key, Value)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


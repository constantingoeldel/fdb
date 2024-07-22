use std::ops::{Deref, DerefMut};


use crate::future::from_raw_fdb_slice;

pub struct Key(Vec<u8>);

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

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        let bytes = value.as_bytes();
        Self(bytes.to_vec())
    }
}

impl From<Vec<u8>> for Key {
    fn from(value: Vec<u8>) -> Self {
        Key(value)
    }
}


pub struct KeySelector {
    // Key the selector starts from
    pub key: Key,
    pub inclusive: bool,
    pub offset: i32,
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
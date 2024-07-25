use crate::types::Key;

pub struct KeyArray(Vec<Key>);

impl From<Vec<Key>> for KeyArray {
    fn from(value: Vec<Key>) -> Self {
        KeyArray(value)
    }
}
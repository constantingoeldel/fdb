use std::ops::Deref;

#[derive(Eq, PartialEq, Debug)]
pub struct Value(Vec<u8>);

impl Deref for Value {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value(value.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}
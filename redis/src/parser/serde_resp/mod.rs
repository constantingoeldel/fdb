use std::fmt::Display;

use serde::{de, ser};
use thiserror::Error;

mod deserializer;
mod serializer;

#[derive(Error, Debug)]
enum Error {
    #[error("Message: {0}")]
    Message(String),

    #[error("Trailing characters")]
    TrailingCharacters,

    #[error("{0}")]
    Parsing(#[from] nom::error::Error<&'static [u8]>),

    #[error("Integer too large to fit in target type. Expected {0}, found {1}")]
    IntegerOutOfRange(i64, i64),

    #[error("There is no concept of a single character in RESP, use a string instead")]
    CharNotSupported,

    #[error("There is no concept of bytes in RESP, use a string instead")]
    BytesNotSupported,

    #[error("String parsing error: {0}")]
    StringParsingError(#[from] std::string::FromUtf8Error),

}


type Result<T> = std::result::Result<T, Error>;

impl de::Error for Error {
    fn custom<T>(msg: T) -> Self where T: Display {
        Error::Message(msg.to_string())
    }
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self where T: Display {
        Error::Message(msg.to_string())
    }
}
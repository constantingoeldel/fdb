use std::fmt::Display;

use serde::{de, ser};
use thiserror::Error;

mod deserializer;
mod serializer;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Message: {0}")]
    Message(String),

    #[error("Trailing characters")]
    TrailingCharacters,

    #[error("Invalid encoding")]
    Parsing(nom::error::Error<Vec<u8>>),

    #[error("Integer too large to fit in target type. Expected {0}, found {1}. Use a larger integer or a string type instead"
    )]
    IntegerOutOfRange(i128, i128),

    #[error("There is no concept of a single character in RESP, use a string instead")]
    CharNotSupported,

    #[error("There is no concept of bytes in RESP, use a string instead")]
    BytesNotSupported,

    #[error("String parsing error: {0}")]
    StrParsingError(#[from] std::str::Utf8Error),

    #[error("String parsing error: {0}")]
    StringParsingError(#[from] std::string::FromUtf8Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

impl<'a> From<nom::error::Error<&'a [u8]>> for Error {
    fn from(e: nom::error::Error<&'a [u8]>) -> Self {
        let input = e.input.to_owned();
        let code = e.code;

        let e = nom::error::Error {
            input,
            code,
        };
        Error::Parsing(e)
    }
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


#[cfg(test)]
mod test {
    use serde::Deserialize;

    use deserializer::from_slice;

    use super::*;

    #[test]
    fn integer() {
        #[derive(Deserialize)]
        struct TestInt(i64);

        let s = b":123\r\n";

        let res: TestInt = from_slice(s).unwrap();
        assert_eq!(res.0, 123);
    }

    #[test]
    fn float() {
        #[derive(Deserialize)]
        struct TestFloat(f64);

        let s = b":123\r\n";

        let res: TestFloat = from_slice(s).unwrap();
        assert_eq!(res.0, 123.0);
    }

    #[test]
    fn string() {
        #[derive(Deserialize)]
        struct TestString(String);

        let s = b"+OK\r\n";

        let res: TestString = from_slice(s).unwrap();
        assert_eq!(res.0, "OK");
    }
    
    
}
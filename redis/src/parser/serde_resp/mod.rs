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
    use std::collections::HashSet;

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


    #[test]
    fn string_from_error() {
        #[derive(Deserialize)]
        struct TestString(String);

        let s = b"-ERR unknown command 'foobar'\r\n";

        let res: TestString = from_slice(s).unwrap();
        assert_eq!(res.0, "ERR unknown command 'foobar'");
    }

    #[test]
    fn string_from_bulk_string() {
        #[derive(Deserialize)]
        struct TestString(String);

        let s = b"$6\r\nfoobar\r\n";

        let res: TestString = from_slice(s).unwrap();
        assert_eq!(res.0, "foobar");
    }

    #[test]
    fn string_from_null_bulk_string() {
        #[derive(Deserialize)]
        struct TestString(());

        let s = b"$-1\r\n";

        let res: TestString = from_slice(s).unwrap();
        assert_eq!(res.0, ());
    }

    #[test]
    fn string_from_bulk_error() {
        #[derive(Deserialize)]
        struct TestString(String);

        let s = b"!21\r\nSYNTAX invalid syntax\r\n";

        let res: TestString = from_slice(s).unwrap();
        assert_eq!(res.0, "SYNTAX invalid syntax");
    }

    #[test]
    fn string_array() {
        #[derive(Deserialize)]
        struct Test(Vec<String>);

        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";

        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.0, vec!["hello", "world"]);
    }

    #[test]
    fn integer_array() {
        #[derive(Deserialize)]
        struct Test(Vec<i64>);

        let s = b"*3\r\n:1\r\n:2\r\n:3\r\n";

        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.0, vec![1, 2, 3]);
    }
    
    #[test]
    fn nested_array() {
        
        #[derive(Deserialize)]
        struct Test(Vec<Vec<i64>>);
        
        let s = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n:4\r\n:5\r\n";
        
        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.0, vec![vec![1, 2, 3], vec![4, 5]]);
        
    }
    
    

    #[test]
    fn bulk_string_array() {
        #[derive(Deserialize)]
        struct Test(Vec<String>);

        let s = b"*2\r\n$6\r\nfoobar\r\n$3\r\nbaz\r\n";

        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.0, vec!["foobar", "baz"]);
    }

    #[test]
    fn string_set() {
        #[derive(Deserialize)]
        struct Test(HashSet<String>);

        let s = b"~2\r\n+first\r\n+second\r\n";

        let res: Test = from_slice(s).unwrap();
        assert!(res.0.contains("first"));
        assert!(res.0.contains("second"));
        assert!(!res.0.contains("third"));
    }
    
    #[test]
    fn integer_set() {
        
        #[derive(Deserialize)]
        struct Test(HashSet<i64>);
        
        let s = b"~2\r\n:1\r\n:2\r\n";
        
        let res: Test = from_slice(s).unwrap();
        assert!(res.0.contains(&1));
        assert!(res.0.contains(&2));
        assert!(!res.0.contains(&3));
    }
    
    #[test]
    fn string_push() {
        #[derive(Deserialize)]
        struct Test(Vec<String>);
        
        let s = b">2\r\n+first\r\n+second\r\n";
        
        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.0, vec!["first", "second"]);
    }


    #[test]
    fn boolean() {
        #[derive(Deserialize)]
        struct TestBool(bool);

        let s = b"#t\r\n";

        let res: TestBool = from_slice(s).unwrap();
        assert_eq!(res.0, true);
    }

    // #[test]
    // fn big_integer() {
    //     #[derive(Deserialize)]
    //     struct TestInt(BigNumber);
    //
    //     let fitting_biting = b"(13\r\n";
    //
    //     let res: TestInt = from_slice(fitting_biting).unwrap();
    //
    //     assert_eq!(res.0, BigNumber::from(BigInt::from(13)));
    //
    //     let oversized_bigint = b"(-3492890328409238509324850943850943825024385\r\n";
    //
    //     let res: Result<TestInt> = from_slice(oversized_bigint);
    //
    //     assert_eq!(res.is_err(), true);
    // }
}
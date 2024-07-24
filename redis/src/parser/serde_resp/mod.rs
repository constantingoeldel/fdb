use std::fmt::Display;

use serde::{de, ser};
use thiserror::Error;

pub use deserializer::from_slice;

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

    #[error("Integer too large to fit in target type. Expected a number between {0} and {1}, but found {2}. Use a larger integer or a string type instead"
    )]
    IntegerOutOfRange(i128, i128, i128),

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

    #[error("Invalid Command {0}")]
    InvalidCommand(String),

    #[error("Unit struct name did not match, expected {0}, got {1}")]
    UnitStructNameMismatch(String, String),
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
    use std::collections::{HashMap, HashSet};

    use serde::Deserialize;

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
    fn string_from_bulk_error() {
        #[derive(Deserialize)]
        struct TestString(String);

        let s = b"!21\r\nSYNTAX invalid syntax\r\n";

        let res: TestString = from_slice(s).unwrap();
        assert_eq!(res.0, "SYNTAX invalid syntax");
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
    fn string_from_null() {
        #[derive(Deserialize)]
        struct Test(());

        let s = b"_\r\n";

        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.0, ());
    }

    #[test]
    fn string_from_null_array() {
        #[derive(Deserialize)]
        struct Test(());

        let s = b"*-1\r\n";

        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.0, ());
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

    #[test]
    fn map() {
        #[derive(Deserialize)]
        struct TestMap(HashMap<String, i64>);

        let s = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";

        let res: TestMap = from_slice(s).unwrap();
        assert_eq!(res.0.get("first"), Some(&1));
        assert_eq!(res.0.get("second"), Some(&2));
        assert_eq!(res.0.get("third"), None);
    }

    #[test]
    fn small_int() {
        #[derive(Deserialize)]
        struct TestInt(i8);

        let s = b":13\r\n";

        let res: TestInt = from_slice(s).unwrap();
        assert_eq!(res.0, 13);
    }

    #[test]
    fn small_uint() {
        #[derive(Deserialize)]
        struct TestInt(u8);

        let s = b":13\r\n";

        let res: TestInt = from_slice(s).unwrap();
        assert_eq!(res.0, 13);
    }

    #[test]
    fn small_int_out_of_range() {
        #[derive(Deserialize, Debug)]
        struct TestInt(i8);

        let s = b":128\r\n";

        let res: Result<TestInt> = from_slice(s);
        assert!(res.is_err());
    }

    #[test]
    fn small_uint_out_of_range() {
        #[derive(Deserialize, Debug)]
        struct TestInt(u8);

        let s = b":-5\r\n";

        let res: Result<TestInt> = from_slice(s);
        assert!(res.is_err());
        dbg!(res);
    }

    #[test]
    fn big_integer() {
        // TODO: utilize the BigInt type
        // + better error message when overflowing the target type
        // Currently, the error stems from parse_digits in integer.rs:
        //fn parse(i: &str) -> IResult<&str, i64> {
        //    map_res(digit1, str::parse)(i)
        // }
        let fitting_bigint = b"(-1000\r\n";
        let oversized_bigint = b"(-3492890328409238509324850943850943825024385\r\n";

        #[derive(Deserialize, Debug)]
        struct TestInt(i64);

        let res: TestInt = from_slice(fitting_bigint).unwrap();
        assert_eq!(res.0, -1000);
        let res: Result<TestInt> = from_slice(oversized_bigint);
        assert!(res.is_err());

        #[derive(Deserialize)]
        struct TestString(String);

        let res: TestString = from_slice(fitting_bigint).unwrap();
        assert_eq!(res.0, "-1000");
        let res: TestString = from_slice(oversized_bigint).unwrap();
        assert_eq!("-3492890328409238509324850943850943825024385", res.0);
    }

    #[test]
    fn test_simple_struct() {
        #[derive(Deserialize)]
        struct Set {
            key: String,
            value: String,
        }

        // Array of 2 elements
        let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let res: Set = from_slice(s).unwrap();

        assert_eq!(res.key, "hello");
        assert_eq!(res.value, "world");
    }

    #[test]
    fn test_unit_enum() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum TestEnum {
            Hello,
            Get,
        }

        let s = b"$5\r\nHello\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Hello);

        let s = b"$3\r\nGet\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Get);
    }

    #[test]
    fn test_unit_enum_renamed() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        #[serde(rename_all = "lowercase")]
        enum TestEnum {
            Hello,
            Get,
        }

        let s = b"$5\r\nhello\r\n";
        let res: TestEnum = from_slice(s).unwrap();

        assert_eq!(res, TestEnum::Hello);

        let s = b"$3\r\nget\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Get);

        let s = b"$3\r\nGET\r\n";
        let res: Result<TestEnum> = from_slice(s);
        assert!(res.is_err());
    }

    #[test]
    fn test_unit_enum_all_spellings() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum TestEnum {
            #[serde(alias = "HELLO")]
            #[serde(alias = "hello")]
            Hello,
            Get,
        }

        let a = b"$5\r\nhello\r\n";
        let b = b"$5\r\nHELLO\r\n";
        let c = b"$5\r\nHello\r\n";

        let res: TestEnum = from_slice(a).unwrap();
        assert_eq!(res, TestEnum::Hello);

        let res: TestEnum = from_slice(b).unwrap();
        assert_eq!(res, TestEnum::Hello);

        let res: TestEnum = from_slice(c).unwrap();
        assert_eq!(res, TestEnum::Hello);
    }

    #[test]
    fn test_wrapper_enum() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum TestEnum {
            Hello(String),
            Get(String),
        }

        let s = b"$5\r\nHello\r\n$5\r\nworld\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Hello("world".to_string()));

        let s = b"$3\r\nGet\r\n$3\r\nout\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Get("out".to_string()));
    }

    #[test]
    fn test_different_wrapper_enum() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum TestEnum {
            Hello(String),
            Get(i64),
        }

        let s = b"$5\r\nHello\r\n$5\r\nworld\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Hello("world".to_string()));

        let s = b"$3\r\nGet\r\n:123\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Get(123));
    }


    #[test]
    fn test_tuple_enum() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum TestEnum {
            Hello(String, String),
            Get(String, String),
        }

        let s = b"$5\r\nHello\r\n$5\r\nHello\r\n$5\r\nworld\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Hello("Hello".to_string(), "world".to_string()));


        let s = b"$3\r\nGet\r\n$3\r\nout\r\n$3\r\nnow\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Get("out".to_string(), "now".to_string()));
    }

    #[test]
    fn test_struct_enum() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum TestEnum {
            Hello { key: String, value: String },
            Get { key: String, value: String },
        }

        let s = b"$5\r\nHello\r\n$5\r\nHello\r\n$5\r\nworld\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Hello { key: "Hello".to_string(), value: "world".to_string() });

        let s = b"$3\r\nGet\r\n$3\r\nout\r\n$3\r\nnow\r\n";
        let res: TestEnum = from_slice(s).unwrap();
        assert_eq!(res, TestEnum::Get { key: "out".to_string(), value: "now".to_string() });
    }

    #[test]
    fn enum_in_struct() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum OnorOff {
            On,
            Off,
        }

        #[derive(Deserialize, Debug, Eq, PartialEq)]
        struct Test {
            key: String,
            value: OnorOff,
        }

        let s = b"*2\r\n$5\r\nHello\r\n$2\r\nOn\r\n";
        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.key, "Hello");
        assert_eq!(res.value, OnorOff::On);
    }

    #[test]
    fn optional_enum_in_struct() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum OnorOff {
            On,
            Off,
        }

        #[derive(Deserialize, Debug, Eq, PartialEq)]
        struct Test {
            key: String,
            #[serde(default)]
            value: Option<OnorOff>,
        }

        let s = b"*2\r\n$5\r\nHello\r\n$2\r\nOn\r\n";
        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.key, "Hello");
        assert_eq!(res.value, Some(OnorOff::On));

        let s = b"*2\r\n$5\r\nHello\r\n_\r\n";
        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.key, "Hello");
        assert_eq!(res.value, None);

        let s = b"*1\r\n$5\r\nHello\r\n";
        let res: Test = from_slice(s).unwrap();
        assert_eq!(res.key, "Hello");
        assert_eq!(res.value, None);
    }

    #[test]
    fn test_unit_struct() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        struct Test;

        let s = b"$4\r\nTest\r\n";
        let res: Test = from_slice(s).unwrap();
        assert_eq!(res, Test);

        let s = b"$4\r\nTEST\r\n";
        let res: Result<Test> = from_slice(s);
        assert!(res.is_err());
    }

    #[test]
    fn test_option_explicit_none() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        struct Key(Option<String>);

        let s = b"$5\r\nHello\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res.0, Some("Hello".to_string()));

        let s = b"$-1\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res.0, None);
    }

    #[test]
    fn test_option_implicit_none() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        struct Key(String, Option<String>);

        let s = b"$5\r\nHello\r\n$5\r\nWorld\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res.0, "Hello".to_string());
        assert_eq!(res.1, Some("World".to_string()));

        let s = b"$5\r\nHello\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res.0, "Hello".to_string());
        assert_eq!(res.1, None);
    }


    #[test]
    fn test_option_none_enum() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum Key {
            Hello(Option<String>),
            Get(Option<String>),
        }

        let s = b"$5\r\nHello\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res, Key::Hello(None));

        let s = b"$3\r\nGet\r\n$5\r\nWorld\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res, Key::Get(Some("World".to_string())));
    }

    #[test]
    fn test_tuple_struct() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        struct Key(String, i64);

        let s = b"$5\r\nHello\r\n:123\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res, Key("Hello".to_string(), 123));
    }

    #[test]
    fn test_tuple() {
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        struct Key {
            a: (String, i64),
        }

        let s = b"*2\r\n$5\r\nHello\r\n:123\r\n";
        let res: Key = from_slice(s).unwrap();
        assert_eq!(res.a, ("Hello".to_string(), 123));
    }

    // TODO: How do access the deserialize_ignored_any_path?
    // #[test]
    // fn test_ignored() {
    //     #[derive(Deserialize, Debug, Eq, PartialEq)]
    //     struct Key {
    //         a: String,
    //         #[serde()]
    //         b: i64,
    //     }
    //
    //     let s = b"*2\r\n$5\r\nHello\r\n:123\r\n";
    //     let res: Key = from_slice(s).unwrap();
    //     assert_eq!(res.a, "Hello");
    //     assert_eq!(res.b, 0);
    // }
}
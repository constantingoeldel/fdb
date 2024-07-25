use std::fmt::Formatter;

use serde::{Deserialize, Deserializer};
use serde::de::{Error, SeqAccess, Visitor};

use macro_derive::DeserializeUntagged;

use crate::parser::from_slice;

/// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
/// EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
///
/// NX -- Only set the key if it does not already exist.
///
/// XX -- Only set the key if it already exists.
///
/// GET -- Return the old string stored at key, or nil if key did not exist.
/// An error is returned and SET aborted if the value stored at key is not a string.
///
/// EX seconds -- Set the specified expire time, in seconds (a positive integer).
///
/// PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
///
/// EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds (a positive integer).
///
/// KEEPTTL -- Retain the time to live associated with the key.

#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct Set {
    cmd: SET,
    pub key: String,
    pub value: String,

    #[serde(default)]
    pub options: Options,
}

// options! { Expiry, Existence, GET }
#[derive(Debug, PartialEq, Eq, Default)]
struct Options {
    expiry: Option<Expiry>,
    existence: Option<Existence>,
    get: Option<GET>,
}


impl<'de> Deserialize<'de> for Options {
    fn deserialize<D>(mut deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        #[derive(DeserializeUntagged, Debug, Eq, PartialEq)]
        enum OptionsEnum {
            Expiry(Expiry),
            Existence(Existence),
            GET(GET),
        }

        struct OptionsVisitor;

        impl<'de> Visitor<'de> for OptionsVisitor {
            type Value = Options;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("One or more variants of the enum")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error> where A: SeqAccess<'de> {
                let mut options = Options {
                    expiry: None,
                    existence: None,
                    get: None,
                };


                for _ in 0..3 {
                    let res = seq.next_element();
                    dbg!(&res);

                    if let Ok(Some(option)) = res {
                        match option {
                            OptionsEnum::Expiry(expiry) => options.expiry = Some(expiry),
                            OptionsEnum::Existence(existence) => options.existence = Some(existence),
                            OptionsEnum::GET(get) => options.get = Some(get),
                        }
                    }
                }

                Ok(options)
            }
        }

        deserializer.deserialize_seq(OptionsVisitor)
    }
}


#[derive(Deserialize, Debug, Eq, PartialEq)]
struct SET;



#[derive(Deserialize, Debug, Eq, PartialEq)]
struct GET;

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum Existence {
    NX,
    XX,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum Expiry {
    EX(u32),
    PX(u32),
    EXAT(u32),
    KEEPTTL,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct Extend(String);

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct EX;

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct KeepTimeToLive(KEEPTTL);

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct KEEPTTL;

#[cfg(test)]
mod test {
    use std::fmt::Formatter;

    use nom::Offset;
    use serde::__private::de::{Content, ContentRefDeserializer};
    use serde::de::{EnumAccess, Error, SeqAccess, VariantAccess, Visitor};
    use serde::Deserializer;

    use crate::parser::Commands;
    use crate::parser::from_slice;

    use super::*;

    #[test]
    fn test_basic_set() {
        let s = b"*3\r\n$3\r\nSet\r\n$5\r\nhello\r\n$5\r\nworld\r\n";

        let res: Set = from_slice(s).unwrap();
        assert_eq!(res.cmd, SET);
        let res: Commands = from_slice(s).unwrap();

        // assert_eq!(res, Commands::Set(Set { cmd: SET::SET, key: "hello".to_string(), value: "world".to_string(), options: None }));
    }

    #[test]
    fn test_set_with_existence_option() {
        let s = b"*4\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nNX\r\n";
        let c = Commands::Set(Set { cmd: SET, key: "hello".to_string(), value: "world".to_string(), options: Options { existence: Some(Existence::NX), expiry: None, get: None } });
        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, c);

        let s = b"*4\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nXX\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { cmd: SET, key: "hello".to_string(), value: "world".to_string(), options: Options { existence: Some(Existence::XX), expiry: None, get: None } }));
    }

    #[test]
    fn test_option_explicit_macro() {
        #[derive(Debug, Eq, PartialEq)]
        enum Options {
            Expiry(Expiry),
            Existence(Existence),
        }

        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum Existence {
            XX,
            NX,
        }

        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum Expiry {
            EX(String),
            KEEPTTL,

        }

        impl<'de> Deserialize<'de> for Options {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
                // See https://stackoverflow.com/questions/75181286/how-to-implement-a-custom-deserializer-using-serde-that-allows-for-parsing-of-un/78793511#78793511
                struct NonSelfDescribingUntaggedEnumVisitor;

                impl<'de> Visitor<'de> for NonSelfDescribingUntaggedEnumVisitor {
                    type Value = Options;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str("One of the variants of the enum")
                    }

                    fn visit_byte_buf<E>(self, mut v: Vec<u8>) -> Result<Self::Value, E> where E: Error {
                        let expiry: Result<Expiry, crate::parser::ParseError> = from_slice(&v);
                        if let Ok(res) = expiry {
                            // v.drain(0..res.len());
                            return Ok(Options::Expiry(res));
                        }

                        let existence: Result<Existence, crate::parser::ParseError> = from_slice(&v);
                        if let Ok(res) = existence {
                            return Ok(Options::Existence(res));
                        }

                        let exp_err = expiry.unwrap_err();
                        let exi_err = existence.unwrap_err();
                        Err(serde::de::Error::custom(format!("No fitting option found. \nError for Expiry was: {}\nError for Existence was: {}", exp_err, exi_err)))
                    }
                }

                deserializer.deserialize_byte_buf(NonSelfDescribingUntaggedEnumVisitor)

                
            }
        }

        let s = b"$2\r\nNX\r\n";
        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::Existence(Existence::NX));


        let s = b"$7\r\nKEEPTTL\r\n";
        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::Expiry(Expiry::KEEPTTL));


        let s = b"$2\r\nEX\r\n$4\r\ntest\r\n";
        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::Expiry(Expiry::EX(String::from("test"))));
    }

    #[test]
    fn test_options() {
        #[derive(DeserializeUntagged, Debug, Eq, PartialEq)]
        enum Options {
            Expiry(Expiry),
            Existence(Existence),
            GET(GET),
        }

        let s = b"$2\r\nNX\r\n";

        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::Existence(Existence::NX));

        let s = b"$3\r\nGET\r\n";

        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::GET(GET));

        let s = b"$3\r\nget\r\n";

        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::GET(GET));

        let s = b"$7\r\nKEEPTTL\r\n";
        let res: KEEPTTL = from_slice(s).unwrap();
        assert_eq!(res, KEEPTTL);
        let res: Expiry = from_slice(s).unwrap();
        let res: Options = from_slice(s).unwrap();


        let s = b"$2\r\nEX\r\n$3\r\n123\r\n";
        let res: Expiry = from_slice(s).unwrap();
        let res: Options = from_slice(s).unwrap();
    }

    #[test]
    fn test_set_with_get() {
        let s = b"*4\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nGET\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { cmd: SET, key: "hello".to_string(), value: "world".to_string(), options: Options { existence: None, get: Some(GET), expiry: None } }));
    }

    #[test]
    fn test_with_expire() {
        let s = b"*5\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nEX\r\n$3\r\n123\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { cmd: SET, key: "hello".to_string(), value: "world".to_string(), options: Options { existence: None, get: None, expiry: Some(Expiry::EX(123)) } }));
    }

    #[test]
    fn test_with_all_options() {
        let s = b"*7\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nNX\r\n$3\r\nGET\r\n$2\r\nPX\r\n$6\r\n123444\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { cmd: SET, key: "hello".to_string(), value: "world".to_string(), options: Options { existence: Some(Existence::NX), get: Some(GET), expiry: Some(Expiry::EX(123)) } }));
    }
}



use serde::Deserialize;

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
    pub key: String,
    pub value: String,

    #[serde(default)]
    pub options: Option<Options>,
}


#[derive(Deserialize, Debug, Eq, PartialEq)]
#[serde(untagged)]
enum Options {
    Expiry(Expiry),
    Existence(NXorXX),
    GET(GET),
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
enum GET {
    GET,
    Get,
    get,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum NXorXX {
    NX,
    XX,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub enum Expiry {
    EX(i64),
    PX(u64),
    EXAT(u64),
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
    use std::rc::Rc;

    use serde::de::{EnumAccess, Error, SeqAccess, VariantAccess};
    use serde::Deserializer;

    use crate::parser::Commands;
    use crate::parser::from_slice;

    use super::*;

    #[test]
    fn test_basic_set() {
        let s = b"*3\r\n$3\r\nSet\r\n$5\r\nhello\r\n$5\r\nworld\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { key: "hello".to_string(), value: "world".to_string(), options: None }));
    }

    #[test]
    fn test_set_with_existence_option() {
        let s = b"*4\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nNX\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { key: "hello".to_string(), value: "world".to_string(), options: Some(Options::Existence(NXorXX::NX)) }));

        let s = b"*4\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nXX\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { key: "hello".to_string(), value: "world".to_string(), options: Some(Options::Existence(NXorXX::XX)) }));
    }

    #[test]
    fn test_option_with_param() {
        #[derive(Debug, Eq, PartialEq)]
        enum Options {
            Expiry(Expiry),
            // Existence(Existence),
        }

        impl<'de> Deserialize<'de> for Options {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
                let deserializer = Rc::new(deserializer);
                let des = deserializer.clone();

                let expiry = Expiry::deserialize();
                if let Ok(res) = expiry {
                    return Ok(Options::Expiry(res));
                }


                // let existence = Existence::deserialize(__deserializer);
                // if let Ok(res) = existence {
                //     return Ok(Options::Existence(res));
                // }

                let exp_err = expiry.unwrap_err();
                // let exist_err = existence.unwrap_err();
                Err(serde::de::Error::custom(format!("No fitting option found. \nError for Expiry was: {}\nError for Existence was: {}", exp_err, exp_err)))
            }
        }

        // #[derive(Deserialize, Debug, Eq, PartialEq)]
        // enum Existence {
        //     XX,
        //     NX,
        // }
        //
        #[derive(Deserialize, Debug, Eq, PartialEq)]
        enum Expiry {
            EX(String),

        }
        //
        // let s = b"$2\r\nNX\r\n";
        // let res: Options = from_slice(s).unwrap();
        // assert_eq!(res, Options::Existence(Existence::NX));
        //

        // let s = b"$7\r\nKEEPTTL\r\n";
        // let res: Options = from_slice(s).unwrap();
        // assert_eq!(res, Options::Expiry(Expiry::KEEPTTL));
        //

        let s = b"$2\r\nEX\r\n$4\r\ntest\r\n";
        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::Expiry(Expiry::EX(String::from("test"))));
    }

    #[test]
    fn test_options() {
        let s = b"$2\r\nNX\r\n";

        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::Existence(NXorXX::NX));

        let s = b"$3\r\nGET\r\n";

        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::GET(GET::GET));

        let s = b"$3\r\nget\r\n";

        let res: Options = from_slice(s).unwrap();
        assert_eq!(res, Options::GET(GET::get));

        let s = b"$7\r\nKEEPTTL\r\n";
        let res: KEEPTTL = from_slice(s).unwrap();
        assert_eq!(res, KEEPTTL);
        let res: Expiry = from_slice(s).unwrap();
        let res: Options = from_slice(s).unwrap();


        let s = b"$2\r\nEX\r\n:1234\r\n";
        let res: Expiry = from_slice(s).unwrap();
        let res: Options = from_slice(s).unwrap();


        // let res: Options = from_slice(s).unwrap();
        // assert_eq!(res, Options::Expiry(Expiry::EX(String::from("1234"))));


        // let s = b"$2\r\nEX\r\n$2\r\nEX\r\n";
        // let res: Options = from_slice(s).unwrap();
        // assert_eq!(res, Options::Expiry(Expiry::EX(EX, String::from("EX"))));
    }

    #[test]
    fn test_set_with_get() {
        let s = b"*4\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$3\r\nGET\r\n";

        let res: Commands = from_slice(s).unwrap();
        assert_eq!(res, Commands::Set(Set { key: "hello".to_string(), value: "world".to_string(), options: Some(Options::GET(GET::GET)) }));
    }

    #[test]
    fn test_with_expire() {
        // let s = b"*5\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n$2\r\nEX\r\n$3\r\n123\r\n";
        //
        // let res: Commands = from_slice(s).unwrap();
        // assert_eq!(res, Commands::Set(Set { key: "hello".to_string(), value: "world".to_string(), options: Some(Options::Expiry(Expiry::EX(String::from("123")))) }));
    }
}

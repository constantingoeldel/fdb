use serde::{Deserialize, Deserializer};
use serde::de::{Error, SeqAccess, Visitor};

use macro_derive::*;

use crate::parser::commands::Command;

use super::{CError, CResult, Response};

#[derive(Deserialize, Debug, Eq, PartialEq)]
pub struct Hello {
    cmd: HELLO,
    #[serde(default)]
    options: Options
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct HELLO;

// TODO: Eigentlich sind AUTH und SETNAME conditional on protover >= 2
// Wie checken?
#[derive(Options, Debug, Eq, PartialEq, Default)]
struct Options {
    protover: Option<u8>,
    auth: Option<Auth>,
    setname: Option<SetClientName>,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct Auth {
    cmd: AUTH,
    username: String,
    password: String,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct AUTH;

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct SetClientName {
    cmd: SETNAME,
    clientname: String,
}

#[derive(Deserialize, Debug, Eq, PartialEq)]
struct SETNAME;


#[cfg(test)]
mod tests {
    use crate::parser::from_slice;

    use super::*;

    #[test]
    fn test_hello() {
        let s = b"*7\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$1\r\nc\r\n$1\r\ng\r\n$7\r\nSETNAME\r\n$4\r\ntest\r\n";

        let res: Hello = from_slice(s).unwrap();
        assert_eq!(res, Hello {
            cmd: HELLO,
            options: Options {
                protover: Some(3),
                auth: Some(Auth {
                    cmd: AUTH,
                    username: "c".to_string(),
                    password: "g".to_string(),
                }),
                setname: Some(SetClientName {
                    cmd: SETNAME,
                    clientname: "test".to_string(),
                }),
            },
        });
    }
}

impl Command for Hello {
    // fn exec(self) -> impl Into<Response> {
    //
    //     todo!("Answer with list/map of server properties")
    // }
    fn check_integrity(&self) -> CResult<()> {
        if !(self.options.protover.is_none() || self.options.protover == Some(3) || self.options.protover == Some(2)) {
            return Err(CError::InvalidProtocolVersion);
        }
        Ok(())
    }
}
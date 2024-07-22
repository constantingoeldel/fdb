use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::opt;
use nom::{Finish, IResult};
use nom::sequence::tuple;

use crate::parser::bulk_string::bulk_string;
use crate::parser::integer::{integer, Integer};
use crate::parser::simple_string::simple_string;
use crate::parser::verbatim_string::verbatim_string;

use super::TryParse;

fn string(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((simple_string, bulk_string, verbatim_string))(i)
}

struct AuthUsernamePassword {
    username: String,
    password: String,
}

struct SetClientName {
    name: String,
}

pub struct ClientHandshake {
    pub protocol_version: i64,
    pub auth: Option<AuthUsernamePassword>,
    pub setname: Option<SetClientName>,
}

impl<'a> TryParse<'a> for ClientHandshake {
    type Output = Self;

    fn try_parse(value: &'a[u8]) -> Result<(&'a[u8], Self::Output), nom::error::Error<&'a[u8]>> {
        let (i, ch) = client_handshake(value).finish()?;
        Ok((i, ch))
    }
}

fn client_handshake(i: &[u8]) -> IResult<&[u8], ClientHandshake> {
    fn hello(i: &[u8]) -> IResult<&[u8], &[u8]> {
        let (i, str) = (string)(i)?;
        let (j, cmd) = tag("HELLO")(str)?;
        assert!(j.is_empty());
        Ok((i, cmd))
    }

    fn auth(i: &[u8]) -> IResult<&[u8], &[u8]> {
        let (i, str) = (string)(i)?;
        let (j, cmd) = tag("AUTH")(str)?;
        assert!(j.is_empty());
        Ok((i, cmd))
    }

    fn protocol_version(i: &[u8]) -> IResult<&[u8], &[u8]> {
        integer(i)
    }

    fn auth_username_password(i: &[u8]) -> IResult<&[u8], AuthUsernamePassword> {
        let (i, (_, username, password)) = tuple((auth, string, string))(i)?;

        Ok((i, AuthUsernamePassword {
            username: String::from_utf8(Vec::from(username)).unwrap(),
            password: String::from_utf8(Vec::from(password)).unwrap(),
        }))
    }

    fn setname(i: &[u8]) -> IResult<&[u8], &[u8]> {
        let (i, str) = (string)(i)?;
        let (j, cmd) = tag("SETNAME")(str)?;
        assert!(j.is_empty());
        Ok((i, cmd))
    }

    fn set_client_name(i: &[u8]) -> IResult<&[u8], SetClientName> {
        let (i, (_, client)) = tuple((setname, string))(i)?;

        Ok((i, SetClientName {
            name: String::from_utf8(Vec::from(client)).unwrap(),
        }))
    }

    let (i, (_, options)) = tuple((hello, opt(tuple((protocol_version, opt(auth_username_password), opt(set_client_name))))))(i)?;


    let mut ch = ClientHandshake {
        protocol_version: 2,
        auth: None,
        setname: None,
    };

    if let Some((protocol_version, auth, setname)) = options {
        let (_, protocol_version) = Integer::try_parse(protocol_version).unwrap();
        let protocol_version : i64 = protocol_version.into();
        assert!((2..=3).contains(&protocol_version));
        ch.protocol_version = protocol_version;
        
        if let Some(auth) = auth {
            ch.auth = Some(auth);
        }
        
        if let Some(setname) = setname {
            ch.setname = Some(setname);
        }
    }

    Ok((i, ch))
}

#[test]
fn test_client_handshake() {
}
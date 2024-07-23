use nom::{Finish, IResult};
use nom::bytes::complete::tag;
use nom::combinator::opt;
use nom::sequence::tuple;
use serde::{Deserialize, Serialize};

use crate::parser::protocol::{integer, string, TryParse};
use crate::parser::protocol::integer::Integer;

#[derive(Deserialize, Serialize, Debug)]
pub struct Hello {
    pub options: Option<ClientHandshakeOptions>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ClientHandshakeOptions {
    pub protocol_version: i64,
    pub auth: Option<AuthUsernamePassword>,
    pub setname: Option<SetClientName>,
}

#[derive(Deserialize, Serialize, Debug)]
struct AuthUsernamePassword {
    username: String,
    password: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct SetClientName {
    clientname: String,
}


impl<'a> TryParse<'a> for Hello {
    type Output = Self;

    fn try_parse(value: &'a[u8]) -> Result<(&'a[u8], Self::Output), nom::error::Error<&'a[u8]>> {
        let (i, ch) = client_handshake(value).finish()?;
        Ok((i, ch))
    }
}


fn client_handshake(i: &[u8]) -> IResult<&[u8], Hello> {
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
            clientname: String::from_utf8(Vec::from(client)).unwrap(),
        }))
    }

    let (i, (_, options)) = tuple((hello, opt(tuple((protocol_version, opt(auth_username_password), opt(set_client_name))))))(i)?;


    let mut ch = ClientHandshakeOptions {
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

    let handshake = Hello {
        options: Some(ch),
    };

    Ok((i, handshake))
}

#[test]
fn test_client_handshake() {
}
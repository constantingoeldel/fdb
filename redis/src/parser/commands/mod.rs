use nom::bytes::complete::tag;
use nom::IResult;

use crate::parser::commands::hello::ClientHandshake;
use crate::parser::protocol::string;

mod get;
mod hello;
mod set;
mod getdel;


enum Commands {
    Get(Get),
    Set(Set),
    GetDel(GetDel),
    Hello(ClientHandshake),
}


(cmd: &str) -> Fn(&[u8]) -> IResult<&[u8], &[u8]> {
    let f = |i| -> IResult<&[u8], &[u8]> {
        let (i, str) = (string)(i)?;
        let (j, cmd) = tag(cmd)(str)?;
        assert!(j.is_empty());
        Ok((i, cmd))
    };
    f
}




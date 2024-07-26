use thiserror::Error;

use get::Get;
use hello::Hello;
use macro_derive::DeserializeUntagged;
use set::Set;

mod get;
mod hello;
mod set;
mod getdel;
mod xadd;
mod test;
mod command;
mod auth;
mod bloom_filter;
mod bgwriteaof;
mod bgsave;
mod bitcount;
mod bitfield;
mod bitfield_ro;
mod bitop;
mod bitpos;
mod blmove;
mod blmpop;
mod blpop;
mod bit;
mod blocking;
mod cuckoo;
mod clientcaching;
mod clientgetname;
mod client;
mod cms;


#[derive(DeserializeUntagged, Debug, Eq, PartialEq)]
pub enum Commands {
    Get(Get),
    Set(Set),
    // GetDel(GetDel),
    Hello(Hello),
    Command(command::Command)
}

pub struct Response;

pub type CResult<T> = std::result::Result<T, CError>;

pub trait Command: Sized {
    fn exec(self) -> impl Into<Response> {
        Response
    }

    fn check_integrity(&self) -> CResult<()> {
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum CError {
    #[error("Requested Protocol Version does not exist")]
    InvalidProtocolVersion,
}

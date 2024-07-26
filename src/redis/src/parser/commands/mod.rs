use serde::Deserialize;

use macro_derive::DeserializeUntagged;

use crate::parser::commands::get::Get;
use crate::parser::commands::set::Set;

mod get;
mod hello;
mod set;
mod getdel;
mod xadd;
mod test;


#[derive(DeserializeUntagged, Debug, Eq, PartialEq)]
pub enum Commands {
    Get(Get),
    Set(Set),
    // GetDel(GetDel),
    // Hello(Hello),
}






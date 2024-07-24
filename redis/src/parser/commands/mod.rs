use serde::Deserialize;

use crate::parser::commands::get::Get;
use crate::parser::commands::set::Set;

mod get;
mod hello;
mod set;
mod getdel;
mod xadd;


#[derive(Deserialize, Debug, Eq, PartialEq)]
#[serde(tag = "untagged")]
pub enum Commands {
    #[serde(alias = "GET")]
    #[serde(alias = "get")]
    Get(Get),
    #[serde(alias = "SET")]
    #[serde(alias = "set")]
    Set(Set),
    // GetDel(GetDel),
    // Hello(Hello),
}






use crate::parser::commands::get::Get;
use crate::parser::commands::getdel::GetDel;
use crate::parser::commands::hello::Hello;
use crate::parser::commands::set::Set;

mod get;
mod hello;
mod set;
mod getdel;
mod xadd;


enum Commands {
    Get(Get),
    Set(Set),
    GetDel(GetDel),
    Hello(Hello),
}







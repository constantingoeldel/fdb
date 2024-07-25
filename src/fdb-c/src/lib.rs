#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn set_api_version() {
        unsafe {
            let r = fdb_select_api_version_impl(710, 710);

            assert_eq!(r, 0)

        }
    }

    #[test]
    fn get_max_api_version() {
        unsafe {
            assert_eq!(fdb_get_max_api_version(), 710)
        }
    }


}
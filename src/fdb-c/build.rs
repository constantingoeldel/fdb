use std::path::PathBuf;
use std::env;
use std::fs;

cfg_if::cfg_if! {
    if #[cfg(feature = "730")] {
const FDB_VERSION : i32 = 730;
    } else if #[cfg(feature = "710")] {
const FDB_VERSION : i32 = 710;
    } else if #[cfg(feature = "700")] {
const FDB_VERSION : i32 = 700;
    } else if #[cfg(feature = "630")] {
const FDB_VERSION : i32 = 630;
    } else if #[cfg(feature = "620")] {
const FDB_VERSION : i32 = 620;
    } else if #[cfg(feature = "610")] {
const FDB_VERSION : i32 = 610;
    } else if #[cfg(feature = "600")] {
const FDB_VERSION : i32 = 600;
    } else if #[cfg(feature = "520")] {
const FDB_VERSION : i32 = 520;
    } else if #[cfg(feature = "510")] {
const FDB_VERSION : i32 = 510;
    } else {
        // No version selected, fails
        const FDB_VERSION : i32 = 0;
    }
}


fn main() {

    if FDB_VERSION == 0 {
        panic!("You must select a version feature for foundation_db")
    }

    println!("cargo:rustc-link-search=native={}", "/nix/store/i9nfm95bakqnlcgxk1d0bvd3sz751539-foundationdb-7.1.32-lib/lib");

    println!("cargo:rustc-link-lib=fdb_c");


    let include_path = format!("-I./include/{}", FDB_VERSION);

    let mut wrapper = String::new();
    wrapper.push_str(&format!("#define FDB_API_VERSION {}\n", FDB_VERSION));
    wrapper.push_str("#include <fdb_c.h>\n");

    fs::write("./wrapper.h", &wrapper).expect("Could not write wrapper file");



    let bindings = bindgen::Builder::default()

        .clang_arg(include_path).header("wrapper.h").parse_callbacks(Box::new(bindgen::CargoCallbacks::new())).generate().expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    bindings.write_to_file(out_path.join("bindings.rs")).expect("Could not write bindings!")

}
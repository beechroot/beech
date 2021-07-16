
use std::env;
use std::path::PathBuf;

fn main() {
    cc::Build::new()
        .file("src/c/shim.c")
        .include(".")
        .static_flag(true)
        .pic(true)
        .compile("shim");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        .whitelist_type("sqlite3_module")
        .whitelist_type("sqlite3_api_routines")
        .whitelist_type("sqlite3_value.*")
        .whitelist_var("SHIM_SQLITE_.*")
        .whitelist_var("SQLITE_.*")

	// Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

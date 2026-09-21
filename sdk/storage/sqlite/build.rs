use std::{env, fs, path::PathBuf};
fn main() {
    for name in [
        "NEWIM_SQLITE_ENGINE",
        "SQLITE3_LIB_DIR",
        "SQLITE3_INCLUDE_DIR",
        "SQLITE3_STATIC",
        "SQLITE3_NO_PKG_CONFIG",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    let engine = PathBuf::from(
        env::var_os("NEWIM_SQLITE_ENGINE")
            .expect("use the verified native runner: make store-engine then make build/check"),
    );
    let identity =
        fs::read_to_string(engine.join("identity")).expect("verified engine identity missing");
    assert_eq!(
        identity.trim(),
        "2026-07-24 19:02:57 bf7c7f30031888f4e796e429ab3978879485813aaca6f641c7b33e4e09459bcc"
    );
    assert_eq!(
        fs::canonicalize(&engine).unwrap(),
        fs::canonicalize(env::var_os("SQLITE3_LIB_DIR").unwrap()).unwrap()
    );
    assert_eq!(env::var("SQLITE3_STATIC").as_deref(), Ok("1"));
    assert_eq!(env::var("SQLITE3_NO_PKG_CONFIG").as_deref(), Ok("1"));
    assert!(engine.join("libsqlite3.a").is_file());
    println!(
        "cargo:rerun-if-changed={}",
        engine.join("identity").display()
    );
}

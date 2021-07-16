extern crate atomicwrites;
extern crate tempfile;

use std::{env,fs,path};
use std::io::{self,Read,Write};
use atomicwrites::{AtomicFile,AllowOverwrite,DisallowOverwrite};
use tempfile::TempDir;

fn get_tmp() -> path::PathBuf {
    TempDir::new().unwrap().into_path()
}

#[test]
fn test_simple_allow_override() {
    let tmpdir = get_tmp();
    let path = tmpdir.join("haha");

    let af = AtomicFile::new(&path, AllowOverwrite).unwrap();
    let _f = std::fs::File::create(&path);
    let res: io::Result<()> = af.write(|f| f.write_all(b"HELLO"))
        .map_err(|x| x.into());
    res.unwrap();

    let mut rv = String::new();
    let mut testfd = fs::File::open(&path).unwrap();
    testfd.read_to_string(&mut rv).unwrap();
    assert_eq!(&rv[..], "HELLO");
}

#[test]
fn test_simple_disallow_overwrite() {
    let tmpdir = get_tmp();
    let path = tmpdir.join("haha");

    let _f = std::fs::File::create(&path);
    let af = AtomicFile::new(&path, DisallowOverwrite).unwrap();
    assert!(af.write(|f| f.write_all(b"HELLO")).is_err());

}

#[test]
fn test_allowed_pathtypes() {
    AtomicFile::new("haha", DisallowOverwrite).unwrap();
    AtomicFile::new(&"haha", DisallowOverwrite).unwrap();
    AtomicFile::new(&path::Path::new("haha"), DisallowOverwrite).unwrap();
    AtomicFile::new(&path::PathBuf::from("haha"), DisallowOverwrite).unwrap();
}

#[test]
fn test_unicode() {
    let dmitri = "Дмитрий";
    let greeting = format!("HELLO {}", dmitri);

    let tmpdir = get_tmp();
    let path = tmpdir.join(dmitri);

    let af = AtomicFile::new(&path, DisallowOverwrite).unwrap();
    af.write(|f| {
        f.write_all(greeting.as_bytes())
    }).unwrap();

    let mut rv = String::new();
    let mut testfd = fs::File::open(&path).unwrap();
    testfd.read_to_string(&mut rv).unwrap();
    assert_eq!(rv, greeting);
}

#[test]
fn test_weird_paths() {
    let tmpdir = get_tmp();
    env::set_current_dir(tmpdir).expect("setup failed");

    AtomicFile::new("foo", AllowOverwrite).unwrap().write(|f| {
        f.write_all(b"HELLO")
    }).unwrap();
    let mut rv = String::new();
    let mut testfd = fs::File::open("foo").unwrap();
    testfd.read_to_string(&mut rv).unwrap();
    assert_eq!(rv, "HELLO");
}

/// Test the error that is returned if the file already exists
/// with `OverwriteBehavior::DisallowOverwrite`.
#[test]
fn disallow_overwrite_error() -> io::Result<()> {
    let tmp = TempDir::new()?;
    let file = tmp.path().join("dest");
    let af = AtomicFile::new(&file, DisallowOverwrite).unwrap();

    // touch file
    fs::write(&file, "")?;

    match af.write(|f: &mut fs::File| f.write(b"abc")) {
        Ok(_) => panic!("should fail!"),
        Err(e) => {
            let e = io::Error::from(e);
            match e.kind() {
                io::ErrorKind::AlreadyExists => Ok(()),
                _ => Err(e)
            }
        }
    }
}

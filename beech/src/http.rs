#![cfg(feature = "curl")]
use anyhow::Result;
use curl::easy::Easy;
use std::thread;
use std::time::Duration;

pub fn get(u: &url::Url) -> Result<(u32, Vec<u8>)> {
    let mut data = Vec::new();

    let mut backoff = Duration::from_millis(10);
    let mut handle = Easy::new();
    let mut rc: u32 = 0;
    for _ in 0..3 {
        handle.url(u.as_str())?;
        handle.accept_encoding("gzip")?;
        handle.follow_location(true)?;
        {
            let mut transfer = handle.transfer();
            transfer.write_function(|new_data| {
                data.extend_from_slice(new_data);
                Ok(new_data.len())
            })?;
            transfer.perform()?;
        }
        rc = handle.response_code()?;
        if rc < 500 || rc >= 600 {
            break;
        }
        thread::sleep(backoff);
        backoff *= 2;
        handle.reset();
        data.clear()
    }
    Ok((rc, data))
}

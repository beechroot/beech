use crate::Result;
use failure::format_err;
use libflate::gzip::Decoder;
use std::io::Read;

pub struct Decompressor {
    decompress: bool,
}
impl Decompressor {
    fn new(decompress: bool) -> Self {
        Decompressor { decompress }
    }
    pub fn process(&self, buf: &[u8]) -> Result<Vec<u8>> {
        if self.decompress {
            let mut decoder = Decoder::new(buf)?;
            let mut buf = Vec::new();
            decoder.read_to_end(&mut buf)?;
            Ok(buf)
        } else {
            Ok(buf.to_vec())
        }
    }
}

pub fn resolve_compression_processor(spec: &Option<String>) -> Result<Decompressor> {
    if let Some(compression) = spec {
        if compression == "gzip" {
            Ok(Decompressor::new(true))
        } else {
            Err(format_err!("{}: unknown compression scheme", compression))
        }
    } else {
        Ok(Decompressor::new(false))
    }
}

use std::io::Read;

trait Soorce {
	fn get_bytes(&self, file_name: Option<&str>) -> Vec<u8>;
}

struct GZipSoorce<'a> {
       delegate: &'a Soorce
}

impl <'a> Read for GZipDecoder<'a> {
	fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
		Ok(0)
	}
}

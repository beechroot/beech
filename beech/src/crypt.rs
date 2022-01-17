use crate::Result;
use failure::format_err;
use ring::aead::*;

const SEGREDO: [u8; 32] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 99,
    99,
];
const EXPECTED_KEY_NAME: &str = "AMILAR1";

pub struct Decryptor {
    key: Option<[u8; 32]>,
}
impl Decryptor {
    fn new(key: Option<[u8; 32]>) -> Self {
        Decryptor { key }
    }
    pub fn process(&self, buf: &[u8], maybe_nonce_bytes: &Option<Vec<u8>>) -> Result<Vec<u8>> {
        if let Some(key_bytes) = self.key {
            let mut in_out = buf.to_vec();
            let opening_key = OpeningKey::new(&CHACHA20_POLY1305, &key_bytes)?;
            let nonce_bytes = maybe_nonce_bytes
                .as_ref()
                .ok_or(format_err!("missing nonce"))?;
            let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes[..])?;
            let _ = open_in_place(&opening_key, nonce, Aad::empty(), 0, &mut in_out)?;
            Ok(in_out)
        } else {
            Ok(buf.to_vec())
        }
    }
}

pub fn resolve_crypto_processor(maybe_key_name: &Option<String>) -> Result<Decryptor> {
    maybe_key_name
        .as_ref()
        .map_or(Ok(Decryptor::new(None)), |key_name| {
            if key_name == EXPECTED_KEY_NAME {
                // TODO: lookup key based on spec_prefix
                Ok(Decryptor::new(Some(SEGREDO)))
            } else {
                Err(format_err!("invalid crypto spec"))
            }
        })
}

use crate::error::Error;
use base64::{engine::general_purpose, Engine as _};

pub fn decode_jwt(token: &str) -> Result<(), Error> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::InvalidResponse(
            "Invalid JWT token format".to_string(),
        ));
    }

    let payload = general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| Error::InvalidResponse(format!("Base64 decoding error: {}", e)))?;

    let decoded_str = String::from_utf8(payload)
        .map_err(|e| Error::InvalidResponse(format!("UTF-8 decoding error: {}", e)))?;

    println!("Decoded JWT payload: {}", decoded_str);

    Ok(())
}

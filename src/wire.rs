//! The Kafka protocol's primitive types and message framing: big-endian
//! integers, length-prefixed strings, bytes and arrays, a request header
//! and a response header, each message behind a four-byte size.

use std::io::Read;

use transport::error::{Result, classify, protocol_error};

pub const API_PRODUCE: i16 = 0;
pub const API_FETCH: i16 = 1;
pub const API_METADATA: i16 = 3;
pub const API_VERSIONS: i16 = 18;

/// The most one message may be.
pub const MAX_MESSAGE: usize = 100 * 1024 * 1024;

/// Writes the primitive types.
#[derive(Default)]
pub struct Writer {
    out: Vec<u8>,
}

impl Writer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn int8(&mut self, value: i8) -> &mut Self {
        self.out.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn int16(&mut self, value: i16) -> &mut Self {
        self.out.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn int32(&mut self, value: i32) -> &mut Self {
        self.out.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn int64(&mut self, value: i64) -> &mut Self {
        self.out.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// A nullable string: `None` is length -1.
    pub fn string(&mut self, value: Option<&str>) -> &mut Self {
        match value {
            Some(text) => {
                self.int16(i16::try_from(text.len()).unwrap_or(i16::MAX));
                self.out.extend_from_slice(text.as_bytes());
            }
            None => {
                self.int16(-1);
            }
        }
        self
    }

    /// Nullable bytes: `None` is length -1.
    pub fn bytes(&mut self, value: Option<&[u8]>) -> &mut Self {
        match value {
            Some(bytes) => {
                self.int32(i32::try_from(bytes.len()).unwrap_or(i32::MAX));
                self.out.extend_from_slice(bytes);
            }
            None => {
                self.int32(-1);
            }
        }
        self
    }

    /// An array's count; the elements follow through the writer.
    pub fn array(&mut self, count: usize) -> &mut Self {
        self.int32(i32::try_from(count).unwrap_or(i32::MAX))
    }

    pub fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.out.extend_from_slice(bytes);
        self
    }

    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.out
    }
}

/// Reads the primitive types.
pub struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| protocol_error("a field that runs past the message"))?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    /// # Errors
    /// Past the end.
    pub fn int8(&mut self) -> Result<i8> {
        Ok(i8::from_be_bytes([self.take(1)?[0]]))
    }

    /// # Errors
    /// Past the end.
    pub fn int16(&mut self) -> Result<i16> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    /// # Errors
    /// Past the end.
    pub fn int32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// # Errors
    /// Past the end.
    pub fn int64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        let mut array = [0u8; 8];
        array.copy_from_slice(b);
        Ok(i64::from_be_bytes(array))
    }

    /// # Errors
    /// Past the end, or not UTF-8.
    pub fn string(&mut self) -> Result<Option<String>> {
        let length = self.int16()?;
        if length < 0 {
            return Ok(None);
        }
        let bytes = self.take(usize::try_from(length).unwrap_or(0))?;
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|_| protocol_error("a string that is not UTF-8"))
    }

    /// # Errors
    /// Past the end.
    pub fn bytes(&mut self) -> Result<Option<&'a [u8]>> {
        let length = self.int32()?;
        if length < 0 {
            return Ok(None);
        }
        self.take(usize::try_from(length).unwrap_or(0)).map(Some)
    }

    /// An array's count, a null array being zero.
    ///
    /// # Errors
    /// Past the end.
    pub fn array(&mut self) -> Result<usize> {
        Ok(usize::try_from(self.int32()?).unwrap_or(0))
    }

    #[must_use]
    pub fn rest(&self) -> &'a [u8] {
        &self.bytes[self.at.min(self.bytes.len())..]
    }
}

/// A request: the size, the header, and `body`.
#[must_use]
pub fn request(
    api_key: i16,
    api_version: i16,
    correlation: i32,
    client: &str,
    body: &[u8],
) -> Vec<u8> {
    let mut header = Writer::new();
    header
        .int16(api_key)
        .int16(api_version)
        .int32(correlation)
        .string(Some(client))
        .raw(body);
    frame(&header.finish())
}

/// A response: the size, the correlation id, and `body`.
#[must_use]
pub fn response(correlation: i32, body: &[u8]) -> Vec<u8> {
    let mut out = Writer::new();
    out.int32(correlation).raw(body);
    frame(&out.finish())
}

fn frame(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 4);
    out.extend_from_slice(
        &i32::try_from(message.len())
            .unwrap_or(i32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(message);
    out
}

/// Read one message behind its size, or `None` when the peer closed
/// between messages.
///
/// # Errors
/// A connection that closes mid-message, or a size over [`MAX_MESSAGE`].
pub fn read_message(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let mut size = [0u8; 4];
    let first = reader
        .read(&mut size[..1])
        .map_err(|e| classify("reading a message size", &e))?;
    if first == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut size[1..])
        .map_err(|e| classify("reading a message size", &e))?;
    let size = usize::try_from(i32::from_be_bytes(size))
        .map_err(|_| protocol_error("a negative message size"))?;
    if size > MAX_MESSAGE {
        return Err(protocol_error("a message over what Xmip will read"));
    }
    let mut message = vec![0u8; size];
    reader
        .read_exact(&mut message)
        .map_err(|e| classify("reading a message", &e))?;
    Ok(Some(message))
}

/// The header of a request: api key, version, correlation id, client id,
/// and the body after them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation: i32,
    pub client: Option<String>,
}

/// Split a request message into its header and body.
///
/// # Errors
/// A message shorter than a header.
pub fn read_request(message: &[u8]) -> Result<(RequestHeader, &[u8])> {
    let mut reader = Reader::new(message);
    let header = RequestHeader {
        api_key: reader.int16()?,
        api_version: reader.int16()?,
        correlation: reader.int32()?,
        client: reader.string()?,
    };
    Ok((header, reader.rest()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitives_and_framing_round_trip() {
        let mut body = Writer::new();
        body.int8(-1)
            .int16(300)
            .int32(-70_000)
            .int64(1 << 40)
            .string(Some("orders"))
            .string(None)
            .bytes(Some(b"bin"))
            .bytes(None)
            .array(2);
        let request = request(API_PRODUCE, 3, 7, "xmip", &body.finish());
        let message = read_message(&mut request.as_slice())
            .expect("read")
            .expect("one");
        let (header, rest) = read_request(&message).expect("header");
        assert_eq!(
            header,
            RequestHeader {
                api_key: API_PRODUCE,
                api_version: 3,
                correlation: 7,
                client: Some("xmip".into())
            }
        );
        let mut reader = Reader::new(rest);
        assert_eq!(reader.int8().expect("i8"), -1);
        assert_eq!(reader.int16().expect("i16"), 300);
        assert_eq!(reader.int32().expect("i32"), -70_000);
        assert_eq!(reader.int64().expect("i64"), 1 << 40);
        assert_eq!(reader.string().expect("s").as_deref(), Some("orders"));
        assert_eq!(reader.string().expect("null"), None);
        assert_eq!(reader.bytes().expect("b"), Some(&b"bin"[..]));
        assert_eq!(reader.bytes().expect("null"), None);
        assert_eq!(reader.array().expect("array"), 2);
        assert!(reader.rest().is_empty());
        assert!(reader.int8().is_err());
        let response = response(7, &[1, 2]);
        let message = read_message(&mut response.as_slice())
            .expect("read")
            .expect("one");
        assert_eq!(Reader::new(&message).int32().expect("correlation"), 7);
        assert!(read_message(&mut &[][..]).expect("closed").is_none());
        assert!(read_message(&mut &[0, 0, 0, 5, 1][..]).is_err(), "short");
        assert!(read_message(&mut &[0x7f, 0, 0, 0][..]).is_err(), "too big");
    }
}

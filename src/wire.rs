//! The Kafka protocol's primitive types and message framing: a request
//! header and a response header, each message behind a four-byte size.
//!
//! Integers are big-endian and read and written through codec's byte cursor
//! and writer (`i16_be`, `i32_be`, ...). What is Kafka's own is here: the
//! nullable string behind an `INT16` length, nullable bytes behind an
//! `INT32` length, and an array's count, as [`Kafka`] on the cursor and
//! [`KafkaWrite`] on the bytes being written.

use std::io::Read;

use codec::cursor::Cursor;
use codec::writer::ByteWriter;
use transport::error::{Result, classify, protocol_error};

pub const API_PRODUCE: i16 = 0;
pub const API_FETCH: i16 = 1;
pub const API_METADATA: i16 = 3;
pub const API_VERSIONS: i16 = 18;

/// The most one message may be.
pub const MAX_MESSAGE: usize = 100 * 1024 * 1024;

/// Kafka's own fields, read off codec's cursor.
pub trait Kafka<'a> {
    /// A nullable string: length -1 is `None`.
    ///
    /// # Errors
    /// Past the end, or not UTF-8.
    fn nullable_string(&mut self) -> Result<Option<String>>;

    /// Nullable bytes: length -1 is `None`.
    ///
    /// # Errors
    /// Past the end.
    fn nullable_bytes(&mut self) -> Result<Option<&'a [u8]>>;

    /// An array's count, a null array being zero.
    ///
    /// # Errors
    /// Past the end.
    fn count(&mut self) -> Result<usize>;
}

impl<'a> Kafka<'a> for Cursor<'a> {
    fn nullable_string(&mut self) -> Result<Option<String>> {
        let Ok(length) = usize::try_from(self.i16_be()?) else {
            return Ok(None);
        };
        String::from_utf8(self.take(length)?.to_vec())
            .map(Some)
            .map_err(|_| protocol_error("a string that is not UTF-8"))
    }

    fn nullable_bytes(&mut self) -> Result<Option<&'a [u8]>> {
        let Ok(length) = usize::try_from(self.i32_be()?) else {
            return Ok(None);
        };
        Ok(Some(self.take(length)?))
    }

    fn count(&mut self) -> Result<usize> {
        Ok(usize::try_from(self.i32_be()?).unwrap_or(0))
    }
}

/// Kafka's own fields, written beside codec's writer.
pub trait KafkaWrite {
    /// A nullable string: `None` is length -1.
    fn nullable_string(&mut self, value: Option<&str>) -> &mut Self;

    /// Nullable bytes: `None` is length -1.
    fn nullable_bytes(&mut self, value: Option<&[u8]>) -> &mut Self;

    /// An array's count; the elements follow.
    fn count(&mut self, count: usize) -> &mut Self;
}

impl KafkaWrite for Vec<u8> {
    fn nullable_string(&mut self, value: Option<&str>) -> &mut Self {
        match value {
            Some(text) => self
                .i16_be(i16::try_from(text.len()).unwrap_or(i16::MAX))
                .bytes(text.as_bytes()),
            None => self.i16_be(-1),
        }
    }

    fn nullable_bytes(&mut self, value: Option<&[u8]>) -> &mut Self {
        match value {
            Some(bytes) => self
                .i32_be(i32::try_from(bytes.len()).unwrap_or(i32::MAX))
                .bytes(bytes),
            None => self.i32_be(-1),
        }
    }

    fn count(&mut self, count: usize) -> &mut Self {
        self.i32_be(i32::try_from(count).unwrap_or(i32::MAX))
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
    let mut header = Vec::new();
    header
        .i16_be(api_key)
        .i16_be(api_version)
        .i32_be(correlation)
        .nullable_string(Some(client))
        .bytes(body);
    frame(&header)
}

/// A response: the size, the correlation id, and `body`.
#[must_use]
pub fn response(correlation: i32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.i32_be(correlation).bytes(body);
    frame(&out)
}

fn frame(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 4);
    out.i32_be(i32::try_from(message.len()).unwrap_or(i32::MAX))
        .bytes(message);
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
    let mut cursor = Cursor::new(message);
    let header = RequestHeader {
        api_key: cursor.i16_be()?,
        api_version: cursor.i16_be()?,
        correlation: cursor.i32_be()?,
        client: cursor.nullable_string()?,
    };
    Ok((header, cursor.remaining()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitives_and_framing_round_trip() {
        let mut body = Vec::new();
        body.i8(-1)
            .i16_be(300)
            .i32_be(-70_000)
            .i64_be(1 << 40)
            .nullable_string(Some("orders"))
            .nullable_string(None)
            .nullable_bytes(Some(b"bin"))
            .nullable_bytes(None)
            .count(2);
        let request = request(API_PRODUCE, 3, 7, "xmip", &body);
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
        let mut cursor = Cursor::new(rest);
        assert_eq!(cursor.i8().expect("i8"), -1);
        assert_eq!(cursor.i16_be().expect("i16"), 300);
        assert_eq!(cursor.i32_be().expect("i32"), -70_000);
        assert_eq!(cursor.i64_be().expect("i64"), 1 << 40);
        assert_eq!(
            cursor.nullable_string().expect("s").as_deref(),
            Some("orders")
        );
        assert_eq!(cursor.nullable_string().expect("null"), None);
        assert_eq!(cursor.nullable_bytes().expect("b"), Some(&b"bin"[..]));
        assert_eq!(cursor.nullable_bytes().expect("null"), None);
        assert_eq!(cursor.count().expect("array"), 2);
        assert!(cursor.is_empty());
        let error = cursor.count().expect_err("past the end");
        assert!(error.message.contains("runs past"), "{}", error.message);
        assert!(!error.retryable);
        let response = response(7, &[1, 2]);
        let message = read_message(&mut response.as_slice())
            .expect("read")
            .expect("one");
        assert_eq!(Cursor::new(&message).i32_be().expect("correlation"), 7);
        assert!(read_message(&mut &[][..]).expect("closed").is_none());
        assert!(read_message(&mut &[0, 0, 0, 5, 1][..]).is_err(), "short");
        assert!(read_message(&mut &[0x7f, 0, 0, 0][..]).is_err(), "too big");
    }
}

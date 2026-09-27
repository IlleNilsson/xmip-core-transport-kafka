//! The record batch, message format version 2: a header with a CRC-32C over
//! everything after it, then records each behind a varint length — the
//! unit a Produce carries and a Fetch returns.
//!
//! A record's fields are zig-zagged varints; the varint, the zig-zag and
//! CRC-32C (`CRC_32_ISCSI`) are codec's. A record carries its headers
//! after its value — a count, then each key and value behind its length —
//! which is where the wire event's Kafka binding puts an event's `ce_`
//! attributes and its `content-type`. Nothing was written there, and
//! nothing read, until 2026-09-26.

use codec::crc::CRC_32_ISCSI;
use codec::cursor::Cursor;
use codec::varint::{unzigzag, zigzag};
use codec::writer::ByteWriter;
use transport::error::{Result, protocol_error};

/// A record header: its key and its value. A null value — length -1 on
/// the wire — is read as an empty one.
pub type Header = (String, Vec<u8>);

/// One record as it was read: its offset in the partition, its key, its
/// value and its headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}

/// A record to write: its key and its value, each nullable, and its
/// headers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [Header],
}

impl<'a> Entry<'a> {
    /// A key and a value, no headers.
    #[must_use]
    pub const fn new(key: Option<&'a [u8]>, value: Option<&'a [u8]>) -> Self {
        Self {
            key,
            value,
            headers: &[],
        }
    }

    /// The same entry carrying `headers`.
    #[must_use]
    pub const fn with_headers(mut self, headers: &'a [Header]) -> Self {
        self.headers = headers;
        self
    }
}

impl<'a> From<&'a Record> for Entry<'a> {
    fn from(record: &'a Record) -> Self {
        Self::new(record.key.as_deref(), record.value.as_deref()).with_headers(&record.headers)
    }
}

/// `value` zig-zagged, as a record's fields carry a signed integer.
fn signed(value: usize) -> u64 {
    zigzag(i64::try_from(value).unwrap_or(i64::MAX))
}

/// One batch of `records`, key, value and headers each, starting at
/// `base_offset`.
#[must_use]
pub fn encode_batch(base_offset: i64, records: &[Entry<'_>]) -> Vec<u8> {
    let last = i32::try_from(records.len().saturating_sub(1)).unwrap_or(i32::MAX);
    let mut body = Vec::new();
    body.i16_be(0) // attributes: no compression
        .i32_be(last)
        .i64_be(0) // first timestamp
        .i64_be(0) // max timestamp
        .i64_be(-1) // producer id
        .i16_be(-1) // producer epoch
        .i32_be(-1) // base sequence
        .i32_be(i32::try_from(records.len()).unwrap_or(i32::MAX));
    for (i, entry) in records.iter().enumerate() {
        let mut record = vec![0u8]; // attributes
        record.varint(zigzag(0)).varint(signed(i)); // timestamp and offset deltas
        for field in [entry.key, entry.value] {
            match field {
                Some(bytes) => record.varint(signed(bytes.len())).bytes(bytes),
                None => record.varint(zigzag(-1)),
            };
        }
        record.varint(signed(entry.headers.len()));
        for (key, value) in entry.headers {
            record
                .varint(signed(key.len()))
                .bytes(key.as_bytes())
                .varint(signed(value.len()))
                .bytes(value);
        }
        body.varint(signed(record.len())).bytes(&record);
    }
    let mut out = Vec::with_capacity(body.len() + 21);
    out.i64_be(base_offset)
        .i32_be(i32::try_from(body.len() + 9).unwrap_or(i32::MAX))
        .i32_be(-1) // partition leader epoch
        .byte(2) // magic
        .u32_be(CRC_32_ISCSI.checksum(&body))
        .bytes(&body);
    out
}

/// Every record in `bytes`, one batch after another.
///
/// # Errors
/// A batch that breaks off, a magic that is not 2, a CRC that does not
/// check, a record that runs past its length, or a header key that is not
/// UTF-8.
pub fn decode_batches(bytes: &[u8]) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut batches = Cursor::new(bytes);
    while batches.remaining().len() >= 12 {
        let base_offset = batches.i64_be()?;
        let length = usize::try_from(batches.i32_be()?)
            .map_err(|_| protocol_error("a negative batch length"))?;
        let mut batch = Cursor::new(batches.take(length)?);
        batch.skip(4)?; // partition leader epoch
        if batch.byte()? != 2 {
            return Err(protocol_error("a batch that is not format version 2"));
        }
        let crc = batch.u32_be()?;
        let body = batch.take_rest();
        if CRC_32_ISCSI.checksum(body) != crc {
            return Err(protocol_error("a batch CRC that does not check"));
        }
        let mut body = Cursor::new(body);
        body.skip(36)?; // attributes to base sequence
        let count = usize::try_from(body.i32_be()?).unwrap_or(0);
        for _ in 0..count {
            let length = usize::try_from(unzigzag(body.varint()?))
                .map_err(|_| protocol_error("a negative record length"))?;
            let mut record = Cursor::new(body.take(length)?);
            record.skip(1)?; // attributes
            record.varint()?; // timestamp delta
            let offset_delta = unzigzag(record.varint()?);
            let key = field(&mut record)?;
            let value = field(&mut record)?;
            let headers = headers(&mut record)?;
            records.push(Record {
                offset: base_offset + offset_delta,
                key,
                value,
                headers,
            });
        }
    }
    Ok(records)
}

/// A record's key or value: a zig-zagged length, -1 being `None`.
fn field(record: &mut Cursor<'_>) -> Result<Option<Vec<u8>>> {
    let Ok(length) = usize::try_from(unzigzag(record.varint()?)) else {
        return Ok(None);
    };
    Ok(Some(record.take(length)?.to_vec()))
}

/// A record's headers: a zig-zagged count, then each key and value. A
/// record that ends before its count — written before format 2 settled on
/// headers — has none.
fn headers(record: &mut Cursor<'_>) -> Result<Vec<Header>> {
    if record.is_empty() {
        return Ok(Vec::new());
    }
    let count = usize::try_from(unzigzag(record.varint()?)).unwrap_or(0);
    let mut headers = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let key = field(record)?.unwrap_or_default();
        let key = String::from_utf8(key)
            .map_err(|_| protocol_error("a record header key that is not UTF-8"))?;
        headers.push((key, field(record)?.unwrap_or_default()));
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_round_trip() {
        let batch = encode_batch(
            10,
            &[
                Entry::new(Some(b"k1"), Some(b"first")),
                Entry::new(None, Some(b"second\r\n\0")),
                Entry::new(Some(b""), None),
            ],
        );
        let records = decode_batches(&batch).expect("decode");
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 10);
        assert_eq!(records[0].key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(records[1].offset, 11);
        assert_eq!(records[1].key, None);
        assert_eq!(records[1].value.as_deref(), Some(&b"second\r\n\0"[..]));
        assert_eq!(records[2].key.as_deref(), Some(&b""[..]));
        assert_eq!(records[2].value, None);
        assert!(records.iter().all(|record| record.headers.is_empty()));
        let third = encode_batch(13, &[Entry::new(None, Some(b"third"))]);
        let two = [batch.clone(), third].concat();
        let records = decode_batches(&two).expect("decode two");
        assert_eq!(records.len(), 4);
        assert_eq!(records[3].offset, 13);
        assert!(decode_batches(&[]).expect("empty").is_empty());
    }

    #[test]
    fn headers_travel_with_their_record_in_order() {
        let headers: Vec<Header> = vec![
            ("ce_specversion".to_string(), b"1.0".to_vec()),
            ("content-type".to_string(), b"application/json".to_vec()),
            ("empty".to_string(), Vec::new()),
            ("bytes".to_string(), vec![0, 0xff, b'\n']),
        ];
        let batch = encode_batch(
            0,
            &[
                Entry::new(None, Some(b"{}")).with_headers(&headers),
                Entry::new(Some(b"k"), Some(b"bare")),
            ],
        );
        let records = decode_batches(&batch).expect("decode");
        assert_eq!(records[0].headers, headers);
        assert_eq!(records[0].value.as_deref(), Some(&b"{}"[..]));
        assert!(records[1].headers.is_empty());
        let again = encode_batch(5, &[Entry::from(&records[0])]);
        let read = decode_batches(&again).expect("again");
        assert_eq!(read[0].headers, headers, "a record written as it was read");
    }

    #[test]
    fn a_header_the_record_does_not_hold_is_refused() {
        let mut two_claimed = Vec::new();
        two_claimed
            .varint(zigzag(2))
            .varint(zigzag(1))
            .bytes(b"k")
            .varint(zigzag(-1));
        let error = headers(&mut Cursor::new(&two_claimed)).expect_err("one short");
        assert!(error.message.contains("end inside"), "{}", error.message);
        let mut null = Vec::new();
        null.varint(zigzag(1))
            .varint(zigzag(1))
            .bytes(b"k")
            .varint(zigzag(-1));
        let read = headers(&mut Cursor::new(&null)).expect("a null value");
        assert_eq!(read, [("k".to_string(), Vec::new())]);
        let mut binary_key = Vec::new();
        binary_key
            .varint(zigzag(1))
            .varint(zigzag(1))
            .bytes(&[0xff])
            .varint(zigzag(0));
        assert!(headers(&mut Cursor::new(&binary_key)).is_err(), "not UTF-8");
        assert!(headers(&mut Cursor::new(&[])).expect("none").is_empty());
    }

    #[test]
    fn a_broken_batch_is_refused() {
        let mut batch = encode_batch(0, &[Entry::new(None, Some(b"x"))]);
        let last = batch.len() - 1;
        batch[last] ^= 0xff;
        assert!(decode_batches(&batch).is_err(), "CRC");
        let mut batch = encode_batch(0, &[Entry::new(None, Some(b"x"))]);
        batch[16] = 1;
        assert!(decode_batches(&batch).is_err(), "magic");
        let batch = encode_batch(0, &[Entry::new(None, Some(b"x"))]);
        let error = decode_batches(&batch[..batch.len() - 3]).expect_err("cut");
        assert!(error.message.contains("runs past"), "{}", error.message);
        assert!(!error.retryable);
    }
}

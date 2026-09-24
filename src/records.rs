//! The record batch, message format version 2: a header with a CRC-32C over
//! everything after it, then records each behind a varint length — the
//! unit a Produce carries and a Fetch returns.
//!
//! A record's fields are zig-zagged varints; the varint, the zig-zag and
//! CRC-32C (`CRC_32_ISCSI`) are codec's.

use codec::crc::CRC_32_ISCSI;
use codec::cursor::Cursor;
use codec::varint::{unzigzag, zigzag};
use codec::writer::ByteWriter;
use transport::error::{Result, protocol_error};

/// One record as it was read: its offset in the partition, its key, its
/// value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// A record to write: its key and its value, each nullable.
pub type Entry<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

/// `value` zig-zagged, as a record's fields carry a signed integer.
fn signed(value: usize) -> u64 {
    zigzag(i64::try_from(value).unwrap_or(i64::MAX))
}

/// One batch of `records`, key and value each, starting at `base_offset`.
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
    for (i, (key, value)) in records.iter().enumerate() {
        let mut record = vec![0u8]; // attributes
        record.varint(zigzag(0)).varint(signed(i)); // timestamp and offset deltas
        for field in [key, value] {
            match field {
                Some(bytes) => record.varint(signed(bytes.len())).bytes(bytes),
                None => record.varint(zigzag(-1)),
            };
        }
        record.varint(zigzag(0)); // headers
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
/// check, or a record that runs past its length.
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
            records.push(Record {
                offset: base_offset + offset_delta,
                key,
                value,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_round_trip() {
        let batch = encode_batch(
            10,
            &[
                (Some(b"k1"), Some(b"first")),
                (None, Some(b"second\r\n\0")),
                (Some(b""), None),
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
        let two = [batch.clone(), encode_batch(13, &[(None, Some(b"third"))])].concat();
        let records = decode_batches(&two).expect("decode two");
        assert_eq!(records.len(), 4);
        assert_eq!(records[3].offset, 13);
        assert!(decode_batches(&[]).expect("empty").is_empty());
    }

    #[test]
    fn a_broken_batch_is_refused() {
        let mut batch = encode_batch(0, &[(None, Some(b"x"))]);
        let last = batch.len() - 1;
        batch[last] ^= 0xff;
        assert!(decode_batches(&batch).is_err(), "CRC");
        let mut batch = encode_batch(0, &[(None, Some(b"x"))]);
        batch[16] = 1;
        assert!(decode_batches(&batch).is_err(), "magic");
        let batch = encode_batch(0, &[(None, Some(b"x"))]);
        let error = decode_batches(&batch[..batch.len() - 3]).expect_err("cut");
        assert!(error.message.contains("runs past"), "{}", error.message);
        assert!(!error.retryable);
    }
}

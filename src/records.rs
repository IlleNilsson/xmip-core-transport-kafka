//! The record batch, message format version 2: a header with a CRC-32C over
//! everything after it, then records each behind a varint length — the
//! unit a Produce carries and a Fetch returns.

use transport::error::{Result, protocol_error};

/// One record as it was read: its offset in the partition, its key, its
/// value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// CRC-32C, Castagnoli, as the batch header carries it.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xffff_ffff;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn varint(out: &mut Vec<u8>, value: i64) {
    let mut zigzag = u64::from_le_bytes(((value << 1) ^ (value >> 63)).to_le_bytes());
    loop {
        let byte = u8::try_from(zigzag & 0x7f).unwrap_or(0);
        zigzag >>= 7;
        if zigzag == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], at: &mut usize) -> Result<i64> {
    let mut value: u64 = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes
            .get(*at)
            .ok_or_else(|| protocol_error("a varint past the end"))?;
        *at += 1;
        if shift < 64 {
            value |= u64::from(byte & 0x7f) << shift;
        }
        if byte & 0x80 == 0 {
            let decoded = i64::try_from(value >> 1).unwrap_or(i64::MAX);
            return Ok(if value & 1 == 1 { !decoded } else { decoded });
        }
    }
    Err(protocol_error("a varint over ten bytes"))
}

/// A record to write: its key and its value, each nullable.
pub type Entry<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

/// One batch of `records`, key and value each, starting at `base_offset`.
#[must_use]
pub fn encode_batch(base_offset: i64, records: &[Entry<'_>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0i16.to_be_bytes()); // attributes: no codec
    let last = i32::try_from(records.len().saturating_sub(1)).unwrap_or(i32::MAX);
    body.extend_from_slice(&last.to_be_bytes());
    body.extend_from_slice(&0i64.to_be_bytes()); // first timestamp
    body.extend_from_slice(&0i64.to_be_bytes()); // max timestamp
    body.extend_from_slice(&(-1i64).to_be_bytes()); // producer id
    body.extend_from_slice(&(-1i16).to_be_bytes()); // producer epoch
    body.extend_from_slice(&(-1i32).to_be_bytes()); // base sequence
    body.extend_from_slice(
        &i32::try_from(records.len())
            .unwrap_or(i32::MAX)
            .to_be_bytes(),
    );
    for (i, (key, value)) in records.iter().enumerate() {
        let mut record = vec![0u8]; // attributes
        varint(&mut record, 0); // timestamp delta
        varint(&mut record, i64::try_from(i).unwrap_or(0)); // offset delta
        for field in [key, value] {
            match field {
                Some(bytes) => {
                    varint(&mut record, i64::try_from(bytes.len()).unwrap_or(0));
                    record.extend_from_slice(bytes);
                }
                None => varint(&mut record, -1),
            }
        }
        varint(&mut record, 0); // headers
        varint(&mut body, i64::try_from(record.len()).unwrap_or(0));
        body.extend_from_slice(&record);
    }
    let mut out = Vec::with_capacity(body.len() + 21);
    out.extend_from_slice(&base_offset.to_be_bytes());
    out.extend_from_slice(
        &i32::try_from(body.len() + 9)
            .unwrap_or(i32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(&(-1i32).to_be_bytes()); // partition leader epoch
    out.push(2); // magic
    out.extend_from_slice(&crc32c(&body).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

/// Every record in `bytes`, one batch after another.
///
/// # Errors
/// A batch that breaks off, a magic that is not 2, a CRC that does not
/// check, or a record that runs past its length.
pub fn decode_batches(bytes: &[u8]) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut at = 0;
    while at + 12 <= bytes.len() {
        let base_offset = i64::from_be_bytes(slice8(bytes, at)?);
        let length = usize::try_from(i32::from_be_bytes(slice4(bytes, at + 8)?))
            .map_err(|_| protocol_error("a negative batch length"))?;
        let batch = bytes
            .get(at + 12..at + 12 + length)
            .ok_or_else(|| protocol_error("a batch that breaks off"))?;
        at += 12 + length;
        if batch.len() < 9 || batch[4] != 2 {
            return Err(protocol_error("a batch that is not format version 2"));
        }
        let crc = u32::from_be_bytes([batch[5], batch[6], batch[7], batch[8]]);
        let body = &batch[9..];
        if crc32c(body) != crc {
            return Err(protocol_error("a batch CRC that does not check"));
        }
        if body.len() < 40 {
            return Err(protocol_error("a batch header that breaks off"));
        }
        let count = usize::try_from(i32::from_be_bytes(slice4(body, 36)?)).unwrap_or(0);
        let mut cursor = 40;
        for _ in 0..count {
            let length = usize::try_from(read_varint(body, &mut cursor)?)
                .map_err(|_| protocol_error("a negative record length"))?;
            let record = body
                .get(cursor..cursor + length)
                .ok_or_else(|| protocol_error("a record that runs past its batch"))?;
            cursor += length;
            let mut inner = 1; // attributes
            read_varint(record, &mut inner)?; // timestamp delta
            let offset_delta = read_varint(record, &mut inner)?;
            let key = field(record, &mut inner)?;
            let value = field(record, &mut inner)?;
            records.push(Record {
                offset: base_offset + offset_delta,
                key,
                value,
            });
        }
    }
    Ok(records)
}

fn field(record: &[u8], at: &mut usize) -> Result<Option<Vec<u8>>> {
    let length = read_varint(record, at)?;
    if length < 0 {
        return Ok(None);
    }
    let length = usize::try_from(length).unwrap_or(0);
    let bytes = record
        .get(*at..*at + length)
        .ok_or_else(|| protocol_error("a field that runs past its record"))?;
    *at += length;
    Ok(Some(bytes.to_vec()))
}

fn slice4(bytes: &[u8], at: usize) -> Result<[u8; 4]> {
    let mut out = [0u8; 4];
    out.copy_from_slice(
        bytes
            .get(at..at + 4)
            .ok_or_else(|| protocol_error("a batch that breaks off"))?,
    );
    Ok(out)
}

fn slice8(bytes: &[u8], at: usize) -> Result<[u8; 8]> {
    let mut out = [0u8; 8];
    out.copy_from_slice(
        bytes
            .get(at..at + 8)
            .ok_or_else(|| protocol_error("a batch that breaks off"))?,
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_crc_is_castagnoli_and_batches_round_trip() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
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
        assert!(decode_batches(&batch[..batch.len() - 3]).is_err(), "cut");
    }
}

//! The client's side of one connection to a broker: metadata, produce,
//! fetch — the three requests a Location needs to append to a partition
//! and to read on from an offset.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, TransportError, classify, protocol_error};
use transport::socket;

use crate::records::{Record, decode_batches, encode_batch};
use codec::cursor::Cursor;
use codec::writer::ByteWriter;

use crate::wire::{API_FETCH, API_METADATA, API_PRODUCE, Kafka, KafkaWrite, read_message, request};

/// What a Metadata answered about one topic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicMetadata {
    pub partitions: Vec<i32>,
    /// The leader of partition 0, as `host:port`.
    pub leader: String,
}

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    name: String,
    correlation: i32,
}

impl Client {
    /// Connect to `broker`.
    ///
    /// # Errors
    /// Where the broker could not be reached.
    pub fn connect(broker: &str, client: &str, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(broker, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        Ok(Self {
            reader,
            writer,
            name: client.to_string(),
            correlation: 0,
        })
    }

    /// Metadata v1 for `topic`.
    ///
    /// # Errors
    /// Where the broker went away, or answered an error for the topic.
    pub fn metadata(&mut self, topic: &str) -> Result<TopicMetadata> {
        let mut body = Vec::new();
        body.count(1).nullable_string(Some(topic));
        let answer = self.call(API_METADATA, 1, &body)?;
        let mut reader = Cursor::new(&answer);
        let mut brokers = Vec::new();
        for _ in 0..reader.count()? {
            let node = reader.i32_be()?;
            let host = reader.nullable_string()?.unwrap_or_default();
            let port = reader.i32_be()?;
            reader.nullable_string()?; // rack
            brokers.push((node, format!("{host}:{port}")));
        }
        reader.i32_be()?; // controller
        let mut metadata = None;
        for _ in 0..reader.count()? {
            let error = reader.i16_be()?;
            let name = reader.nullable_string()?.unwrap_or_default();
            reader.i8()?; // internal
            let mut partitions = Vec::new();
            let mut leader = String::new();
            for _ in 0..reader.count()? {
                reader.i16_be()?; // partition error
                let partition = reader.i32_be()?;
                let leader_node = reader.i32_be()?;
                for _ in 0..reader.count()? {
                    reader.i32_be()?; // replica
                }
                for _ in 0..reader.count()? {
                    reader.i32_be()?; // isr
                }
                if partition == 0 {
                    leader = brokers
                        .iter()
                        .find(|(node, _)| *node == leader_node)
                        .map(|(_, address)| address.clone())
                        .unwrap_or_default();
                }
                partitions.push(partition);
            }
            if name == topic {
                if error != 0 {
                    return Err(broker_error(error, &format!("metadata for {topic}")));
                }
                metadata = Some(TopicMetadata { partitions, leader });
            }
        }
        metadata.ok_or_else(|| protocol_error(format!("no metadata for {topic}")))
    }

    /// Produce v3: one record to `partition` of `topic`, acknowledged by
    /// the leader; the offset it was written at.
    ///
    /// # Errors
    /// Where the broker went away or answered an error.
    pub fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> Result<i64> {
        let batch = encode_batch(0, &[(key, Some(value))]);
        let mut body = Vec::new();
        body.nullable_string(None)
            .i16_be(1)
            .i32_be(10_000)
            .count(1)
            .nullable_string(Some(topic))
            .count(1)
            .i32_be(partition)
            .nullable_bytes(Some(&batch));
        let answer = self.call(API_PRODUCE, 3, &body)?;
        let mut reader = Cursor::new(&answer);
        if reader.count()? == 0 {
            return Err(protocol_error("a produce answered without a topic"));
        }
        reader.nullable_string()?;
        if reader.count()? == 0 {
            return Err(protocol_error("a produce answered without a partition"));
        }
        reader.i32_be()?;
        let error = reader.i16_be()?;
        let base_offset = reader.i64_be()?;
        if error != 0 {
            return Err(broker_error(error, "the produce"));
        }
        Ok(base_offset)
    }

    /// Fetch v4: the records of `partition` of `topic` from `offset` on.
    ///
    /// # Errors
    /// Where the broker went away or answered an error.
    pub fn fetch(&mut self, topic: &str, partition: i32, offset: i64) -> Result<Vec<Record>> {
        let mut body = Vec::new();
        body.i32_be(-1)
            .i32_be(100)
            .i32_be(1)
            .i32_be(1 << 20)
            .i8(0)
            .count(1)
            .nullable_string(Some(topic))
            .count(1)
            .i32_be(partition)
            .i64_be(offset)
            .i32_be(1 << 20);
        let answer = self.call(API_FETCH, 4, &body)?;
        let mut reader = Cursor::new(&answer);
        reader.i32_be()?; // throttle
        if reader.count()? == 0 {
            return Err(protocol_error("a fetch answered without a topic"));
        }
        reader.nullable_string()?;
        if reader.count()? == 0 {
            return Err(protocol_error("a fetch answered without a partition"));
        }
        reader.i32_be()?;
        let error = reader.i16_be()?;
        reader.i64_be()?; // high watermark
        reader.i64_be()?; // last stable offset
        for _ in 0..reader.count()? {
            reader.i64_be()?;
            reader.i64_be()?;
        }
        let set = reader.nullable_bytes()?.unwrap_or(&[]);
        if error != 0 {
            return Err(broker_error(error, "the fetch"));
        }
        decode_batches(set)
    }

    fn call(&mut self, api_key: i16, api_version: i16, body: &[u8]) -> Result<Vec<u8>> {
        self.correlation += 1;
        let message = request(api_key, api_version, self.correlation, &self.name, body);
        self.writer
            .write_all(&message)
            .map_err(|e| classify("writing a request", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a request", &e))?;
        let answer = read_message(&mut self.reader)?
            .ok_or_else(|| protocol_error("the broker closed before answering"))?;
        let mut reader = Cursor::new(&answer);
        if reader.i32_be()? != self.correlation {
            return Err(protocol_error("an answer to another request"));
        }
        Ok(reader.remaining().to_vec())
    }
}

/// A broker error code as an error: the retriable ones retryable.
fn broker_error(code: i16, what: &str) -> TransportError {
    let message = format!("the broker answered {what} with error {code}");
    match code {
        3 | 5 | 6 | 7 | 8 | 9 | 13 | 14 | 15 | 16 | 19 | 20 => TransportError::retryable(message),
        _ => TransportError::permanent(message),
    }
}

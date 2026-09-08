//! The broker's side of one connection: what a test puts at the far end,
//! and what a Location that lets a producer write straight into Xmip runs.
//!
//! Not a broker. One session answers one client from logs kept in memory:
//! Metadata names itself the leader of every partition, Produce appends,
//! Fetch reads on from an offset. Replication, retention, consumer groups
//! and the rest of a cluster are a broker's.

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::records::{Entry, Record, decode_batches, encode_batch};
use crate::wire::{
    API_FETCH, API_METADATA, API_PRODUCE, API_VERSIONS, Reader, Writer, read_message, read_request,
    response,
};

/// What the client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client produced; here is the Stream, one per record.
    Produced(Arrived),
    /// The client fetched `topic`'s partition from this offset.
    Fetched { topic: String, offset: i64 },
}

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: SocketAddr,
    local: String,
    logs: BTreeMap<(String, i32), Vec<Record>>,
    pending: Vec<Arrived>,
}

impl Session {
    /// Accept one client on `listener`.
    ///
    /// # Errors
    /// Where the connection could not be accepted.
    pub fn accept(listener: &TcpListener, timeout: Option<Duration>) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let local = stream
            .local_addr()
            .map_err(|e| classify("reading the local address", &e))?
            .to_string();
        let (reader, writer) = socket::split(stream)?;
        Ok(Self {
            reader,
            writer,
            peer,
            local,
            logs: BTreeMap::new(),
            pending: Vec::new(),
        })
    }

    /// Hold `values` in `topic`'s partition 0 for a consumer to fetch.
    #[must_use]
    pub fn with_records(mut self, topic: &str, values: &[&[u8]]) -> Self {
        let log = self.logs.entry((topic.to_string(), 0)).or_default();
        for value in values {
            let offset = i64::try_from(log.len()).unwrap_or(0);
            log.push(Record {
                offset,
                key: None,
                value: Some(value.to_vec()),
            });
        }
        self
    }

    /// The next record the client produces, or `None` when it closed.
    /// Metadata and fetches are answered on the way.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_produce(&mut self) -> Result<Option<Arrived>> {
        loop {
            if !self.pending.is_empty() {
                return Ok(Some(self.pending.remove(0)));
            }
            match self.next_event()? {
                Some(Event::Produced(arrived)) => return Ok(Some(arrived)),
                Some(Event::Fetched { .. }) => {}
                None => return Ok(None),
            }
        }
    }

    /// The next thing the client did, or `None` when it closed. A produce
    /// of several records reports the first here and the rest through
    /// [`Session::next_produce`].
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or
    /// the client asked for what this session does not serve.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            let Some(message) = read_message(&mut self.reader)? else {
                return Ok(None);
            };
            let (header, body) = read_request(&message)?;
            let mut reader = Reader::new(body);
            let mut answer = Writer::new();
            let event = match header.api_key {
                API_VERSIONS => {
                    answer.int16(0).array(3);
                    for (key, min, max) in
                        [(API_PRODUCE, 3, 3), (API_FETCH, 4, 4), (API_METADATA, 1, 1)]
                    {
                        answer.int16(key).int16(min).int16(max);
                    }
                    None
                }
                API_METADATA => {
                    self.answer_metadata(&mut reader, &mut answer)?;
                    None
                }
                API_PRODUCE => self.answer_produce(&mut reader, &mut answer)?,
                API_FETCH => self.answer_fetch(&mut reader, &mut answer)?,
                other => {
                    return Err(protocol_error(format!(
                        "api key {other} is not served here"
                    )));
                }
            };
            let bytes = response(header.correlation, &answer.finish());
            self.writer
                .write_all(&bytes)
                .map_err(|e| classify("writing a response", &e))?;
            self.writer
                .flush()
                .map_err(|e| classify("flushing a response", &e))?;
            if let Some(event) = event {
                return Ok(Some(event));
            }
        }
    }

    /// Metadata v1: this session is broker 0 and leads partition 0 of every
    /// topic asked for.
    fn answer_metadata(&self, reader: &mut Reader<'_>, answer: &mut Writer) -> Result<()> {
        let count = reader.array()?;
        let mut topics = Vec::with_capacity(count);
        for _ in 0..count {
            topics.push(reader.string()?.unwrap_or_default());
        }
        let (host, port) = self.local.rsplit_once(':').unwrap_or((&self.local, "0"));
        answer.array(1).int32(0).string(Some(host));
        answer
            .int32(port.parse().unwrap_or(0))
            .string(None)
            .int32(0);
        answer.array(topics.len());
        for topic in &topics {
            answer.int16(0).string(Some(topic)).int8(0).array(1);
            answer
                .int16(0)
                .int32(0)
                .int32(0)
                .array(1)
                .int32(0)
                .array(1)
                .int32(0);
        }
        Ok(())
    }

    /// Produce v3: append every batch, answer the base offset of each.
    fn answer_produce(
        &mut self,
        reader: &mut Reader<'_>,
        answer: &mut Writer,
    ) -> Result<Option<Event>> {
        reader.string()?;
        reader.int16()?;
        reader.int32()?;
        let topics = reader.array()?;
        answer.array(topics);
        let mut first = None;
        for _ in 0..topics {
            let topic = reader.string()?.unwrap_or_default();
            let partitions = reader.array()?;
            answer.string(Some(&topic)).array(partitions);
            for _ in 0..partitions {
                let partition = reader.int32()?;
                let set = reader.bytes()?.unwrap_or(&[]);
                let base = self.append(&topic, partition, set, &mut first)?;
                answer.int32(partition).int16(0).int64(base).int64(-1);
            }
        }
        answer.int32(0);
        Ok(first.map(Event::Produced))
    }

    /// Fetch v4: the records from the offset on, in one batch.
    fn answer_fetch(&self, reader: &mut Reader<'_>, answer: &mut Writer) -> Result<Option<Event>> {
        for _ in 0..4 {
            reader.int32()?;
        }
        reader.int8()?;
        let topics = reader.array()?;
        answer.int32(0).array(topics);
        let mut fetched = None;
        for _ in 0..topics {
            let topic = reader.string()?.unwrap_or_default();
            let partitions = reader.array()?;
            answer.string(Some(&topic)).array(partitions);
            for _ in 0..partitions {
                let partition = reader.int32()?;
                let offset = reader.int64()?;
                reader.int32()?;
                let log = self.logs.get(&(topic.clone(), partition));
                let high = log.map_or(0, |l| i64::try_from(l.len()).unwrap_or(0));
                let set = log.map_or_else(Vec::new, |l| {
                    let from = usize::try_from(offset).unwrap_or(0).min(l.len());
                    let records: Vec<Entry<'_>> = l[from..]
                        .iter()
                        .map(|r| (r.key.as_deref(), r.value.as_deref()))
                        .collect();
                    if records.is_empty() {
                        Vec::new()
                    } else {
                        encode_batch(offset, &records)
                    }
                });
                answer.int32(partition).int16(0).int64(high).int64(high);
                answer.array(0).bytes(Some(&set));
                fetched = Some(Event::Fetched {
                    topic: topic.clone(),
                    offset,
                });
            }
        }
        Ok(fetched)
    }

    /// Append the records of `set` to the log; the base offset, and the
    /// first Stream into `first`, the rest pending.
    fn append(
        &mut self,
        topic: &str,
        partition: i32,
        set: &[u8],
        first: &mut Option<Arrived>,
    ) -> Result<i64> {
        let records = decode_batches(set)?;
        let log = self.logs.entry((topic.to_string(), partition)).or_default();
        let base = i64::try_from(log.len()).unwrap_or(0);
        for (i, record) in records.into_iter().enumerate() {
            let offset = base + i64::try_from(i).unwrap_or(0);
            let origin = format!("kafka://{}/{topic}/{partition}?offset={offset}", self.peer);
            let arrived = Arrived::new(origin, record.value.clone().unwrap_or_default());
            if first.is_none() {
                *first = Some(arrived);
            } else {
                self.pending.push(arrived);
            }
            log.push(Record { offset, ..record });
        }
        Ok(base)
    }
}

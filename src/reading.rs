//! A partition as a Receive Location reads it: the cursor its records'
//! acknowledgements move, and the mark of what it has handed up.
//!
//! A Fetch answers whole record batches, so it may return records below the
//! offset asked for; a compacted topic leaves gaps between offsets; and the
//! runtime asks a waiting Location again the moment its receive returns,
//! while what it handed up is still in its receive cycle. So a fetch reads
//! from the mark, not the cursor, and skips every record below it: a record
//! is handed up once per acknowledgement. Each record's acknowledgement
//! moves the cursor from where the record before it ended to past the
//! record, only where the cursor stands there ([`Contiguous`]), so a gap is
//! crossed and the cursor never passes a record not acknowledged. A failed
//! record, or one dropped untold, moves the mark back to where it began, so
//! it and every record after it are fetched again: at least once, never a
//! skip. The cursor lives in memory: a Location that starts again reads
//! again from its configured offset, every record not acknowledged among
//! what it reads.

use std::sync::{Arc, Mutex};

use transport::contiguous::Contiguous;
use transport::{Acknowledgement, Arrived, Headers, Verdict};
use xcore::lock::Locked;

use crate::records::Record;

/// The cursor and the mark of one partition; a clone shares both.
#[derive(Clone, Debug)]
pub struct Reading {
    /// Past the last record acknowledged in order.
    cursor: Contiguous<i64>,
    /// Past the last record handed up and not failed: where the next fetch
    /// reads from.
    mark: Arc<Mutex<i64>>,
}

impl Reading {
    /// Cursor and mark at `at`.
    #[must_use]
    pub fn new(at: i64) -> Self {
        Self {
            cursor: Contiguous::new(at),
            mark: Arc::new(Mutex::new(at)),
        }
    }

    /// Stand cursor and mark at `at`: where a Location starts reading.
    pub fn set(&self, at: i64) {
        self.cursor.set(at);
        *self.mark.locked() = at;
    }

    /// Past the last record acknowledged in order.
    #[must_use]
    pub fn cursor(&self) -> i64 {
        self.cursor.at()
    }

    /// The offset the next fetch reads from.
    #[must_use]
    pub fn next(&self) -> i64 {
        *self.mark.locked()
    }

    /// What a fetch from `from` returned, handed up: no record below `from`
    /// or below one already handed, at most `most`, each with its origin
    /// `{origin}?offset=N` and its acknowledgement. The mark moves past the
    /// last one handed, where it still stands at `from`.
    #[must_use]
    pub fn hand_up(
        &self,
        from: i64,
        records: Vec<Record>,
        most: usize,
        origin: &str,
    ) -> Vec<Arrived> {
        let mut before = from;
        let mut arrived = Vec::with_capacity(records.len().min(most));
        for record in records {
            if arrived.len() == most {
                break;
            }
            let offset = record.offset;
            if offset < before {
                continue;
            }
            let pending = Pending {
                reading: self.clone(),
                before,
                after: offset + 1,
                told: false,
            };
            arrived.push(
                Arrived::whole(
                    format!("{origin}?offset={offset}"),
                    record.value.unwrap_or_default(),
                    Acknowledgement::deferred(move |verdict| {
                        pending.told(verdict);
                        Ok(())
                    }),
                )
                .detected()
                .with_headers(Headers::of("kafka").octets(record.headers)),
            );
            before = offset + 1;
        }
        let mut mark = self.mark.locked();
        if *mark == from {
            *mark = before;
        }
        arrived
    }

    /// Move the mark back to `to`, where it stands past it.
    fn rewind(&self, to: i64) {
        let mut mark = self.mark.locked();
        *mark = (*mark).min(to);
    }
}

/// One record handed up and not yet told: where the record before it
/// ended, and past it.
struct Pending {
    reading: Reading,
    before: i64,
    after: i64,
    told: bool,
}

impl Pending {
    /// `Accepted` and `Refused` — a log has no place to reject a record
    /// into — move the cursor past the record; `Failed` fetches it again.
    fn told(mut self, verdict: Verdict) {
        self.told = true;
        if verdict == Verdict::Failed {
            self.reading.rewind(self.before);
        } else {
            self.reading.cursor.advance(&self.before, self.after);
        }
    }
}

impl Drop for Pending {
    /// A record dropped untold is fetched again, as a failed one is.
    fn drop(&mut self) {
        if !self.told {
            self.reading.rewind(self.before);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::Refusal;

    fn log(offsets: &[i64]) -> Vec<Record> {
        offsets
            .iter()
            .map(|&offset| Record {
                offset,
                key: None,
                value: Some(offset.to_string().into_bytes()),
                headers: Vec::new(),
            })
            .collect()
    }

    fn offsets(arrived: &[Arrived]) -> Vec<String> {
        arrived
            .iter()
            .map(|one| one.origin_uri.rsplit('=').next().unwrap_or("").to_string())
            .collect()
    }

    #[test]
    fn records_below_the_offset_asked_for_are_skipped() {
        let reading = Reading::new(2);
        let arrived = reading.hand_up(2, log(&[0, 1, 2, 3]), 10, "kafka://b/t/0");
        assert_eq!(offsets(&arrived), ["2", "3"]);
        assert_eq!((reading.cursor(), reading.next()), (2, 4));
    }

    #[test]
    fn a_batch_taken_in_part_reaches_every_record_in_order() {
        let reading = Reading::new(0);
        let batch = [0, 1, 2, 3, 4];
        let mut seen = Vec::new();
        while reading.cursor() < 5 {
            let from = reading.next();
            for one in reading.hand_up(from, log(&batch), 2, "o") {
                seen.push(one.taken().expect("taken").bytes);
            }
        }
        let expected: Vec<Vec<u8>> = batch.iter().map(|o| o.to_string().into_bytes()).collect();
        assert_eq!(seen, expected, "each once, in order");
    }

    #[test]
    fn a_record_in_its_cycle_is_not_handed_up_again() {
        let reading = Reading::new(0);
        let first = reading.hand_up(0, log(&[0, 1]), 10, "o");
        let again = reading.hand_up(reading.next(), log(&[]), 10, "o");
        assert!(
            again.is_empty(),
            "the next fetch reads past what is in flight"
        );
        assert_eq!(reading.next(), 2);
        drop(again);
        for one in first {
            one.taken().expect("taken");
        }
        assert_eq!(reading.cursor(), 2);
    }

    #[test]
    fn a_failed_record_and_those_after_it_are_fetched_again() {
        let reading = Reading::new(0);
        let mut arrived = reading.hand_up(0, log(&[0, 1, 2]), 10, "o").into_iter();
        arrived.next().expect("0").taken().expect("taken");
        arrived.next().expect("1").failed().expect("told");
        assert_eq!((reading.cursor(), reading.next()), (1, 1));
        arrived.next().expect("2").taken().expect("taken");
        assert_eq!(reading.cursor(), 1, "never a skip past the failed record");
        let again = reading.hand_up(1, log(&[0, 1, 2]), 10, "o");
        assert_eq!(offsets(&again), ["1", "2"]);
    }

    #[test]
    fn a_record_dropped_untold_is_fetched_again() {
        let reading = Reading::new(0);
        drop(reading.hand_up(0, log(&[0, 1]), 10, "o"));
        assert_eq!((reading.cursor(), reading.next()), (0, 0));
    }

    #[test]
    fn the_cursor_crosses_the_gaps_a_compacted_topic_leaves() {
        let reading = Reading::new(3);
        let mut arrived = reading.hand_up(3, log(&[5, 8]), 10, "o").into_iter();
        arrived.next().expect("5").taken().expect("taken");
        assert_eq!(reading.cursor(), 6);
        arrived
            .next()
            .expect("8")
            .refused(Refusal::Unacceptable)
            .expect("told");
        assert_eq!((reading.cursor(), reading.next()), (9, 9));
    }

    #[test]
    fn a_mark_moved_back_during_a_fetch_is_kept() {
        let reading = Reading::new(0);
        let first = reading.hand_up(0, log(&[0, 1]), 10, "o");
        let from = reading.next();
        drop(first);
        assert_eq!(reading.hand_up(from, log(&[2]), 10, "o").len(), 1);
        assert_eq!(reading.next(), 0, "the failure's mark, not the fetch's");
    }
}

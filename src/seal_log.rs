//! The seq→wall-clock index: an append-only log of table seals.
//!
//! One line per seal — `table_id,max_seq,wall_clock_secs` — appended when
//! the sweeper first observes a sealed table, replayed on boot. This is a
//! *hint*, not source of truth: if it is empty or stale, the pressure
//! sweep still bounds capacity; the only effect is archival timing shifting
//! by the staleness window.
//!
//! Durability: every record is flushed and fsynced before `record`
//! returns (seals are infrequent — a few per minute at most — so the
//! syscall cost is noise). [`SealLog::gc`] drops entries for tables that
//! are no longer live, keeping the file bounded.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// One observed seal: the table id, its (immutable) max sequence number,
/// and the wall-clock second the sweeper first saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealEntry {
    /// Table id.
    pub table_id: u32,
    /// Highest sequence number in the table.
    pub max_seq: u64,
    /// Seconds since the Unix epoch when first observed.
    pub wall_clock: u64,
}

/// Append-only `table_id,max_seq,wall_clock` log with an in-memory index.
#[derive(Debug)]
pub struct SealLog {
    path: PathBuf,
    entries: HashMap<u32, SealEntry>,
}

impl SealLog {
    /// Opens the log at `path` (creating it), replaying existing entries.
    /// Malformed lines are skipped and reported via the returned count —
    /// a torn tail write from a crashed process must not poison the index.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the file cannot be opened or read.
    pub fn open(path: impl AsRef<Path>) -> io::Result<(Self, usize)> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let mut entries = HashMap::new();
        let mut skipped = 0usize;
        for line in BufReader::new(&file).lines() {
            let line = line?;
            match parse_line(&line) {
                Some(entry) => {
                    entries.insert(entry.table_id, entry);
                }
                None => skipped += 1,
            }
        }
        Ok((Self { path, entries }, skipped))
    }

    /// Records a seal: appends one line, flushes, and fsyncs.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the record cannot be persisted.
    pub fn record(&mut self, table_id: u32, max_seq: u64, wall_clock: u64) -> io::Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        writeln!(file, "{table_id},{max_seq},{wall_clock}")?;
        file.flush()?;
        file.sync_all()?;
        entries_insert(&mut self.entries, table_id, max_seq, wall_clock);
        Ok(())
    }

    /// The entry for `table_id`, if observed.
    #[must_use]
    pub fn get(&self, table_id: u32) -> Option<SealEntry> {
        self.entries.get(&table_id).copied()
    }

    /// Number of indexed entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drops every entry whose table id is not in `live_ids`, rewriting
    /// the file. Call after each sweep with the currently-live table ids.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the rewritten file cannot be persisted.
    pub fn gc(&mut self, live_ids: &[u32]) -> io::Result<()> {
        self.entries.retain(|id, _| live_ids.contains(id));
        let tmp = self.path.with_extension("gc.tmp");
        {
            let mut file = File::create(&tmp)?;
            // Deterministic order: ascending table id.
            let mut ids: Vec<u32> = self.entries.keys().copied().collect();
            ids.sort_unstable();
            for id in ids {
                let e = self.entries[&id];
                writeln!(file, "{},{},{}", e.table_id, e.max_seq, e.wall_clock)?;
            }
            file.flush()?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

fn entries_insert(map: &mut HashMap<u32, SealEntry>, table_id: u32, max_seq: u64, wall_clock: u64) {
    map.insert(
        table_id,
        SealEntry {
            table_id,
            max_seq,
            wall_clock,
        },
    );
}

fn parse_line(line: &str) -> Option<SealEntry> {
    let mut parts = line.split(',');
    let table_id = parts.next()?.parse().ok()?;
    let max_seq = parts.next()?.parse().ok()?;
    let wall_clock = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(SealEntry {
        table_id,
        max_seq,
        wall_clock,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "horton-sweeper-logtest-{}-{}-{name}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("seals.log")
    }

    #[test]
    fn append_and_replay() {
        let path = tmp_path("replay");
        let (mut log, skipped) = SealLog::open(&path).unwrap();
        assert_eq!(skipped, 0);
        assert!(log.is_empty());
        log.record(1, 100, 1_759_000_001).unwrap();
        log.record(2, 250, 1_759_000_002).unwrap();
        log.record(3, 300, 1_759_000_003).unwrap();
        assert_eq!(log.len(), 3);

        // Reopen: the index rebuilds from the file.
        let (log2, skipped) = SealLog::open(&path).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(log2.len(), 3);
        assert_eq!(
            log2.get(2),
            Some(SealEntry {
                table_id: 2,
                max_seq: 250,
                wall_clock: 1_759_000_002
            })
        );
        assert_eq!(log2.get(99), None);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn gc_drops_dead_ids() {
        let path = tmp_path("gc");
        let (mut log, _) = SealLog::open(&path).unwrap();
        for i in 1..=5u32 {
            log.record(i, u64::from(i) * 100, 1_759_000_000 + u64::from(i))
                .unwrap();
        }
        log.gc(&[2, 4]).unwrap();
        assert_eq!(log.len(), 2);
        assert!(log.get(2).is_some());
        assert!(log.get(4).is_some());

        // The rewritten file replays to the same index.
        let (log2, skipped) = SealLog::open(&path).unwrap();
        assert_eq!(skipped, 0);
        assert_eq!(log2.len(), 2);

        // New records still append after a gc.
        let (mut log3, _) = SealLog::open(&path).unwrap();
        log3.record(9, 900, 1_759_000_009).unwrap();
        assert_eq!(log3.len(), 3);
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn torn_tail_lines_are_skipped() {
        let path = tmp_path("torn");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            "1,100,1759000001\n2,200\n3,300,1759000003,extra\n4,400,1759000004\n",
        )
        .unwrap();
        let (log, skipped) = SealLog::open(&path).unwrap();
        assert_eq!(skipped, 2);
        assert_eq!(log.len(), 2);
        assert!(log.get(1).is_some());
        assert!(log.get(4).is_some());
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }
}

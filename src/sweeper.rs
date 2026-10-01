//! The sweeper: host-side policy loop over a horton [`Db`][horton::Db].
//!
//! Each [`Sweeper::sweep`] polls the table inventory
//! ([`horton::Db::tables`]), stamps newly observed seals into the
//! append-only [`SealLog`][crate::SealLog], then runs two sub-sweeps:
//!
//! - **Age sweep** (steady state): tables whose seal wall-clock is older
//!   than `age_threshold_secs` are archived — upload via the
//!   [`ColdSink`][crate::ColdSink], then [`horton::Db::archive_commit`].
//! - **Pressure sweep** (safety valve): when free table slots drop below
//!   `min_free_slots`, the coldest tables are archived until the watermark
//!   is restored. Coldness is inferred from seq recency (no read tracking
//!   in the DB, ever); victims skip the L0 head, tables with range
//!   tombstones, and — conservatively — the whole sweep defers while a
//!   compaction job is in flight (compaction itself frees slots, and the
//!   policy shouldn't routinely abort its job).
//!
//! Tombstone rule: a victim whose commit is refused with
//! [`horton::Error::WouldResurrect`] is *skipped*, not failed — the table
//! stays local and the sweep continues. (The uploaded bytes are still a
//! valid copy of that data; a later sweep after compaction can commit
//! them.) Insert-only workloads never trip this.
//!
//! Multi-writer note: the sweeper is per-primary; it stamps its own
//! `node_id` into every descriptor. The deterministic LWW collision rule —
//! `(seal wall-clock, node_id)` — is a *replica read* concern (convergent,
//! not linearizable) and is documented, not implemented, here.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use horton::{BlockDevice, Db, Manifest};

use crate::seal_log::SealLog;
use crate::sink::{ColdSink, Receipt, SealHandle, TableDescriptor};

/// Policy knobs for one sweeper.
#[derive(Debug, Clone)]
pub struct SweeperConfig {
    /// This primary's id, stamped into every descriptor (part of the
    /// replica-side LWW collision rule).
    pub node_id: u32,
    /// Age sweep: archive tables sealed longer ago than this (seconds).
    pub age_threshold_secs: u64,
    /// Pressure sweep: trigger when free table slots drop below this.
    pub min_free_slots: usize,
    /// Upper bound on tables archived in a single [`Sweeper::sweep`].
    pub max_tables_per_sweep: usize,
}

impl Default for SweeperConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            age_threshold_secs: 7 * 24 * 3600,
            min_free_slots: 2,
            max_tables_per_sweep: 8,
        }
    }
}

/// What one [`Sweeper::sweep`] did.
#[derive(Debug, Clone, Default)]
pub struct SweepReport {
    /// Table ids archived by the age sweep.
    pub age_archived: Vec<u32>,
    /// Table ids archived by the pressure sweep.
    pub pressure_archived: Vec<u32>,
    /// Victims whose commit was refused with `WouldResurrect` (still
    /// local; the sweep continued).
    pub skipped_would_resurrect: Vec<u32>,
    /// True when the pressure sweep deferred because a compaction job was
    /// in flight.
    pub pressure_deferred_for_compaction: bool,
    /// Bytes uploaded to the sink across both sweeps.
    pub bytes_uploaded: u64,
}

/// Failure modes of [`Sweeper::sweep`].
#[derive(Debug)]
pub enum SweeperError<SinkE, DevE> {
    /// The cold sink failed.
    Sink(SinkE),
    /// horton failed (device I/O, corrupt block, …).
    Db(horton::Error<DevE>),
    /// The seal log failed.
    Log(io::Error),
    /// A victim vanished between inventory and commit in a way the commit
    /// did not report cleanly (defensive; `archive_commit` is idempotent).
    Vanished {
        /// The level the table was expected at.
        level: usize,
        /// The missing table id.
        table_id: u32,
    },
}

impl<SinkE: fmt::Debug, DevE: fmt::Debug> fmt::Display for SweeperError<SinkE, DevE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sink(e) => write!(f, "sink error: {e:?}"),
            Self::Db(e) => write!(f, "horton error: {e:?}"),
            Self::Log(e) => write!(f, "seal log error: {e}"),
            Self::Vanished { level, table_id } => {
                write!(f, "table {table_id} vanished from level {level} mid-sweep")
            }
        }
    }
}

impl<SinkE: fmt::Debug, DevE: fmt::Debug> std::error::Error for SweeperError<SinkE, DevE> {}

/// One archive candidate: the fields victim selection needs.
#[derive(Debug, Clone, Copy)]
pub struct Candidate {
    /// The level holding the table.
    pub level: usize,
    /// Table id.
    pub table_id: u32,
    /// Highest sequence number (coldness signal: lower is colder).
    pub max_seq: u64,
    /// Range-tombstone blocks: nonzero tables are skipped (they would
    /// trip the tombstone rule; compact first).
    pub rdel_blocks: u32,
    /// True for the newest table at L0 (the write head's landing zone):
    /// never archived by the pressure sweep.
    pub is_l0_head: bool,
}

/// Pure victim selection: coldest (`max_seq` ascending) first, skipping
/// the L0 head and tombstone-bearing tables, taking at most `need`.
///
/// Kept free of horton types so it unit-tests without a database.
#[must_use]
pub fn select_victims(candidates: &[Candidate], need: usize) -> Vec<(usize, u32)> {
    let mut eligible: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| !c.is_l0_head && c.rdel_blocks == 0)
        .collect();
    eligible.sort_by_key(|c| c.max_seq);
    eligible
        .into_iter()
        .take(need)
        .map(|c| (c.level, c.table_id))
        .collect()
}

/// The sweeper: owns the sink, the seal log, and the policy config.
#[derive(Debug)]
pub struct Sweeper<S> {
    sink: S,
    log: SealLog,
    config: SweeperConfig,
}

impl<S: ColdSink> Sweeper<S> {
    /// Builds a sweeper over `sink`, with the seal log at `log_path`.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the seal log cannot be opened.
    pub fn new(
        sink: S,
        log_path: impl AsRef<std::path::Path>,
        config: SweeperConfig,
    ) -> io::Result<Self> {
        let (log, _skipped) = SealLog::open(log_path)?;
        Ok(Self { sink, log, config })
    }

    /// Borrows the sink (for reads on the cold path, tests, …).
    #[must_use]
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Runs one full sweep: age sweep, then pressure sweep.
    ///
    /// # Errors
    ///
    /// [`SweeperError`] on sink, database, or seal-log failure. A victim
    /// refused with `WouldResurrect` is *not* an error — it lands in
    /// [`SweepReport::skipped_would_resurrect`].
    pub fn sweep<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
        const CACHE: usize,
    >(
        &mut self,
        db: &mut Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    ) -> Result<SweepReport, SweeperError<S::Error, D::Error>>
    where
        D::Error: fmt::Debug,
    {
        let mut report = SweepReport::default();
        let mut budget = self.config.max_tables_per_sweep;

        budget -= self.age_sweep(db, &mut report, budget)?;
        self.pressure_sweep(db, &mut report, budget)?;

        // Forget seals for tables that are gone (archived or compacted).
        let live: Vec<u32> = self.live_ids(db);
        self.log.gc(&live).map_err(SweeperError::Log)?;
        Ok(report)
    }

    /// Stamp-then-archive tables older than the age threshold.
    fn age_sweep<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
        const CACHE: usize,
    >(
        &mut self,
        db: &mut Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
        report: &mut SweepReport,
        budget: usize,
    ) -> Result<usize, SweeperError<S::Error, D::Error>>
    where
        D::Error: fmt::Debug,
    {
        if budget == 0 {
            return Ok(0);
        }
        let now = now_secs();
        // Stamp every newly observed seal first (observation order is the
        // age signal; a table seen for the first time ages from now).
        for level in 0..LEVELS {
            let tables = db.tables(level).unwrap_or(&[]);
            for t in tables {
                if self.log.get(t.id).is_none() {
                    self.log
                        .record(t.id, t.max_seq, now)
                        .map_err(SweeperError::Log)?;
                }
            }
        }
        // Victims: sealed longer ago than the threshold, oldest first.
        let mut victims: Vec<(u64, usize, u32)> = Vec::new();
        for level in 0..LEVELS {
            let tables = db.tables(level).unwrap_or(&[]);
            for t in tables {
                if let Some(entry) = self.log.get(t.id)
                    && now.saturating_sub(entry.wall_clock) > self.config.age_threshold_secs
                {
                    victims.push((t.max_seq, level, t.id));
                }
            }
        }
        victims.sort_by_key(|v| v.0);
        let mut done = 0;
        for (_, level, table_id) in victims.into_iter().take(budget) {
            if let Some(id) = self.archive_one(db, level, table_id, report)? {
                report.age_archived.push(id);
                done += 1;
            }
        }
        Ok(done)
    }

    /// Archive the coldest tables until the free-slot watermark holds.
    fn pressure_sweep<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
        const CACHE: usize,
    >(
        &mut self,
        db: &mut Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
        report: &mut SweepReport,
        budget: usize,
    ) -> Result<(), SweeperError<S::Error, D::Error>>
    where
        D::Error: fmt::Debug,
    {
        if budget == 0 {
            return Ok(());
        }
        // Conservative rule: never target an in-flight compaction's inputs.
        // Compaction itself frees slots, so deferring is the safe valve.
        if db.compaction_pending() {
            report.pressure_deferred_for_compaction = true;
            return Ok(());
        }
        let live: usize = (0..LEVELS)
            .map(|level| db.tables(level).unwrap_or(&[]).len())
            .sum();
        let capacity = Manifest::<LEVELS, TABLES, KEY_MAX>::CAPACITY;
        let free = capacity.saturating_sub(live);
        if free >= self.config.min_free_slots {
            return Ok(());
        }
        let need = self.config.min_free_slots - free;

        let l0_len = db.tables(0).unwrap_or(&[]).len();
        let mut candidates = Vec::new();
        for level in 0..LEVELS {
            let tables = db.tables(level).unwrap_or(&[]);
            for (i, t) in tables.iter().enumerate() {
                candidates.push(Candidate {
                    level,
                    table_id: t.id,
                    max_seq: t.max_seq,
                    rdel_blocks: t.rdel_blocks,
                    is_l0_head: level == 0 && i + 1 == l0_len,
                });
            }
        }
        for (level, table_id) in select_victims(&candidates, need.min(budget)) {
            if let Some(id) = self.archive_one(db, level, table_id, report)? {
                report.pressure_archived.push(id);
            }
        }
        Ok(())
    }

    /// Upload-then-commit for one table. Returns the table id when it was
    /// archived, `None` when it vanished first (compacted away or already
    /// gone — not an error) or was skipped by the tombstone rule.
    fn archive_one<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
        const CACHE: usize,
    >(
        &mut self,
        db: &mut Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
        level: usize,
        table_id: u32,
        report: &mut SweepReport,
    ) -> Result<Option<u32>, SweeperError<S::Error, D::Error>>
    where
        D::Error: fmt::Debug,
    {
        let Some(plan) = db.archive_plan(level, table_id) else {
            return Ok(None);
        };
        let desc = TableDescriptor {
            level,
            table_id,
            min_seq: plan.table.min_seq,
            max_seq: plan.table.max_seq,
            block_count: plan.table.block_count,
            entry_count: plan.table.entry_count,
            node_id: self.config.node_id,
            seal_wall_clock: now_secs(),
        };
        let mut handle: SealHandle = self.sink.seal(&desc).map_err(SweeperError::Sink)?;
        if !handle.complete {
            let mut buf = vec![0u8; BLOCK];
            let end = plan.table.end_block();
            let mut block = plan.table.first_block;
            while block < end {
                poll_read(db.device(), block, &mut buf).map_err(SweeperError::Db)?;
                self.sink
                    .write_block(&mut handle, &buf)
                    .map_err(SweeperError::Sink)?;
                block += 1;
            }
        }
        let receipt: Receipt = self.sink.commit(handle).map_err(SweeperError::Sink)?;
        report.bytes_uploaded += receipt.bytes;

        match block_on(db.archive_commit(level, table_id)) {
            Ok(true) => Ok(Some(table_id)),
            Ok(false) => Ok(None), // Compacted away concurrently; the
            // uploaded bytes are still a valid copy.
            Err(horton::Error::WouldResurrect { .. }) => {
                // Tombstone rule: leave it local, keep sweeping.
                report.skipped_would_resurrect.push(table_id);
                Ok(None)
            }
            Err(e) => Err(SweeperError::Db(e)),
        }
    }

    /// Every live table id across all levels.
    fn live_ids<
        D: BlockDevice,
        const BLOCK: usize,
        const KEY_MAX: usize,
        const VAL_MAX: usize,
        const CAP: usize,
        const ARENA: usize,
        const LEVELS: usize,
        const TABLES: usize,
        const BLOOM_BYTES: usize,
        const CACHE: usize,
    >(
        &self,
        db: &Db<D, BLOCK, KEY_MAX, VAL_MAX, CAP, ARENA, LEVELS, TABLES, BLOOM_BYTES, CACHE>,
    ) -> Vec<u32> {
        let mut ids = Vec::new();
        for level in 0..LEVELS {
            for t in db.tables(level).unwrap_or(&[]) {
                ids.push(t.id);
            }
        }
        ids
    }
}

/// Wall-clock seconds since the Unix epoch (0 on clock failure — the age
/// sweep then treats the table as brand new, the safe direction).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A waker that does nothing: horton's poll-based futures never
/// meaningfully pend on a memory device, and a yielding spin covers the
/// rest.
fn noop_waker() -> std::task::Waker {
    unsafe fn noop(_: *const ()) {}
    unsafe fn clone(data: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(data, &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: the vtable functions are no-ops that never touch the null
    // data pointer; the waker is never meaningfully cloned or woken.
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// Drives a horton future to completion on the calling thread (host-side;
/// no executor dependency). Horton's futures are poll-based and
/// short-lived; a yielding spin is plenty.
fn block_on<F: Future>(mut future: F) -> F::Output {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // SAFETY: the future is never moved after pinning, and it is dropped
    // before this function returns.
    let mut future = unsafe { Pin::new_unchecked(&mut future) };
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// Reads one block through a poll-based device, spinning on `Pending`.
fn poll_read<D: BlockDevice>(
    device: &D,
    id: u64,
    buf: &mut [u8],
) -> Result<(), horton::Error<D::Error>> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        match device.poll_read_block(&mut cx, id, buf) {
            Poll::Ready(Ok(())) => return Ok(()),
            Poll::Ready(Err(e)) => return Err(horton::Error::Device(e)),
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(id: u32, max_seq: u64) -> Candidate {
        Candidate {
            level: 1,
            table_id: id,
            max_seq,
            rdel_blocks: 0,
            is_l0_head: false,
        }
    }

    #[test]
    fn victims_are_coldest_first() {
        let cs = vec![cand(1, 300), cand(2, 100), cand(3, 200)];
        assert_eq!(select_victims(&cs, 2), vec![(1, 2), (1, 3)]);
        // Asking for more than available takes all.
        assert_eq!(select_victims(&cs, 10).len(), 3);
        // Zero need takes none.
        assert!(select_victims(&cs, 0).is_empty());
    }

    #[test]
    fn victims_skip_l0_head_and_rdel_tables() {
        let mut head = cand(1, 50); // coldest, but it is the L0 head
        head.level = 0;
        head.is_l0_head = true;
        let mut rdel = cand(2, 60); // cold, but carries range tombstones
        rdel.rdel_blocks = 2;
        let ok = cand(3, 999); // warmest, but eligible
        let victims = select_victims(&[head, rdel, ok], 5);
        assert_eq!(victims, vec![(1, 3)]);
    }

    #[test]
    fn victims_span_levels_oldest_first() {
        let mut a = cand(1, 10);
        a.level = 2;
        let mut b = cand(2, 5);
        b.level = 0; // L0, but not the head
        let victims = select_victims(&[a, b], 2);
        assert_eq!(victims, vec![(0, 2), (2, 1)]);
    }
}

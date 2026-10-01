//! End-to-end sweeper tests: a real horton [`Db`][horton::Db] on a memory
//! device, swept into a [`DirSink`][horton_sweeper::DirSink].

use std::fmt;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::task::{Context, Poll};

use horton::{BlockDevice, Config, Db};
use horton_sweeper::{ColdSink, DirSink, Sweeper, SweeperConfig};

type TestDb = Db<MemDevice, 4096, 256, 1024, 64, 4096, 7, 4, 1024, 8>;

const fn test_config() -> Config {
    Config::new(8, 136, 136, 4224, 0, 4)
}

/// In-memory block device for tests.
#[derive(Debug)]
struct MemDevice {
    blocks: Vec<[u8; 4096]>,
}

#[derive(Debug)]
struct MemError;

impl fmt::Display for MemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mem device error")
    }
}

impl std::error::Error for MemError {}

impl MemDevice {
    fn new() -> Self {
        Self { blocks: Vec::new() }
    }
}

impl BlockDevice for MemDevice {
    type Error = MemError;
    const BLOCK: usize = 4096;

    fn poll_read_block(
        &self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &mut [u8],
    ) -> Poll<Result<(), Self::Error>> {
        let i = id as usize;
        if i < self.blocks.len() {
            buf.copy_from_slice(&self.blocks[i]);
        } else {
            buf.fill(0);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write_block(
        &mut self,
        _cx: &mut Context<'_>,
        id: u64,
        buf: &[u8],
    ) -> Poll<Result<(), Self::Error>> {
        let i = id as usize;
        while self.blocks.len() <= i {
            self.blocks.push([0u8; 4096]);
        }
        self.blocks[i].copy_from_slice(buf);
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

fn block_on<F: std::future::Future>(mut f: F) -> F::Output {
    unsafe fn noop(_: *const ()) {}
    unsafe fn clone(data: *const ()) -> std::task::RawWaker {
        std::task::RawWaker::new(data, &VTABLE)
    }
    static VTABLE: std::task::RawWakerVTable =
        std::task::RawWakerVTable::new(clone, noop, noop, noop);
    // SAFETY: no-ops; the null pointer is never touched.
    let waker =
        unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut cx = Context::from_waker(&waker);
    // SAFETY: never moved after pinning; dropped before return.
    let mut f = unsafe { std::pin::Pin::new_unchecked(&mut f) };
    loop {
        match std::pin::Pin::as_mut(&mut f).poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn tmp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "horton-sweeper-e2e-{}-{}-{name}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn open_db() -> TestDb {
    let mut db = TestDb::new(MemDevice::new(), test_config());
    block_on(db.open()).unwrap();
    db
}

fn put_flush(db: &mut TestDb, start: u8, end: u8) {
    for i in start..end {
        block_on(db.put(&[i], &[i, i])).unwrap();
    }
    block_on(db.flush()).unwrap();
}

fn sweeper_at(root: &std::path::Path, name: &str, config: SweeperConfig) -> Sweeper<DirSink> {
    let sink = DirSink::open(root.join(format!("{name}-sink"))).unwrap();
    Sweeper::new(sink, root.join(format!("{name}.log")), config).unwrap()
}

#[test]
fn age_sweep_archives_sealed_tables() {
    let root = tmp_dir("age");
    let mut db = open_db();
    put_flush(&mut db, 0, 20);
    put_flush(&mut db, 20, 25);
    let ids: Vec<u32> = db.tables(0).unwrap().iter().map(|t| t.id).collect();
    assert_eq!(ids.len(), 2);
    let block_counts: Vec<u32> = db
        .tables(0)
        .unwrap()
        .iter()
        .map(|t| t.block_count)
        .collect();

    // Pre-seed the seal log with ancient stamps so the age sweep fires
    // deterministically (no sleeping on wall-clock resolution).
    let ancient = 1_700_000_000u64;
    let log_path = root.join("age.log");
    {
        let mut f = File::create(&log_path).unwrap();
        for (i, id) in ids.iter().enumerate() {
            writeln!(f, "{},{},{ancient}", id, 20 + i).unwrap();
        }
    }

    let config = SweeperConfig {
        age_threshold_secs: 60,
        min_free_slots: 1, // pressure sweep stays quiet
        max_tables_per_sweep: 8,
        node_id: 3,
    };
    let mut sweeper = sweeper_at(&root, "age", config);
    let report = sweeper.sweep(&mut db).unwrap();

    assert_eq!(report.age_archived.len(), 2);
    assert!(report.pressure_archived.is_empty());
    assert!(report.skipped_would_resurrect.is_empty());
    assert!(report.bytes_uploaded > 0);

    // Both tables landed in the sink, checksummed, and left the inventory.
    let archived = sweeper.sink().list_archived().unwrap();
    assert_eq!(archived.len(), 2);
    for (i, id) in ids.iter().enumerate() {
        assert!(archived.contains(id));
        let bytes = sweeper.sink().read_table(*id).unwrap().unwrap();
        assert_eq!(bytes.len() as u32, block_counts[i] * 4096);
        let stored = sweeper.sink().read_descriptor(*id).unwrap().unwrap();
        assert_eq!(stored.node_id, 3);
        assert_eq!(stored.block_count, block_counts[i]);
        assert_eq!(stored.table_id, *id);
    }
    assert_eq!(db.tables(0).unwrap().len(), 0);
    // Archived data is gone locally by design.
    let mut buf = [0u8; 2048];
    assert_eq!(block_on(db.get(&[1], &mut buf)).unwrap(), None);

    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn pressure_sweep_takes_coldest_non_head_table() {
    let root = tmp_dir("pressure");
    let mut db = open_db();
    put_flush(&mut db, 0, 10); // oldest → victim
    put_flush(&mut db, 10, 20); // newest → L0 head, spared
    let ids: Vec<u32> = db.tables(0).unwrap().iter().map(|t| t.id).collect();

    // Capacity is 28 slots; force the valve with a huge watermark.
    let config = SweeperConfig {
        age_threshold_secs: u64::MAX, // age sweep stays quiet
        min_free_slots: 28,
        max_tables_per_sweep: 8,
        node_id: 1,
    };
    let mut sweeper = sweeper_at(&root, "pressure", config);
    let report = sweeper.sweep(&mut db).unwrap();

    assert!(report.age_archived.is_empty());
    assert_eq!(report.pressure_archived, vec![ids[0]]);
    assert!(!report.pressure_deferred_for_compaction);
    let remaining: Vec<u32> = db.tables(0).unwrap().iter().map(|t| t.id).collect();
    assert_eq!(remaining, vec![ids[1]]);
    assert_eq!(sweeper.sink().list_archived().unwrap(), vec![ids[0]]);

    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn tombstone_victim_is_skipped_not_failed() {
    let root = tmp_dir("tombstone");
    let mut db = open_db();
    // t1: a live value plus a range tombstone. The rdel blocks shield it
    // from victim selection — but its live value keeps t2's tombstone
    // load-bearing.
    block_on(db.put(b"k", b"v")).unwrap();
    block_on(db.delete_range(b"a", b"b")).unwrap();
    block_on(db.flush()).unwrap();
    // t2: point tombstone hiding t1's value. Eligible (not the head, no
    // rdel blocks) — but its commit must hit WouldResurrect while t1 lives.
    block_on(db.delete(b"k")).unwrap();
    block_on(db.flush()).unwrap();
    // t3: fresh head (never a victim).
    block_on(db.put(b"j", b"w")).unwrap();
    block_on(db.flush()).unwrap();

    let tables = db.tables(0).unwrap();
    assert_eq!(tables.len(), 3);
    assert!(tables[0].rdel_blocks > 0, "t1 carries the range tombstone");
    let t2_id = tables[1].id;

    let config = SweeperConfig {
        age_threshold_secs: u64::MAX, // age sweep stays quiet
        min_free_slots: 28,           // capacity 28, live 3 → valve opens
        max_tables_per_sweep: 8,
        node_id: 1,
    };
    let mut sweeper = sweeper_at(&root, "tombstone", config);
    let report = sweeper.sweep(&mut db).unwrap();

    // t2 was attempted and refused — skipped, not failed. Nothing else
    // was eligible, so nothing archived.
    assert!(report.pressure_archived.is_empty());
    assert_eq!(report.skipped_would_resurrect, vec![t2_id]);
    let remaining: Vec<u32> = db.tables(0).unwrap().iter().map(|t| t.id).collect();
    assert_eq!(remaining.len(), 3);
    // The tombstone still hides the key; the head is intact.
    let mut buf = [0u8; 2048];
    assert_eq!(block_on(db.get(b"k", &mut buf)).unwrap(), None);
    assert_eq!(
        block_on(db.get(b"j", &mut buf))
            .unwrap()
            .map(|n| buf[..n].to_vec()),
        Some(b"w".to_vec())
    );

    std::fs::remove_dir_all(&root).unwrap();
}

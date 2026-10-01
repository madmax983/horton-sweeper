//! The cold-sink contract: the shape every cold target implements.
//!
//! A sink receives immutable sealed tables: the sweeper declares a
//! [`TableDescriptor`] via [`ColdSink::seal`], streams the table's blocks in
//! order through [`ColdSink::write_block`], then finalizes with
//! [`ColdSink::commit`]. Reads go through [`ColdSink::read_table`] (the
//! cold-read path and replica bootstrap).
//!
//! Discipline (the contract every impl must honor):
//!
//! - **Idempotent per table id.** Re-uploading a fully uploaded table is
//!   always allowed: [`ColdSink::seal`] on an already-committed table with
//!   an identical descriptor returns a handle [`ColdSink::commit`] turns
//!   into the same [`Receipt`] without duplicating bytes. This is what
//!   makes crash-retry safe — a crash anywhere before
//!   [`horton::Db::archive_commit`] simply replays the upload.
//! - **Immutable bytes.** A committed table's bytes never change.
//! - **Checksum verified.** [`ColdSink::commit`] verifies the streamed
//!   bytes against the expected block count (and the sink's own running
//!   checksum) before acknowledging; a short or corrupt stream is an
//!   error, never a receipt.
//!
//! [`DirSink`] is the reference implementation: a local directory shaped
//! like object storage (one immutable blob per table id plus a descriptor
//! sidecar). Postgres arrives via the existing `horton-pg-sink` crate,
//! which implements this same contract.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::crc64::Rolling;

/// Declared at [`ColdSink::seal`], before any bytes flow: everything the
/// sink needs to name and verify one sealed table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDescriptor {
    /// The level the table was archived from.
    pub level: usize,
    /// Table id, unique per database lifetime; the sink's primary key.
    pub table_id: u32,
    /// Lowest sequence number in the table.
    pub min_seq: u64,
    /// Highest sequence number in the table.
    pub max_seq: u64,
    /// Total blocks (`rdel` + data + bloom + index + footer).
    pub block_count: u32,
    /// Key/value entries (including tombstones).
    pub entry_count: u32,
    /// The primary that sealed it; stamped by the sweeper. Part of the
    /// deterministic LWW collision rule on replicas.
    pub node_id: u32,
    /// Wall-clock (seconds since the Unix epoch) when the sweeper sealed
    /// it. Part of the deterministic LWW collision rule on replicas.
    pub seal_wall_clock: u64,
}

/// An in-progress (or already-completed) upload. Constructed by the sink
/// in [`ColdSink::seal`]; the sweeper threads it through `write_block` and
/// `commit` without interpreting it.
#[derive(Debug, Clone)]
pub struct SealHandle {
    /// The table being uploaded.
    pub table_id: u32,
    /// Expected blocks, from the descriptor.
    pub expected_blocks: u32,
    /// Blocks streamed so far.
    pub blocks_written: u32,
    /// Running checksum over the streamed bytes.
    pub checksum: u64,
    /// True when the table is already durably stored (idempotent
    /// re-upload): `write_block` is accepted and ignored, `commit`
    /// re-issues the receipt.
    pub complete: bool,
}

/// Proof of durable storage, returned by [`ColdSink::commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    /// The archived table id.
    pub table_id: u32,
    /// Bytes stored.
    pub bytes: u64,
    /// CRC-64 over the stored bytes.
    pub checksum: u64,
    /// Blocks stored.
    pub block_count: u32,
}

/// The cold-sink contract. See the module docs for the discipline.
pub trait ColdSink {
    /// The sink's error type.
    type Error: std::error::Error;

    /// Declares an upload. Idempotent: an already-committed table with an
    /// identical descriptor yields a `complete` handle.
    fn seal(&mut self, desc: &TableDescriptor) -> Result<SealHandle, Self::Error>;

    /// Streams one block, in table order. On a `complete` handle the block
    /// is accepted and ignored (idempotent re-upload).
    fn write_block(&mut self, handle: &mut SealHandle, block: &[u8]) -> Result<(), Self::Error>;

    /// Verifies and finalizes the upload, returning the receipt. Fails
    /// when the stream is short or the checksum mismatches.
    fn commit(&mut self, handle: SealHandle) -> Result<Receipt, Self::Error>;

    /// Reads a committed table's bytes, or `None` when absent.
    fn read_table(&self, table_id: u32) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Ids of all committed tables.
    fn list_archived(&self) -> Result<Vec<u32>, Self::Error>;
}

/// Reference sink: a local directory shaped like object storage.
///
/// Layout per table id `<id>`:
/// - `<id>.sst` — the immutable blob (all table blocks, in order).
/// - `<id>.desc` — the descriptor sidecar (text, one `key=value` per
///   line, including the receipt checksum).
///
/// Uploads stream to `<id>.sst.part` and rename into place on commit, so a
/// crashed upload never looks committed.
#[derive(Debug)]
pub struct DirSink {
    root: PathBuf,
    /// Open seals: table id → declared descriptor (block count re-verified
    /// at commit; the full descriptor becomes the sidecar).
    open: HashMap<u32, TableDescriptor>,
}

impl DirSink {
    /// Opens (creating) the sink directory.
    ///
    /// # Errors
    ///
    /// [`io::Error`] when the directory cannot be created.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            open: HashMap::new(),
        })
    }

    fn blob(&self, table_id: u32) -> PathBuf {
        self.root.join(format!("{table_id}.sst"))
    }

    fn part(&self, table_id: u32) -> PathBuf {
        self.root.join(format!("{table_id}.sst.part"))
    }

    fn desc_path(&self, table_id: u32) -> PathBuf {
        self.root.join(format!("{table_id}.desc"))
    }

    /// Reads a table's descriptor sidecar, or `None` when absent.
    ///
    /// # Errors
    ///
    /// [`io::Error`] on I/O failure or a malformed sidecar.
    pub fn read_descriptor(&self, table_id: u32) -> io::Result<Option<StoredDescriptor>> {
        let path = self.desc_path(table_id);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&path)?;
        StoredDescriptor::parse(&text)
            .map(Some)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed descriptor"))
    }

    fn write_descriptor(&self, desc: &TableDescriptor, receipt: &Receipt) -> io::Result<()> {
        let text = format!(
            "level={}\ntable_id={}\nmin_seq={}\nmax_seq={}\nblock_count={}\nentry_count={}\nnode_id={}\nseal_wall_clock={}\nbytes={}\nchecksum={:016x}\n",
            desc.level,
            desc.table_id,
            desc.min_seq,
            desc.max_seq,
            desc.block_count,
            desc.entry_count,
            desc.node_id,
            desc.seal_wall_clock,
            receipt.bytes,
            receipt.checksum,
        );
        let path = self.desc_path(desc.table_id);
        fs::write(&path, text)?;
        // Best-effort durability for the sidecar; the blob's fsync is the
        // real guarantee, and the sidecar is re-derivable from it.
        if let Ok(f) = File::open(&path) {
            let _ = f.sync_all();
        }
        Ok(())
    }

    fn receipt_from_stored(&self, stored: &StoredDescriptor) -> Receipt {
        Receipt {
            table_id: stored.table_id,
            bytes: stored.bytes,
            checksum: stored.checksum,
            block_count: stored.block_count,
        }
    }
}

/// A descriptor sidecar as stored on disk (descriptor fields plus the
/// receipt checksum).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDescriptor {
    /// The level the table was archived from.
    pub level: usize,
    /// Table id.
    pub table_id: u32,
    /// Lowest sequence number.
    pub min_seq: u64,
    /// Highest sequence number.
    pub max_seq: u64,
    /// Total blocks.
    pub block_count: u32,
    /// Key/value entries.
    pub entry_count: u32,
    /// Sealing primary.
    pub node_id: u32,
    /// Seal wall-clock (seconds since the Unix epoch).
    pub seal_wall_clock: u64,
    /// Bytes stored.
    pub bytes: u64,
    /// CRC-64 over the stored bytes.
    pub checksum: u64,
}

impl StoredDescriptor {
    fn parse(text: &str) -> Option<Self> {
        let get = |key: &str| {
            text.lines().find_map(|line| {
                let (k, v) = line.split_once('=')?;
                (k == key).then_some(v)
            })
        };
        Some(Self {
            level: get("level")?.parse().ok()?,
            table_id: get("table_id")?.parse().ok()?,
            min_seq: get("min_seq")?.parse().ok()?,
            max_seq: get("max_seq")?.parse().ok()?,
            block_count: get("block_count")?.parse().ok()?,
            entry_count: get("entry_count")?.parse().ok()?,
            node_id: get("node_id")?.parse().ok()?,
            seal_wall_clock: get("seal_wall_clock")?.parse().ok()?,
            bytes: get("bytes")?.parse().ok()?,
            checksum: u64::from_str_radix(get("checksum")?, 16).ok()?,
        })
    }

    /// Whether this stored descriptor matches a seal declaration (the
    /// idempotency check: same table, same bytes expected).
    #[must_use]
    pub fn matches(&self, desc: &TableDescriptor) -> bool {
        self.level == desc.level
            && self.table_id == desc.table_id
            && self.min_seq == desc.min_seq
            && self.max_seq == desc.max_seq
            && self.block_count == desc.block_count
            && self.entry_count == desc.entry_count
            && self.node_id == desc.node_id
    }
}

impl ColdSink for DirSink {
    type Error = io::Error;

    fn seal(&mut self, desc: &TableDescriptor) -> Result<SealHandle, Self::Error> {
        // Idempotent re-upload: already committed with the same shape.
        if self.blob(desc.table_id).exists()
            && let Some(stored) = self.read_descriptor(desc.table_id)?
            && stored.matches(desc)
        {
            return Ok(SealHandle {
                table_id: desc.table_id,
                expected_blocks: desc.block_count,
                blocks_written: desc.block_count,
                checksum: stored.checksum,
                complete: true,
            });
        }
        // Fresh (or superseded) upload: stream to the part file.
        let part = self.part(desc.table_id);
        File::create(&part)?.sync_all()?;
        self.open.insert(desc.table_id, desc.clone());
        Ok(SealHandle {
            table_id: desc.table_id,
            expected_blocks: desc.block_count,
            blocks_written: 0,
            checksum: Rolling::new().finish(),
            complete: false,
        })
    }

    fn write_block(&mut self, handle: &mut SealHandle, block: &[u8]) -> Result<(), Self::Error> {
        if handle.complete {
            // Idempotent re-upload of an already-stored table.
            return Ok(());
        }
        if handle.blocks_written >= handle.expected_blocks {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block stream longer than the declared block count",
            ));
        }
        let mut part = OpenOptions::new()
            .append(true)
            .open(self.part(handle.table_id))?;
        part.write_all(block)?;
        // Un-invert, update, re-invert: the running CRC-64 state.
        let mut rolling = Rolling::from_finished(handle.checksum);
        rolling.update(block);
        handle.checksum = rolling.finish();
        handle.blocks_written += 1;
        Ok(())
    }

    fn commit(&mut self, handle: SealHandle) -> Result<Receipt, Self::Error> {
        if handle.complete {
            let stored = self.read_descriptor(handle.table_id)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "complete handle without a descriptor",
                )
            })?;
            self.open.remove(&handle.table_id);
            return Ok(self.receipt_from_stored(&stored));
        }
        let desc = self.open.remove(&handle.table_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "commit without an open seal")
        })?;
        if handle.blocks_written != desc.block_count {
            let _ = fs::remove_file(self.part(handle.table_id));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "block stream shorter than the declared block count",
            ));
        }
        // Re-read and verify before the rename, so a torn local write can
        // never become a receipt.
        let part = self.part(handle.table_id);
        let bytes = fs::read(&part)?;
        if crate::crc64::checksum(&bytes) != handle.checksum {
            let _ = fs::remove_file(&part);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "part file checksum mismatch",
            ));
        }
        let receipt = Receipt {
            table_id: handle.table_id,
            bytes: bytes.len() as u64,
            checksum: handle.checksum,
            block_count: desc.block_count,
        };
        File::open(&part)?.sync_all()?;
        fs::rename(&part, self.blob(handle.table_id))?;
        self.write_descriptor(&desc, &receipt)?;
        Ok(receipt)
    }

    fn read_table(&self, table_id: u32) -> Result<Option<Vec<u8>>, Self::Error> {
        let path = self.blob(table_id);
        if !path.exists() {
            return Ok(None);
        }
        fs::read(&path).map(Some)
    }

    fn list_archived(&self) -> Result<Vec<u32>, Self::Error> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".sst")
                && let Ok(id) = stem.parse::<u32>()
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc(id: u32) -> TableDescriptor {
        TableDescriptor {
            level: 0,
            table_id: id,
            min_seq: 1,
            max_seq: 20,
            block_count: 3,
            entry_count: 20,
            node_id: 7,
            seal_wall_clock: 1_759_000_000,
        }
    }

    fn tmp() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "horton-sweeper-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn roundtrip_seal_stream_commit_read() {
        let root = tmp();
        let mut sink = DirSink::open(&root).unwrap();
        let d = desc(42);
        let mut handle = sink.seal(&d).unwrap();
        assert!(!handle.complete);
        let blocks: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 512]).collect();
        for b in &blocks {
            sink.write_block(&mut handle, b).unwrap();
        }
        let receipt = sink.commit(handle).unwrap();
        assert_eq!(receipt.table_id, 42);
        assert_eq!(receipt.block_count, 3);
        assert_eq!(receipt.bytes, 3 * 512);

        let bytes = sink.read_table(42).unwrap().unwrap();
        assert_eq!(bytes.len(), 3 * 512);
        assert_eq!(crate::crc64::checksum(&bytes), receipt.checksum);

        let stored = sink.read_descriptor(42).unwrap().unwrap();
        assert!(stored.matches(&d));
        assert_eq!(stored.checksum, receipt.checksum);
        assert_eq!(sink.list_archived().unwrap(), vec![42]);
        assert!(sink.read_table(43).unwrap().is_none());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn idempotent_reupload() {
        let root = tmp();
        let mut sink = DirSink::open(&root).unwrap();
        let d = desc(7);
        let mut h1 = sink.seal(&d).unwrap();
        sink.write_block(&mut h1, &[9u8; 128]).unwrap();
        sink.write_block(&mut h1, &[9u8; 128]).unwrap();
        sink.write_block(&mut h1, &[9u8; 128]).unwrap();
        let r1 = sink.commit(h1).unwrap();

        // Second upload of the same table: seal reports complete, commit
        // re-issues the identical receipt without touching bytes.
        let mut h2 = sink.seal(&d).unwrap();
        assert!(h2.complete);
        sink.write_block(&mut h2, &[1u8; 128]).unwrap(); // ignored
        let r2 = sink.commit(h2).unwrap();
        assert_eq!(r1, r2);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn short_stream_fails_commit() {
        let root = tmp();
        let mut sink = DirSink::open(&root).unwrap();
        let d = desc(9);
        let mut h = sink.seal(&d).unwrap();
        sink.write_block(&mut h, &[1u8; 64]).unwrap(); // 1 of 3
        let err = sink.commit(h).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(sink.read_table(9).unwrap().is_none());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn overlong_stream_rejected() {
        let root = tmp();
        let mut sink = DirSink::open(&root).unwrap();
        let d = desc(11);
        let mut h = sink.seal(&d).unwrap();
        for _ in 0..3 {
            sink.write_block(&mut h, &[1u8; 64]).unwrap();
        }
        let err = sink.write_block(&mut h, &[1u8; 64]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        fs::remove_dir_all(&root).unwrap();
    }
}

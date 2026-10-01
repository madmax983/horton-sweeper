//! The tiered-storage sweeper for [horton](https://github.com/madmax983/horton):
//! host-side policy that archives sealed tables to cold storage by *age*
//! (steady state) and by *pressure* (safety valve), and ships sealed
//! tables for eventually-consistent replication.
//!
//! Layering: everything here is a host-side policy loop. It needs clocks,
//! heuristics, and file/network I/O — none of which belongs in horton's
//! `no_alloc`, clock-free core. The mechanism it drives already exists in
//! horton: [`horton::Db::tables`] (inventory), [`horton::Db::archive_plan`]
//! (immutable sealed-table bytes), [`horton::Db::archive_commit`] (atomic
//! drop + slot reclaim), and [`horton::Db::ingest_table`] (idempotent
//! re-attach).
//!
//! The cold target is a pluggable [`ColdSink`]: Postgres and object storage
//! are first-party shapes; any third sink implements the trait. This crate
//! is the keeper of the contract and the shape, agnostic to where the bytes
//! land.
//!
//! Design authority: `docs/tiered-sweeper.md` in the horton repo.

pub mod crc64;
pub mod seal_log;
pub mod sink;
pub mod sweeper;

pub use seal_log::SealLog;
pub use sink::{ColdSink, DirSink, Receipt, SealHandle, TableDescriptor};
pub use sweeper::{Candidate, SweepReport, Sweeper, SweeperConfig, SweeperError, select_victims};

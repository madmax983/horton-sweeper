# horton-sweeper

Host-side tiered-storage sweeper and table-shipping replication policy for
[horton](https://github.com/madmax983/horton). **Local crate — no remote,
not published.**

## What it does

Runs two archive sweeps over a horton database on a small fixed volume:

1. **Age sweep** (steady state) — tables sealed longer ago than
   `age_threshold_secs` are uploaded to cold storage.
2. **Pressure sweep** (safety valve) — when free table slots drop below
   `min_free_slots`, the coldest tables are archived until the watermark
   is restored.

Both go through the same upload-then-commit discipline: `archive_plan` →
stream immutable table bytes to the sink → verify → `archive_commit`
(atomically drops the table and reclaims its slot). A victim refused with
`WouldResurrect` is *skipped*, not failed.

## Layering

Policy lives here, not in horton. Horton's core is `no_alloc`,
clock-free, and never tracks reads; this crate owns clocks, heuristics,
and I/O. It drives only public horton APIs: `Db::tables` (inventory),
`Db::archive_plan`, `Db::archive_commit`, `Db::device`, and
`Db::compaction_pending`.

## The sink contract

The cold target is pluggable. `ColdSink` is the trait:

- `seal(descriptor)` → handle (idempotent per table id)
- `write_block(handle, block)` — stream immutable bytes in order
- `commit(handle)` → `Receipt` (verifies block count + checksum first)
- `read_table(table_id)` / `list_archived()` — cold-read path and
  replica bootstrap

`DirSink` is the reference implementation: a local directory shaped like
object storage (`<id>.sst` immutable blob + `<id>.desc` sidecar carrying
level, table/seq bounds, block count, node id, seal wall-clock, and the
CRC-64 receipt checksum). Postgres arrives via the existing
`horton-pg-sink` crate, implementing this same contract.

## The seal log

The seq→wall-clock index is an append-only log: one
`table_id,max_seq,wall_clock_secs` line per observed seal, fsynced per
record, replayed on boot, `gc`'d against the live inventory after each
sweep. Torn tail lines are skipped, not fatal.

## Multi-writer note

The sweeper is per-primary; it stamps its own `node_id` into every
descriptor. The deterministic LWW collision rule (`seal wall-clock`,
`node_id`) is a *replica read* concern — convergent, not linearizable —
and is documented, not implemented, here.

## Layout

- `src/sink.rs` — `ColdSink` trait, `TableDescriptor`, `SealHandle`,
  `Receipt`, `DirSink`
- `src/seal_log.rs` — append-only `SealLog`
- `src/sweeper.rs` — `Sweeper`, `SweeperConfig`, `SweepReport`,
  pure `select_victims`
- `src/crc64.rs` — dependency-free CRC-64/ECMA
- `tests/e2e.rs` — real `Db` on a memory device: age archive,
  pressure victim selection, tombstone skip

## Gates

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Design authority: `docs/tiered-sweeper.md` in the horton repo.

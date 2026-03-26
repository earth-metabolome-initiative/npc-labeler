# Streaming Specification

Status: draft

## Goal

Replace the current SQLite-centered pipeline with a streaming architecture that can run for a long time on a small VM with low RAM and low disk usage.

## Why This Exists

The current design is structurally mismatched with the deployment goal:

- it stores the entire PubChem input locally in SQLite before classification starts
- it uses the same local database as queue, state store, and export source
- it periodically rebuilds a separate release artifact from scratch

Observed local artifact sizes already show the problem:

- `CID-SMILES.gz`: about `1.47 GB`
- `classifications.sqlite`: about `13.27 GB`

The new design must make the local canonical state close to the final published dataset instead of keeping a large intermediate database.

## Non-Goals

- support arbitrary random access into the input stream
- keep a durable retry queue
- publish invalid rows in Zenodo releases
- preserve compatibility with the old SQLite workflow

## Core Decisions

The following decisions are already made:

- the canonical local completed-output format is chunked `jsonl.zst`
- local completed chunks are immutable once sealed
- the Zenodo release should contain a merged `completed.jsonl.zst`
- the release should also contain a JSON manifest
- invalid rows are not part of the release artifacts
- invalid rows are tracked locally in a bitvec
- durable retries are removed entirely
- transient failures use bounded inline retry only, then become terminal failures
- the inline retry schedule is `1s`, `5s`, `15s` after the initial failure
- local progress/state tracking is line-indexed, not CID-indexed
- bitvecs should be memory-mapped
- chunk rotation target is `128 MiB` compressed
- chunks also rotate on clean shutdown
- the merged release file is produced by direct byte concatenation of sealed zstd chunks
- output durability should use periodic batch syncing, not sync-per-row
- a local rolling failure log should exist for diagnostics
- the rolling failure log should remain plain JSONL, not compressed
- the rolling failure log includes `smiles`
- Zenodo releases contain only `completed.jsonl.zst` and `manifest.json`
- staged local release artifacts are deleted immediately after confirmed successful upload
- interrupted release staging artifacts are deleted before the next release attempt
- completed rows preserve original input order by line index
- there is no separate checkpoint or progress file
- the local chunk index is append-only JSONL
- the completed-record schema is minimal and contains no explicit status field
- manifest metadata is compact
- bitvec files are exact-sized after a startup prepass that counts total input rows
- the startup prepass does not compute an input checksum
- the manifest does not store input identity metadata
- completed result fields are always present as arrays, including empty arrays

## High-Level Architecture

The new runtime flow is:

1. read `CID-SMILES.gz` as a stream
2. assign each input row a dense line index
3. skip rows that are already terminal according to local bitvec state
4. classify the current row
5. on success, append a record to the current completed-output chunk
6. on permanent invalid input, mark the row invalid locally
7. on transient failure, perform bounded inline retry
8. after the retry budget is exhausted, mark the row failed locally
9. periodically sync output and local state
10. on release, concatenate sealed completed chunks into one merged `completed.jsonl.zst`

## Local Canonical State

### Completed Output

Completed rows are stored in rotated immutable chunk files:

- `completed/part-000001.jsonl.zst`
- `completed/part-000002.jsonl.zst`
- ...

Rules:

- each chunk must be a valid standalone `.jsonl.zst`
- each record must end with `\n`
- each sealed chunk must end with a final newline
- chunks are append-only while active and immutable after sealing
- completed rows are emitted in original input line order

### Completed Record Schema

Each completed record is a single JSON object with exactly these fields:

- `cid`
- `smiles`
- `class_results`
- `superclass_results`
- `pathway_results`
- `isglycoside`

Example:

```json
{
  "cid": 123,
  "smiles": "CCO",
  "class_results": [],
  "superclass_results": [],
  "pathway_results": [],
  "isglycoside": false
}
```

Rules:

- there is no explicit `status` field
- `class_results`, `superclass_results`, and `pathway_results` are always present
- those three fields are always arrays
- successful empty rows are represented by empty arrays
- schema/version/provenance live in the manifest, not in each row

### Bitvec State

The local terminal state is tracked by line-indexed memory-mapped bitvecs:

- `state/done.bitvec`
- `state/invalid.bitvec`
- `state/failed.bitvec`

Semantics:

- `done.bitvec`: row was successfully processed and written to completed output
- `invalid.bitvec`: row is terminal invalid input and excluded from release
- `failed.bitvec`: row is terminal failure after bounded inline retry and excluded from release

Important rule:

- line index, not CID, is the canonical row identifier for streaming state

Why:

- line indexes are dense
- the input stream order is stable
- sparse CID space would waste storage

### Failure Diagnostics

Persistent local diagnostics use a rolling plain-JSONL log:

- `logs/failures.log`
- `logs/failures.1.log`
- `logs/failures.2.log`
- ...

Rotation policy:

- rotate when the active file exceeds `10 MiB`
- keep the newest `5` files
- delete the oldest file on rotation overflow

This log is local only and is not part of the release artifacts.

Each failure record is a JSON object containing:

- `ts`
- `line`
- `cid`
- `smiles`
- `kind`
- `message`
- `attempt`

## Removed Concepts

The new design removes all of the following:

- the SQLite queue
- the SQLite result store
- the weekly full rebuild from SQLite
- durable retry scheduling
- retry metadata files or retry databases
- a second-pass retry phase

## Input and Restart Model

Restarts are assumed to be rare.

Because of that, the restart model is intentionally simple:

- on startup, do one full prepass over `CID-SMILES.gz` to count total input rows
- allocate exact-sized bitvec files from that row count
- on restart, re-read `CID-SMILES.gz` from the beginning
- determine the current line index while streaming
- skip any line whose state is already terminal

Terminal means:

- `done.bitvec` set
- or `invalid.bitvec` set
- or `failed.bitvec` set

This design avoids the need for a seekable compressed input format.

Preconditions:

- the input file must remain stable and immutable across runs
- line numbering must be deterministic
- no separate checkpoint or progress file is maintained

## Retry and Failure Policy

There is no durable retry queue.

The retry policy is:

- perform a small bounded number of inline retries for transient failures
- retry after `1s`, then `5s`, then `15s`
- if those retries succeed, write the row to completed output
- if those retries do not succeed, mark the row in `failed.bitvec`
- continue to the next input row

This keeps the runtime simple and guarantees that the process cannot be permanently wedged on one row by a persistent `429` or other upstream problem.

## Durability Model

Durability should be batched.

Recommended policy:

- sync the active output chunk every `1,000` rows or every `5` seconds, whichever comes first
- sync bitvec state on the same cadence, but only after output durability is established for the corresponding rows
- on clean shutdown, perform a final sync and seal the current chunk

Crash-ordering rule:

1. append row to output
2. flush or sync output
3. set the corresponding bit
4. sync bitvec changes

Reason:

- if the bit is set before durable output exists, data can be silently skipped after restart
- if output lands first and the bit sync lags, the worst case is a duplicate row

Duplicates are safer than silent loss.

## Chunk Rotation

Rotation is based on compressed size, not wall clock time.

Policy:

- rotate when the active chunk reaches about `128 MiB` compressed size
- rotate on clean shutdown
- once a chunk is sealed, never modify it again

Why:

- small enough to keep corruption and upload failure localized
- small enough to make validation cheap
- large enough to avoid an excessive number of files

With the current size estimate, this would likely produce roughly `18` completed chunks for a full release.

## Release Artifacts

Each Zenodo release should include:

- `completed.jsonl.zst`
- `manifest.json`

Invalid rows and failed rows are not released.
Raw sealed completed chunks are not uploaded to Zenodo.

## Merge Strategy

The merged `completed.jsonl.zst` is built by direct byte concatenation of sealed `.jsonl.zst` chunk files.

No decode/re-encode pass is required.

Why this is acceptable:

- zstd supports concatenated frames
- every chunk is a valid standalone stream
- every chunk ends on a JSONL newline boundary

This keeps release assembly cheap and avoids reintroducing the old full-rebuild problem.

## Manifest

The manifest format is JSON.

It is the machine-readable release description and the exact specification of what went into the release.

The manifest is intentionally compact.

Top-level fields:

- `manifest_version`
- `dataset_schema_version`
- `created_at`
- `output_filename`
- `output_bytes`
- `output_sha256`
- `successful_rows`
- `invalid_rows`
- `failed_rows`
- `chunks`

Each `chunks` entry contains:

- `filename`
- `row_count`
- `bytes`
- `sha256`

The human-readable Zenodo description can summarize the same information, but `manifest.json` is the strict source of truth.

### Example Manifest Shape

```json
{
  "manifest_version": 1,
  "dataset_schema_version": 1,
  "created_at": "2026-03-26T12:00:00Z",
  "output_filename": "completed.jsonl.zst",
  "output_bytes": 1234567890,
  "output_sha256": "abc123",
  "successful_rows": 123000000,
  "invalid_rows": 37,
  "failed_rows": 1234,
  "chunks": [
    {
      "filename": "part-000001.jsonl.zst",
      "row_count": 7340032,
      "bytes": 134217728,
      "sha256": "def456"
    }
  ]
}
```

## Chunk Index

Sealed chunk metadata is recorded in append-only JSONL at:

- `state/chunks.jsonl`

Each record contains:

- `created_at`
- `filename`
- `first_line`
- `last_line`
- `row_count`
- `bytes`
- `sha256`

Example:

```json
{
  "created_at": "2026-03-26T12:00:00Z",
  "filename": "part-000001.jsonl.zst",
  "first_line": 0,
  "last_line": 7340031,
  "row_count": 7340032,
  "bytes": 134217728,
  "sha256": "def456"
}
```

The chunk index is local-only metadata and is not part of the release artifacts.

## Estimated Release Size

The current completed corpus was used as a baseline.

Measured local baseline:

- completed rows: `368,387`
- `classified`: `299,240`
- `empty`: `69,147`
- raw JSONL export size: `76,728,746` bytes
- gzipped JSONL export size: `6,460,343` bytes
- average raw JSONL size: about `208.3` bytes per row
- average compressed size: about `17.5` bytes per row
- observed compression ratio: about `11.9x`

Scaled estimate if that average remains similar:

- for `123,541,080` rows:
  - raw JSONL: about `25.73 GB`
  - compressed release: about `2.17 GB`
- for `130,000,000` rows:
  - raw JSONL: about `27.08 GB`
  - compressed release: about `2.28 GB`

Implication:

- a merged `completed.jsonl.zst` release artifact is likely to be on the order of `2.2-2.3 GB`
- this is far more compatible with the target VM than the current SQLite-based design

## Expected Directory Layout

The following layout is the current target:

```text
completed/
  part-000001.jsonl.zst
  part-000002.jsonl.zst
state/
  done.bitvec
  invalid.bitvec
  failed.bitvec
  chunks.jsonl
logs/
  failures.log
  failures.1.log
  failures.2.log
releases/
  completed.jsonl.zst
  manifest.json
```

Notes:

- `releases/` is a local staging area for release artifacts, not canonical data
- sealed completed chunks remain the canonical local source

## Open Points

The design is much narrower now, but some details are still open:

- exact error taxonomy for `kind` values in the rolling failure log
- exact implementation of bit counting for progress/reporting
- exact startup behavior when the existing bitvec sizes do not match the newly counted input row total
- exact policy for deleting local staged release artifacts after upload failure versus success

## Bottom Line

The target system is now:

- stream input directly from `CID-SMILES.gz`
- classify in one pass
- write successful rows straight into chunked publishable `jsonl.zst`
- keep only tiny local bitvec state plus a bounded failure log
- assemble a merged `completed.jsonl.zst` only at release time by concatenating sealed chunks

This is the architecture that best fits the small-VM constraint.

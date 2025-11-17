# Distributed snapshot design notes

## Current single-writer snapshot pipeline

### Snapshot ID generation
- `table/metadata.go` exposes `generateSnapshotID()` which XORs bytes from a random UUID and takes the absolute value to produce a positive 63-bit integer.
- `MetadataBuilder.newSnapshotID()` (same file) calls the generator, checks that the ID is not already used in `snapshotList`, and retries locally until it finds a unique value.
- `snapshotProducer` instances are created via `createSnapshotProducer()` in `table/snapshot_producers.go`; during construction each producer calls `txn.meta.newSnapshotID()` and captures the value inside the transaction. The ID is therefore owned by the producer instance before any manifests or manifest lists exist.

### Sequence number assignment
- The builder keeps the table-level `lastSequenceNumber` that was read from metadata. `MetadataBuilder.nextSequenceNumber()` returns `lastSequenceNumber + 1` for v2+ tables and `0` for v1 tables.
- When a `snapshotProducer` commits, it calls `txn.meta.nextSequenceNumber()` to obtain the next value and writes it into the manifest list and the snapshot entry. Sequence numbers are never exposed outside the transaction.

### Manifest and manifest list emission
- `snapshotProducer.manifests()` (table/snapshot_producers.go) gathers three buckets in parallel: brand new manifests for files appended in the transaction, manifests that encode deleted entries, and any existing manifests that should be carried forward from the parent snapshot (`producerImpl.existingManifests`).
- New manifests are written via `newManifestOutput()` → `WriteFileIO.Create()` with deterministic names such as `<commit-uuid>-m<N>.avro`. Each writer is closed before the manifest file metadata (`ManifestFile`) is computed.
- Delete manifests and carry-over manifests are built in-memory or rewritten as needed. Optional post-processing (e.g., merge append) may regroup them.
- `snapshotProducer.commit()` writes a single manifest list for the new snapshot by calling `newManifestListFileName()` (`snap-<snapshot-id>-<attempt>-<commit-uuid>.avro`), obtaining a table metadata location, and invoking `iceberg.WriteManifestList()`. The manifest list embeds the snapshot ID, parent snapshot reference, sequence number, and for v3 tables the row range.

### Transaction commit flow
- Each writer builds metadata updates (e.g., `NewAddSnapshotUpdate`, `NewSetSnapshotRefUpdate`) via its `snapshotProducer.commit()` call.
- `Transaction.apply()` accumulates the updates and requirements (`table/transaction.go`).
- `Transaction.Commit()` performs optimistic concurrency by evaluating requirements such as `AssertTableUUID` and `AssertRefSnapshotID` against the table state that was refreshed when the transaction started. It then invokes `Table.doCommit()` (`table/table.go`), which forwards the updates to the catalog, receives the new metadata location, and cleans up superseded metadata files. Finally, each update’s `PostCommit` hook runs in-process.

### Why this only works for a single host
- Snapshot IDs exist only within a transaction’s memory; they are minted lazily and coupled to a particular `snapshotProducer` instance. No API exposes “reserve snapshot ID” semantics, so every host that wants to contribute data must also own the transaction and its metadata builder.
- Sequence numbers are derived from the builder state that lives inside the transaction. Two hosts cannot coordinate on the same sequence number without sharing the builder instance.
- Manifest file names include the commit UUID that is private to the producer. They are written through the transaction’s `WriteFileIO`, and only the producer knows how many manifests it created before the manifest list is sealed.
- The manifest list and snapshot entry are written by the producer immediately after it finishes emitting manifests. There is no protocol for multiple processes to feed manifests into a shared manifest list.
- `Transaction.Commit` pushes exactly one snapshot worth of updates and asserts that the main branch moved from the transaction’s starting snapshot. That assumption breaks if multiple hosts attempted to commit the same snapshot ID independently because each commit would clobber the branch pointer and racing writers would fail the optimistic checks. Consequently, today only a single process can safely own a snapshot.

## Distributed snapshot goals
1. Reserve a snapshot ID (and optionally sequence number allocation metadata) without writing manifests or manifest lists yet.
2. Allow multiple workers to write manifests—data and delete manifests—tagged with the reserved snapshot ID, using only public APIs and without sharing the transaction object.
3. Provide a coordinator role that collects the manifests written by workers, assembles a single manifest list, writes one snapshot record into table metadata, and performs the optimistic concurrency checks that already exist in `Transaction.Commit`.
4. Keep the new APIs compatible with existing `Transaction`/`Update`/`Requirement` workflows so that non-distributed writers remain unaffected and mixed deployments can migrate incrementally.

## Proposed high-level design for distributed snapshots

### Snapshot ID reservation lifecycle
- Introduce a `SnapshotReservation` object produced by the catalog or by a light-weight transaction builder. The reservation records:
  - Reserved `snapshotID`.
  - Optional `sequenceNumber` hint or a flag signaling that the coordinator must assign it later.
  - Metadata pointers such as the commit UUID prefix and target metadata location namespace.
- Add an `Update`/`Requirement` pair (`NewReserveSnapshotUpdate`, `AssertSnapshotReservation`) so reservations participate in the optimistic concurrency system. A reservation should check that the referenced branch still points at the expected snapshot when the reservation is created.
- Reservations time out or can be explicitly canceled, preventing unbounded growth of unused IDs.

### Worker APIs for manifest production
- Allow `snapshotProducer` (or a new `DistributedSnapshotWriter`) to accept an externally supplied snapshot ID, sequence number (optional), and commit UUID. The producer still uses `WriteFileIO` to emit manifests, but it no longer writes the manifest list or snapshot updates.
- Each worker reports back the manifest metadata (file path, partition spec ID, statistics) along with per-manifest sequence metadata (if assigned locally). The coordinator stores these descriptors in a temporary manifest index table or an object store manifest queue.
- Workers can reuse the existing manifest merge and summary logic by operating over their local file batches. The summary output becomes part of the worker result so the coordinator can merge summaries.

### External/Distributed Manifest Writer
- A new helper, `iceberg.NewManifestWriterForSnapshot`, lets workers create manifest files for a pre-reserved snapshot ID without a `Transaction`.
- Callers provide the table’s `WriteFileIO`, partition spec, schema, manifest format version, snapshot ID, and the full manifest file path. The helper opens the file, wraps it in a counting writer, and returns an `ExternalManifestWriter` that embeds the regular `ManifestWriter` API.
- When `ExternalManifestWriter.ToManifestFile()` is invoked the manifest file is closed and the returned `ManifestFile` metadata is populated with the supplied snapshot ID, ensuring every encoded `ManifestEntry` inherits the same snapshot reference.

### Coordinator assembly and commit
- The coordinator retrieves all manifest descriptors associated with the reservation, groups them by content (data vs. delete), and optionally runs the manifest merge routines before writing the manifest list.
- Sequence number management happens centrally: the coordinator asks the catalog for the current `lastSequenceNumber` and writes `nextSequenceNumber` into the manifest list and snapshot entry exactly once.
- The coordinator writes the manifest list file (using the deterministic naming scheme) and then issues the usual metadata updates: `AddSnapshot`, `SetSnapshotRef`, optional ref updates, and any summary properties aggregated from workers.
- Optimistic concurrency is enforced by reusing existing requirements (`AssertRefSnapshotID`, `AssertTableUUID`) plus a new `AssertSnapshotReservation` that verifies no other coordinator reused the reservation.

### Compatibility and migration
- Local writers continue to use `Transaction` and `snapshotProducer` exactly as today; their code paths implicitly obtain a reservation and immediately fulfill it.
- Distributed writers first call the reservation API, fan out work, then have a coordinator instantiate a `Transaction` seeded with the reservation metadata. Most of the `Transaction` and `Update` types stay untouched, which keeps catalogs and file IO implementations compatible.
- Catalog implementations only need to understand the new reservation update/requirement types and persist reservation state (e.g., in table properties or a sidecar catalog table). Existing catalogs that ignore the new types simply reject distributed snapshot attempts without affecting local commits.

### Failure handling considerations
- Because manifests are written before the snapshot exists, reservations must include garbage-collection metadata (e.g., temporary manifest prefixes) so unused manifests can be cleaned when a reservation expires.
- Coordinators should support retries by making the manifest list write idempotent: all workers reference the same snapshot ID, and the manifest list filename remains stable (attempt counter can increase on each retry, similar to today’s behavior).
- Requirements ensure that if two coordinators race on the same reservation, only one succeeds; the other sees that the reservation has been consumed when it validates requirements before commit.

This document captures the baseline behavior of the v0.4.0 transaction stack and the high-level plan for extending it to distributed snapshots with shared snapshot IDs. Future iterations should break down the proposed APIs into concrete Go types and catalog protocol changes.

## Commit из готовых manifest’ов

- `Transaction.commitSnapshotFromManifests()` (table/transaction.go) writes a manifest list for a caller-provided `snapshotID` by serializing the supplied `[]ManifestFile` without touching the underlying manifest contents. The helper reuses `newManifestListFileName()` so the manifest list location stays deterministic.
- After the manifest list is persisted the helper builds a `Snapshot` struct (sequence number, schema ID, timestamp, summary, manifest list path) and enqueues the usual updates (`AddSnapshot`, `SetSnapshotRef`). Metadata state therefore mirrors the result of a local `snapshotProducer.commit()` even though manifests were prepared elsewhere.
- The helper is intentionally internal to `Transaction` for now; future distributed writers can reserve snapshot IDs, write manifests on worker hosts, and then call this coordinator path to finalize metadata.

### Проверки перед коммитом

- `AssertRefSnapshotID` is added to the transaction requirements so optimistic concurrency still ensures the main branch references the expected parent snapshot when the coordinator applies the metadata updates.
- The builder assigns sequence numbers via `MetadataBuilder.nextSequenceNumber()`; `AddSnapshot` subsequently validates that the new sequence number is strictly greater than the previously committed one on v2+ tables.
- `MetadataBuilder.AddSnapshot` also enforces timestamp monotonicity (snapshot timestamp must be newer than both the snapshot log and the table’s `lastUpdated` value) and verifies that schema/spec metadata already exists before the snapshot is appended.
- Because the manifest list path is generated through the table’s `LocationProvider`, retries continue to overwrite the same logical manifest list file until the snapshot is successfully registered, matching the single-writer behavior.

## Public API (Table)

- `Table.BeginDistributedSnapshot(ctx, props)` reserves a new snapshot ID using the current table metadata and returns a `DistributedSnapshot` descriptor. The descriptor includes the reserved `snapshotID`, the parent snapshot ID (if any), a commit UUID that worker hosts can use to derive manifest file names, and a copy of the snapshot properties that should be applied during commit. Coordinators call this method once before fanning out work to worker hosts.
- `Table.CommitDistributedSnapshot(ctx, ds, manifests, summary)` accepts the `DistributedSnapshot` from the coordinator, the list of manifest files produced by worker hosts, and a summary map that contains at least the `operation` key. The method validates that the table’s main branch still references `ds.ParentSnapshotID`, writes the manifest list, enqueues the snapshot updates, and commits them through the catalog. On success it returns a refreshed `Table` instance with the new snapshot set as current.
- Worker hosts use the `DistributedSnapshot` payload (especially `SnapshotID` and `CommitUUID`) to produce manifest files independently. Once all manifests are available, the coordinator aggregates them into a slice of `ManifestFile` descriptors and invokes `CommitDistributedSnapshot`.

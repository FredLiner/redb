# ADR XXXX: Persistence — Effect Log over Per-Campaign redb Stores

- Status: Proposed
- Date: 2026-11-18
- Location: `FredLiner/redb` fork, `docs/zemla/` on branch `zemla` (until the Zemla
  server repo exists)
- Owner: Engineer
- Approver: Engineer
- Supersedes: SQLite single-writer/multi-reader persistence model (prior project ADR-0004)

## Context

The prior server persisted to SQLite through a single writer lane and a reader pool,
with mutation + outbox + audit committed atomically per transaction. Two problems
motivate a replacement:

1. **Weak storage typing.** SQLite type affinity lets an `INTEGER` column hold text.
   STRICT tables fix storage classes but provide no enums, booleans, timestamps,
   domains, or composite types. Enum and range constraints live in `CHECK` clauses.
2. **Discipline does not survive the console.** Any typing enforced only in Rust is
   bypassed by a manual SQL edit. Type guarantees must hold for every write path,
   including administrative ones.

Domain characteristics that shape the decision (Zemla design manual, Ch. 2, 10, 11, 13):

- Hosted service. A campaign is a principality-sized map of **40–50 players**, running
  a **fixed ~6-month cycle**. Many campaigns run concurrently (planning range 10–200
  maps).
- Campaigns are fully self-contained. Cross-campaign data is limited to accounts,
  auth, entitlements, sanctions, hall of fame, matchmaking, and reference libraries
  (ruleset, regional templates, seed maps).
- Semi-structured data. Reads are either small keyed row sets or ordered sequential
  streams (outbox, map data). Joins and ad-hoc predicates are rare. SQL buys
  familiarity, not capability.
- **Turns are seasons.** Within a season's async window, uncontested private actions
  resolve immediately and contested actions are *committed, not resolved*. All
  contested actions resolve together at turn close. Shared-state contention during the
  window is minimal by design.
- Much of the remaining command surface is propose/approve: a player issues an order,
  the GM or liege approves, modifies, or denies it.
- **Delves are the exception:** instanced, synchronous sessions of up to 12
  characters with per-person timers and round-by-round resolution. The rules core
  emits every action and roll as an event (Ch. 13).
- **Conduct enforcement depends on logs.** Death review, friendly-fire records,
  visible regent/transfer/declaration records, and moderation appeals all need an
  ordered, replayable history (Ch. 11).
- Campaign-layer write rates are low (season-paced). Delve write rates are higher but
  bounded per instance.
- **Production platform:** Ubuntu LTS (current at production launch) on ext4 or XFS.
  btrfs is not used. Development happens on Windows. Durability semantics are
  specified for Linux.

## Decision

### 1. Source of truth: an append-only effect log, owned in-house

- The command handler validates the command, determines the outcome, and produces a
  **decided effect**. Only accepted effects are logged. Rejections never enter the log.
- The effect is appended to the log and fsynced. **The client is acked on successful
  log write.**
- Nondeterministic inputs (server timestamps, assigned IDs, dice results, ruleset
  version in force) are resolved before append and recorded in the entry. Replay never
  re-runs handlers. It only applies effects, so only effect application must be
  deterministic.
- The log is part of the dispatch layer's batch queue and is the sequencing authority
  for incoming writes.
- Each entry carries the originating user, command ID, and sequence stamp (see §4).
  It serves as the audit record and the source of outbound events, so
  mutation, outbox, and audit collapse into one append.

Log file format:
- Partitioned: one log per campaign, one child log per delve instance (see §5), plus
  one global log.
- Segmented files. Each entry is length + CRC framed, with a per-record version tag
  and a binary payload (postcard or equivalent).
- Group commit: a batch of entries shares one `fdatasync`. Every entry in the batch
  is acked after it returns.
- Creating a segment file, and any rename (sealing, archive moves), is followed by an
  `fsync` of the parent directory before the segment is used or the operation is
  considered done. On ext4/XFS a new directory entry is not durable until its
  directory is synced.
- Startup recovery truncates a torn tail (short read or CRC mismatch on the final
  entry).
- Segments are preallocated, sealed, and rotated. A segment is eligible for trim only
  when every store it affects has a watermark past it and a snapshot exists.

Accepted risk: a crash between accept and log fsync loses the in-flight command. The
client was never acked, so it sees a timeout or disconnect and retries. The window
is sub-millisecond processing time.

### 2. State: redb stores as rebuildable projections

- State is materialized into **redb** (pure-Rust, copy-on-write B-tree, ACID, single
  writer + MVCC readers, multiple tables committed atomically in one file).
- The redb stores are projections of the log. Losing or corrupting a store, or a
  format change, is recovered by rebuilding from snapshot + log replay.
- Each store holds an `applied_through` watermark, written in the same redb
  transaction as the applied effects. Replay resumes after the watermark and applies
  each effect exactly once.
- Effects are applied to redb by a background apply lane after ack. Validation reads
  through an **in-memory overlay** of logged-but-unapplied effects layered over redb.
  An entry is evicted from the overlay once the store's watermark passes it.

redb configuration (from the review of the fork):
- **Quick-repair on every write transaction** (`set_quick_repair(true)`, which also
  forces two-phase commit). Opening a `Database` marks it recovery-required, so with
  default settings every store open at crash time would need a full three-pass
  checksum walk and allocator rebuild on its next open. The saved allocator state is
  trusted only when the *last* commit used two-phase commit, so this must be set on
  every commit, not just some.
- **Explicit cache size per store** (`Builder::set_cache_size`). The default is 1 GiB
  per `Database` with no shared budget. Total cache = open-store cap × per-store
  cache.
- **Apply lane uses `Durability::None`, with periodic `Durability::Immediate`.** The
  effect log is the durability point, so redb does not need an fsync per apply batch.
  On a crash, non-durable commits and the watermark roll back together, and replay
  resumes from the durable watermark. `Immediate` commits happen at a bounded interval
  or entry count, before snapshots, and on store close.
- **No `compact()` on live stores.** An interrupted compaction forces a full repair,
  and compaction refuses to run with any live read transaction. Compact only at
  archive time, or rebuild a fresh file from the log instead.
- Admin and archive inspection tooling opens stores with `ReadOnlyDatabase`, so it
  never marks a store recovery-required.

### 3. File topology

- `global.redb`: accounts, credentials, sessions, campaign registry and lifecycle
  state, campaign rosters/membership, entitlements (campaign entry), cross-campaign
  sanctions and bans, hall of fame and final placings, matchmaking and party-finding
  state, global outbox/audit projections. Payment instruments and transaction details
  stay with the payment processor or merchant of record. Only entitlement state is
  stored locally.
- `campaigns/<id>.redb`: all campaign entities and campaign-scoped projections.
  Campaign scoping is structural: a campaign's data is reachable only through its own
  store handle.
- **Reference material is snapshotted into the campaign at provisioning.** This covers
  ruleset data, the regional template, and seed maps from the global map library (see
  §7). The campaign records each source library ID and version as provenance and keeps
  no live dependency on the library afterward.
- Campaign stores open lazily and close after an idle period (each `Database` holds
  its own page cache and an exclusive file lock).
- Each store upgrades its records on open. Mixed schema versions across files are
  expected and handled by per-record version tags.

Cross-file operations (campaign provisioning, close and archive, roster changes
that touch a campaign store, sanctions removing a player from a campaign) use the
global log as coordinator:
1. Append an intent to the global log (`pending`).
2. Apply to the campaign store, recording the intent sequence in the same transaction.
3. Apply to the global store and mark the intent `complete` in the same transaction.
4. On startup, scan for `pending` intents and resume. Per-store intent records make
   each step idempotent.

Ordering rules:
- Create the campaign file, `fsync` the `campaigns/` directory, then register it.
  redb does not sync the parent directory when it creates a file, so without this
  step a power loss can leave a registered campaign with no file.
- Unregister before deleting the file, then `fsync` the directory after the delete.
- A startup sweep removes orphaned files.

### 4. Ordering and idempotency

- One in-flight command per user session. A new command is not accepted until the
  previous one is acked.
- Each command carries a command ID and sequence stamp. A retry with an already-logged
  command ID returns the original ack and does not re-apply. This closes the
  "logged, ack lost" window.
- **Contested actions are commitments.** During a season's async window, contested
  actions (claims, declarations, military orders) are logged as per-player commitment
  effects, so players never write the same state. Uncontested private actions resolve
  and log immediately.
- **Turn close is a single resolver pass.** The resolver reads the campaign state plus
  all commitments for the season, resolves them together (rolls included), and emits
  the outcome as a bounded, ordered batch of decided effects under a `TurnClosed`
  boundary marker. Upkeep and standing changes are part of the same batch. The herald
  feed is published from the batch.
- Inter-user contention otherwise is confined to propose/approve flows on a single
  pending order (liege duty calls, GM rulings). Each resolution references the order
  revision it acted on, and validation rejects it if the order is no longer pending at
  that revision. No campaign-wide serialization is required for correctness.
- Pending orders are first-class state: `OrderProposed`, `OrderRevised`,
  `OrderWithdrawn`, `OrderResolved { approved | modified | denied }` are effects. The
  current order state is a projection, and the full order history is available from
  the log.

### 5. Delve instances: child log partitions

- Each delve instance gets its own log partition under its campaign, using the same
  entry framing as campaign logs.
- The delve rules core's event stream (every action, roll, declaration, and timer
  outcome) *is* the delve log. One log feeds the client combat log, debug replay, and
  the death-review and friendly-fire record (Ch. 11, 13).
- Live delve state is held in memory by the delve runtime and does not materialize into
  the campaign redb store per round.
- On completion, a single `DelveConcluded` effect folds into the campaign log: XP,
  treasure shares, character deaths, henchman outcomes, logged notoriety events, and a
  reference to the delve log partition. Only this effect changes campaign state.
- Recovery: a delve interrupted by a crash resumes by replaying its delve log into the
  rules core. Delve logs are retained with the campaign for conduct review (see §8).

### 6. Campaign lifecycle

States, recorded in the global registry and mirrored in the campaign store:

`Provisioning → Enrollment → Active → Closing → Archived`

- **Provisioning:** campaign store and log created and seeded (§7). Not visible to
  players.
- **Enrollment:** roster is filled from premade parties and matchmaking. Entitlements
  are checked against the global store.
- **Active:** season turn cycle runs. Act transitions (Frontier → Realms → Crown) are
  campaign-state effects, not lifecycle states.
- **Closing:** final turn resolves and end-of-campaign scoring runs. Final placings
  and hall-of-fame entries are written to the global store through a cross-file
  intent.
- **Archived:** store and all log partitions (campaign + delves) are sealed, then
  moved to cold storage. Read-only access is retained for review and appeals.

Every transition is a logged effect. Transitions that touch both files use the
coordinator protocol (§3).

### 7. Campaign provisioning

Provisioning is an admin-tool operation (`acks-admin` successor) that produces a new,
self-contained campaign store from versioned library inputs:

- regional template (terrain, starting NPC domains and settlements, external threats,
  source of legitimacy, gatherings, regional rule)
- pinned ruleset version
- seed map from the global map library
- private-campaign rule overrides, if any
- campaign parameters (season length, season count, start date)

Provisioning runs as a cross-file intent: create and seed the campaign store and its
log, record provenance for every input, `fsync` the parent directories, then register
the campaign in the global store at `Provisioning`. A failed or abandoned provisioning leaves an orphan file that the
startup sweep removes.

Library inputs are immutable once versioned. Changing a template or ruleset creates
a new version and never alters campaigns already provisioned.

### 8. Log retention

- Campaign and delve logs are retained for at least the campaign's lifetime plus the
  moderation appeal window, because conduct review depends on them.
- Default: retain indefinitely in cold storage after archive. Storage cost per
  campaign is small.
- Segment trimming applies only to live-store recovery bookkeeping (snapshots
  supersede replay), never to the archival copy.

### 9. Typing: a Rust record layer is the only write path

- All records are Rust types with validated newtypes (e.g. ability score 3–18). They
  are checked on encode **and** decode, so bad bytes fail loudly on read.
- A record trait defines the primary key and secondary index keys, Btrieve
  key-segment style. The layer maintains secondary index tables in the same redb
  transaction as the primary write.
- Composite keys use redb tuple `Key` ordering, e.g. `(entity_kind, id)`.
- **Encoding rule:** any change to a key's or value's encoding, or to a key's
  `compare()`, requires a new `TypeName`, or a rebuild of affected stores from the
  log. redb only checks `TypeName` on open. It cannot detect a changed encoding or
  ordering under the same name, and the B-tree's order silently becomes invalid.
- Referential integrity is enforced in code. There are no declarative foreign keys.
- No raw console access to stores. Administrative inspection and edits go through
  tooling built on the same record layer: dump to JSON, then validated load. A lazy
  manual edit is still type-checked.

### 10. Engine ownership

- redb is dual-licensed Apache-2.0 / MIT and is primarily single-maintainer.
- The fork (`FredLiner/redb`) is pinned at **`v4.6.0`**, on a branch cut from the
  tag, unmodified initially. Divergence happens only for a concrete need. `master`
  tracks upstream for monitoring.
- Upstream is mid-way to an unreleased 5.0 with breaking encoding and API changes.
  Adopting 5.0 is a deliberate later decision, done by rebuilding stores from the
  log into the new format.
- Fork changes on `zemla` so far:
  - `96ea596`: cherry-pick of upstream `33e2204` (fail the storage when a panic
    unwinds past a write-buffer lock). At 4.6 such a panic double-panics during
    unwind and aborts the process, so one corrupt campaign store would take down
    every campaign. It was adapted to 4.6: CHANGELOG left unchanged, and the test
    uses the 4.6 `BackendIoError` alias.
  - `fe511e7`: a `compile_error!` guard in `src/lib.rs` that fails the build if
    `experimental-api-5` or `experimental-niches` is enabled. Those features switch
    to the unreleased 5.0 API and change on-disk `Option<T>` encodings. A guard
    was chosen over trimming the 240 upstream `cfg` sites, which would conflict with
    every future cherry-pick.
  - `3465d76`: the `justfile` recipes build with an explicit feature list
    (`logging,cache_metrics,uuid,chrono_v0_4`) instead of `--all-features`. The
    `--all` recipes exclude `redb-bench` and `redb-bench-compare`, which enable
    `experimental-api-5` through feature unification. Two unit-test imports used only
    under `experimental-api-5` are gated on it. `build_no_std`, `bench`, and
    `bench_containerized` need the experimental features and fail at the guard by
    design. Upstream's `ci.yml` is left unchanged. Its no_std, wasm, and 32-bit steps
    fail if GitHub Actions runs on the fork.
- The 5.0 byte-range locking rework (`2f4ef91`) is not needed, since multi-process
  access is not used.
- Branch layout on the fork: `zemla` is the default branch and the only line Zemla
  builds from. `master` stays a clean mirror of upstream, used for review and
  cherry-picking. Never merge between them, and never use GitHub's "Sync fork" on
  `zemla` (it would merge upstream 5.0 in).
- Workspace dependency: a **local path dependency** on a clone of the fork checked
  out at `zemla`. No git URLs in the codebase. Updating redb means pulling the
  clone deliberately and re-running the fork's tests.
- This ADR lives in the fork (`docs/zemla/`) until the Zemla server repo exists.
- The fork keeps its test suite and fuzz targets. **Fuzzing and crash testing run on
  Linux**, matching production. Windows-native tests are a development convenience.
- Upstream changelog is monitored for durability and corruption fixes, which are
  cherry-picked.
- The file format is under our control. No forced upgrades. Because stores are
  projections, even our own format changes are a rebuild.
- License compliance: retain both license files and copyright notices, and mark
  modified files.

## Consequences

Positive:
- Type guarantees hold on every write path, including admin.
- Campaign isolation is physical. Per-campaign backup, export, archive, delete, and
  restore are file operations, and corruption is contained to one campaign.
- Per-campaign writers run in parallel with no cross-campaign contention.
- Engine risk is bounded: stores are rebuildable, so redb can be replaced and the
  log replayed into the new engine.
- Log, outbox, and audit unify into one durable, ordered stream.
- The conduct system (death review, friendly fire, regent and transfer visibility,
  appeals) reads the same log the game writes. No separate audit pipeline.
- The campaign lifecycle maps directly to files: provision, run, seal, archive.
- Delve runtime load stays off the campaign store. Campaign logs stay season-paced.
- Handler logic separates cleanly into validate/decide (pure over state + overlay) and
  apply (deterministic transform).

Negative:
- We own the log file format, fsync discipline, torn-write recovery, and segment
  lifecycle, including directory-fsync discipline for file creates, renames, and
  deletes.
- We own the vendored engine and its upstream tracking.
- No ad-hoc SQL. Debugging and inspection need purpose-built tooling.
- Referential integrity and secondary indexes are hand-maintained.
- The overlay adds a consistency surface: validation and reads must see pending effects.
- Cross-file operations need the coordinator protocol rather than a single transaction.
- Provisioning and lifecycle tooling is required before the first campaign can run.
- The turn-close resolver emits a large batch in one pass, so batch size and fsync
  behavior at close need sizing.

## Alternatives considered

1. **SQLite with STRICT tables and a typed Rust boundary.**
   Rejected. Storage classes only, a thin type vocabulary, and the console bypasses
   Rust-side discipline.
2. **PostgreSQL** (native enums, domains, `timestamptz`, `jsonb`, RLS, sqlx compile-time
   checks).
   Rejected for campaign persistence. It has the strongest type system of the
   options, and as a hosted service the operational cost of running it is acceptable,
   so ops burden is *not* the reason. The reasons: `psql` still bypasses anything
   constraints don't cover, its relational strengths don't match the campaign access
   pattern, and it gives up the per-campaign file lifecycle (provision, seal, archive
   as file operations).
   **Revisit for the global store specifically.** Accounts, entitlements,
   matchmaking, and sanctions are the most relational part of the system. If any part
   of the system adopts SQL, it should be that one, not the campaign stores.
3. **Fully hand-rolled ISAM** (own B-tree, own crash safety).
   Rejected for the storage engine. The B-tree is tractable, but atomic multi-record
   commit, torn-write handling, crash-consistent indexes, and free-space management
   are where hand-rolled stores fail. The log (where custom ownership is required
   anyway) is kept in-house. The page store is not.
4. **LMDB via `heed`.**
   Retained as the known fallback. Same model as redb (CoW B-tree, single writer,
   MVCC, multiple named DBs per file), with over a decade of production use. Not
   chosen: C dependency, a fixed memory-map size that must be grown explicitly, and
   less idiomatic typing.
5. **Log stored as a redb table inside each store** (commit-atomic with the mutation).
   Rejected. The log is the dispatch layer's sequencing authority and must exist
   ahead of state application. Co-locating it in the store would also lose it with a
   corrupted file.
6. **Single global log.**
   Rejected. It serializes all campaigns through one fsync stream. Per-campaign
   partitions plus a global coordinator partition preserve parallelism.
7. **DuckDB, libSQL/Turso/Limbo.**
   Rejected. DuckDB is columnar/OLAP and poor for small OLTP writes. libSQL-family
   engines share SQLite's type model.

## Open Questions

- **Effect representation:** domain-level events (better for outbox and audit, apply
  logic per event) or concrete record writes (trivial apply, weaker audit semantics)?
- **Appeal window length:** sets the minimum hot-retention period before archive (§8).
- **Turn-close batch shape:** one `TurnClosed` entry containing all outcomes, or a
  bounded sequence of entries between begin and end markers? Affects entry size limits
  and partial-replay behavior.
- **Delve log promotion:** does a delve log stay a separate partition forever, or merge
  into the campaign archive at close?
- **Global store engine:** redb under the same record layer, or SQL for the
  relational global data (see Alternative 2)?
- **Read consistency:** do client reads go through the overlay, or is apply lag
  accepted given event push from the log?
- **Snapshot mechanism:** redb file copy at a quiesced watermark, or a logical export?
  How often?
- **Ruleset version pinning:** confirm a campaign never auto-upgrades its snapshotted
  ruleset, and define the explicit upgrade path if one is offered.
- **Apply-lane `Immediate` cadence:** interval or entry count between durable redb
  commits. This trades replay length after a crash against fsync load.
- **Fork commit trailers:** the fork's `AGENTS.md` asks for an `Assisted-by:` trailer,
  not `Co-authored-by:`. `96ea596` and `fe511e7` use `Co-Authored-By`. Decide whether
  to amend them, which needs a force-push of `zemla`.

Resolved since the previous draft:
- Re-verified at `v4.6.0`: Builder defaults are unchanged (1 GiB cache, quick-repair
  off for single-writer). The Windows try-lock path is the same, and now affects
  development only.
- `33e2204` cherry-picked and passing (`96ea596`).
- Fork test tooling works with the feature guard (`3465d76`). Format, clippy, the
  workspace build, and 697 tests pass on Windows with warnings denied. Not yet run:
  the Linux suite, and `cargo deny`.

## Sign-off Criteria Check

- [ ] Log entry framing and versioning specified
- [ ] Effect representation decided
- [ ] Overlay read semantics defined
- [ ] Cross-file coordinator protocol specified with failure-point matrix
- [ ] Snapshot and log-trim policy defined
- [x] redb fork branched at `v4.6.0` (`zemla`, default branch)
- [x] Panic-abort fix (`33e2204`) applied to the fork
- [x] Experimental 5.0 features blocked by compile guard
- [x] Fork test recipes use a feature list that excludes the experimental features
- [ ] Fork tests and fuzzing run on Linux
- [ ] redb configuration (quick-repair, cache size, durability cadence) encoded in the
      store manager
- [ ] Directory-fsync steps crash-tested on Linux (provisioning, segment create/seal,
      archive)
- [ ] Encoding/`TypeName` rule enforced in the record layer (test or lint)
- [ ] Admin dump/load tooling scoped
- [ ] Campaign lifecycle transitions and their effects specified
- [ ] Provisioning inputs (template, ruleset, seed map, overrides) versioned and specified
- [ ] Delve partition lifecycle and `DelveConcluded` contents specified
- [ ] Turn-close resolver output batch specified
- [ ] Archive and cold-storage procedure defined

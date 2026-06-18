# A shared Dialect seam: a generic write Store, but per-backend serving

The SQLite and PostgreSQL backends differ only in a small set of SQL-text
facts — positional-placeholder syntax (`?` vs `$N`), the intra-block
tie-breaker (`rowid` vs `ctid`), the column-type keyword each `SqlType` maps to
(INTEGER vs BIGINT, etc.), the monotonic-watermark upsert expression (`MAX` vs
`GREATEST`), and the undefined-table classification (driver message vs SQLSTATE
`42P01`). Before this change those facts were smeared across four adapters
(`SqliteStore`, `PostgresStore`, and the two serving backends): the placeholder
and tie-breaker appeared in four places, the `write_block` orchestration body
was written out twice, the paged range query was hand-rolled twice, and
`is_undefined_table` was copy-pasted verbatim into both `postgres.rs` and the
serving `backend.rs`.

We named those facts as a **Dialect** (`src/persistence/dialect.rs`): a small,
stateless trait with one adapter per backend (`SqliteDialect`, `PgDialect`).
The query-shaping free functions, the write **Store**, and the read serving
backends all consume the same Dialect, so the two sides can no longer drift on
a placeholder or tie-breaker.

The seam is applied **asymmetrically**, and that asymmetry is the decision worth
recording:

- **Write side — one generic.** `SqlStore<DB, D: Dialect>` owns the
  `write_block` / `last_block` / `replay` orchestration exactly once, generic
  over the sqlx `Database`, with the Dialect supplying the differing tokens.
  `SqliteStore` / `PostgresStore` remain as `pub type` aliases so the public API
  is unchanged; per-backend `connect` stays per-backend (pool tuning — WAL,
  synchronous, busy-timeout, single-writer — genuinely differs). This pays the
  sqlx generic trait-bound wall once, concentrated in a single impl, in exchange
  for the `write_block` body existing once instead of twice.

- **Read side — two structs, shared Dialect (NOT generic).** The serving
  backends keep their own structs and bodies and consume the same Dialect for
  the three points that genuinely share (`query_rows` placeholder/tie-breaker,
  watermark/undefined-table classification).

## Why the read side is deliberately *not* generic

The obvious symmetric move — a parallel `SqlServingBackend<DB, D>` — was
considered and rejected. The serving layer's catalog introspection is
structurally divergent, not token-divergent: SQLite reads `sqlite_master` +
`PRAGMA table_info`; PostgreSQL reads `information_schema`. These are different
queries, with different result columns, and different type-normalisation tables
(`list_tables`, `table_exists`, `table_columns`). A generic serving backend
would force the Dialect to emit whole, structurally-different queries (and own
their decode and normalisation), turning a small fact-bearing trait into a
god-trait and leaving the generic wrapper a hollow pass-through whose interface
equals its implementation. The deletion test confirms it: deleting such a
wrapper concentrates no complexity, because the complexity would already live in
the per-backend Dialect impls.

There are really **two seams** on the read side, and they partition the backends
differently: the token-level **Dialect** (shared with the write side) and a
**Catalog** seam (how a backend enumerates its own schema). A future
`information_schema`-family backend (MySQL, …) would join PostgreSQL on the
Catalog axis while still carrying its own Dialect. Merging Catalog into Dialect
to achieve write/read symmetry would entangle two seams that vary independently.
The write side has no Catalog concern at all — it creates tables, never lists
them — which is why only the write side is generic.

Don't re-propose making the serving layer generic over the Dialect for symmetry
with the Store unless the catalog introspection stops being structurally
divergent (e.g. all supported backends converge on `information_schema`), at
which point the Catalog becomes its own shareable seam — still distinct from the
Dialect.

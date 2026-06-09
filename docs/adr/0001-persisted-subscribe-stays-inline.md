# Persisted::subscribe stays an inline interpreter — no plan/execute split

`Persisted::subscribe` is a ~60-line orchestration method that composes three
Segments (Replay → Backfill → Live Tail) with the boundary arithmetic and
ordering invariants explained in comments. That shape pattern-matches to a
plan/execute refactor: extract a pure `plan(first_subscribe, last, tip) ->
SubscribePlan` whose fields encode the segment boundaries, test it directly,
and shrink `subscribe` to an interpreter.

We considered exactly that and rejected it. The extractable arithmetic is one
`+1` and one `> tip` — a pure plan module fails the deletion test (single
caller, near-zero conceptual mass), and its unit tests would be tautological
with its implementation. The invariants that actually carry risk are
I/O-ordering, which a pure plan cannot hold: subscribe to the live source
*before* querying the tip (so head events are buffered, not lost), and flip
the replay-once flag only *after* every fallible setup step (so a failed
subscribe doesn't strand stored history on retry). Those are already guarded
by tests through the `Collector` interface (`tests/persistence.rs`, e.g.
`failed_subscribe_does_not_consume_replay`).

Instead, the segment contract is named structurally: the private `Segments`
struct in `src/persistence/persisted.rs` owns the delivery order in
`into_stream()`, and the arithmetic stays at the construction site with its
comments. Don't re-propose extracting the orchestration into a pure plan
unless the boundary logic grows real mass (e.g. per-log identity for
overlap-and-dedup at the tip cut, deferred in PR #18).

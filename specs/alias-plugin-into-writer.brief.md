# Brief: alias plugin as a first-class writer participant

Status: idea only — not scheduled. Captures the architectural direction
surfaced while diagnosing the many-to-one byStoreesl convergence regression
(see `S:\Pricer\platform\link-service\rxdatasets-alias-regression-handoff.md`
for the immediate hotfix that landed in `DatasetWriter` instead).

## Motivation

Today the alias subsystem is a *consumer* of deltas, not a participant in
them:

- `AliasDatasetReader<T>` runs a background loop that wakes on
  `DatasetUpdatedNotification`, calls `WaitChangedNames`, re-resolves the
  changeset, and updates its alias↔ID maps **after** the delta is durable
  and visible to everyone else.
- The writer has no knowledge of alias factories. It cannot:
  - reject a write whose generated aliases would collide with an existing
    alias owned by a different live entity,
  - reject a single write that internally produces duplicate aliases,
  - present a consistent (entity + aliases) snapshot atomically,
  - drive its own version-bump signal as part of the same critical section
    that updates `_index`.
- The TOCTOU race fixed in `DatasetWriter` is a symptom: a downstream
  observer (the alias reader) is wired into a side-channel and has no
  contract with the writer about when its state has settled.

## Proposed shape

Make the alias system a registered plugin on the writer (analogous to the
existing snapshot/reporting plugins but with **veto + state mutation
rights**):

```
interface IAliasPlugin<T>
{
    // Phase 1 — vote: receive the proposed delta, return:
    //   - the aliases this delta would add / remove, and
    //   - veto reason if any (e.g. "alias 'X' would collide with live entity Y").
    // Plugin may read its own indices but MUST NOT mutate them yet.
    AliasVote Vote(ProposedDelta<T> delta, ImmutableDatasetState currentIndex);

    // Phase 2 — apply: called only after vote() returned no veto AND after
    // the delta is durable on disk. Plugin atomically updates its maps.
    // No I/O — pure in-memory state transition.
    void Commit(AppliedDelta<T> delta);
}
```

Writer pseudocode (`DatasetWriter.PutEntitiesAsync`):

```
1. Build deltaIndex from input stream (no I/O on plugin side).
2. For each registered IAliasPlugin: var vote = plugin.Vote(proposed).
   - If any veto: return InvalidArgument (whole transaction aborts, nothing
     stored, no notification fired).
3. _index = oldIndex.ApplyDelta(deltaIndex)            // in-memory swap
4. await _storageProvider.StoreDelta(...)              // durability
5. foreach plugin: plugin.Commit(applied)              // atomic in-memory update
6. await OnDeltaApplied(deltaIndex)                    // fire wakeups, change-queue, etc.
```

The notifications hub continues to publish for cross-process readers
(read-only replicas), but the in-process alias state is now updated
**before** any wakeup, by design.

## What this buys

1. **Alias collision validation at write time.** The TestServer's gRPC
   handlers already call `idValidator.CheckAliasPatterns(...)` against
   *pattern shape*, but cannot catch collisions against the current dataset
   state (e.g. trying to put a link whose `by_storeesl` alias is already
   owned by another live link). Today such a write succeeds and the alias
   reader silently overwrites — losing the previous owner without warning.
2. **No more TOCTOU race for in-process observers.** The race the hotfix
   patches around becomes structurally impossible: there is no separate
   wakeup path that can fire before in-memory state has settled.
3. **Atomic (entity + aliases) snapshot.** Readers querying `by_storeesl`
   immediately after a write see either the pre-state or the full post-state
   for the affected aliases, never a half-applied transient.
4. **Cleaner contract with the writer.** Plugins declare their
   participation; the writer drives them deterministically rather than
   relying on a publish-subscribe side-channel.
5. **Foundation for future participants.** History/audit, change-queue
   pre-filters, validation hooks, etc., can use the same vote/commit
   protocol.

## Non-goals / open questions

- Cross-process / read-only-replica alias readers stay on the existing
  subscription path; they cannot vote. The plugin model is for in-process
  writers (`DatasetReaderWriter<T>`).
- How to express "ordered plugin dependencies" if one plugin's vote depends
  on another's projected state. Most likely: forbid for v1; require plugins
  to be commutative.
- Locking. The writer already serializes deltas through `_semaphore`; the
  plugin protocol runs inside that critical section, so plugins do not need
  their own synchronization.
- Initialization. Plugins need a one-time `Hydrate(ImmutableDatasetState)`
  to build their initial indices, replacing today's full-scan
  `InitializeAndSync` in `AliasDatasetReader`.

## Migration sketch

1. Introduce `IAliasPlugin<T>` and writer wiring; keep the old
   subscription-driven `AliasDatasetReader` working in parallel.
2. Port one alias (`by_externalid` on `Store`) to the plugin model end-to-end
   as a proof.
3. Migrate the remaining aliases per DTO. Drop the legacy
   subscription path once empty.
4. Storage-provider `DatasetUpdatedNotification` keeps its current shape —
   it remains the cross-process signal for read-only replicas, and the
   writer's `OnDeltaApplied` becomes the in-process signal.

## Why not now

The immediate regression has a small targeted fix in `DatasetWriter` (move
`_index` swap before `StoreDelta`, revert on failure). That unblocks
link-service. The plugin model is a multi-week refactor that touches the
public surface of the alias subsystem and every consumer that registers an
alias reader — out of scope for the current incident.

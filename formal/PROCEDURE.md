# Formal review procedure: fiducia-lock-many-fencing

Canonical repository instructions: <https://github.com/ORESoftware/my-ai/blob/main/AGENTS.md>.

## Boundary

`formal/model.mjs` is an independent two-lock, two-client model of atomic lock-many acquisition and release, TTL expiry, network partition, grouped fenced writes, stale-leader rejection, and exact replay. It imports no production code and therefore remains a differential oracle rather than a shared-code tautology. The model explores every reachable state inside the explicit bounds in `formal/fm.toml`, then runs concrete adversarial witnesses.

## Required properties

1. **`atomic-lock-many-acquire`** — A grouped acquisition grants every lock with newer fences or grants none.
2. **`atomic-lock-many-release`** — A grouped release frees every lock or leaves the complete grant unchanged.
3. **`atomic-fenced-write`** — A protected multi-resource update commits every member or no member.
4. **`partition-retains-grant-until-expiry`** — Network loss cannot implicitly release the authority grant before TTL expiry.
5. **`monotonic-per-lock-fences`** — Each lock fencing token and persisted resource fence is monotonic.
6. **`stale-leader-rejected`** — A partitioned or superseded holder cannot write with an older fence.

## Refinement obligation

A behavioral change in a source repository, test plan, immutable source pin, or integration harness that touches this domain must keep the finite model green. Production implementations should consume JSON-lines action traces through the `--json-stdin` adapter and compare accepted/rejected outcomes plus the canonical final state. Native database, queue, provider, and concurrency tests remain mandatory.

## Bounds and nonclaims

The claim is a **finite exhaustive abstraction**, not a proof over unbounded production state. It does not prove real network timing, provider availability, cryptographic holder identity, or unbounded lock sets. Increasing a bound must not weaken an invariant, remove a witness, or normalize away a counterexample.

## Commands

```sh
node formal/model.mjs
printf '%s\n' '{"actions":[{"kind":"acquire-many","client":"a"},{"kind":"write-many","client":"a","value":1},{"kind":"partition","client":"a"},{"kind":"tick"},{"kind":"tick"},{"kind":"acquire-many","client":"b"},{"kind":"write-many","client":"a","value":2},{"kind":"write-many","client":"b","value":2}]}' | node formal/model.mjs --json-stdin
```

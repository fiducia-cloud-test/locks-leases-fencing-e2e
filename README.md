# fiducia-cloud-test/locks-leases-fencing-e2e

A destructive-style, real-process test-org harness for Fiducia locks, leases, and downstream fencing behavior.

## Automated scenario

The `DEN-1391 stale fencing real-process proof` workflow builds an exact public `fiducia-node` source commit with its exact routing and generated-interface siblings, then starts three durable Raft members on one disposable GitHub Actions host.

The scenario proves:

1. Holder A acquires a fenced lock and the reference downstream accepts token `N`.
2. A's TTL expires; holder B acquires the same key with token `N+1`.
3. The reference downstream accepts `N+1` and rejects the resumed holder A using `N`.
4. A cannot renew or release B's lock using the old token.
5. The current lock leader is killed; the new quorum preserves B and `N+1`.
6. The killed member rejoins and catches up without token regression.
7. A complete three-member restart preserves the committed holder/token.
8. The next transfer receives a strictly newer token.
9. A multi-key lock blocks an overlapping single-key acquire atomically, and the later transfer advances fencing.

The downstream reference is deliberately separate from Fiducia. It maintains the highest accepted fencing token and rejects every lower token, demonstrating the customer-side enforcement required by the managed-beta service contract.

## Immutable sources

| Source | Commit |
|---|---|
| `fiducia-cloud/fiducia-node.rs` | `4dbccfe5bcb0007bf9405537155b289dd1136c6a` |
| `fiducia-cloud/fiducia-routing.rs` | `c694bc5c58587bec12989a347e926c0040aacada` |
| `fiducia-cloud/fiducia-interfaces` | `6081bc3f3b7cbe0312870968b61acc38ca91c66a` |

The workflow checks out only public repositories using the test-org Actions token. No cross-organization PAT, production credential, customer identity, or production endpoint is required.

## Evidence boundary

A passing run is **synthetic test-org automation**, not production release evidence:

- all three Raft members share one GitHub Actions host, kernel, network, and scheduler failure domain;
- HTTP uses loopback without production TLS, mTLS, ingress, or NetworkPolicy;
- the reference downstream is an in-process test server rather than a customer database;
- no production image digest, cluster configuration, or independent reviewer is involved.

The bounded JSON evidence records source SHA, monotonic tokens, leader crash/restart events, downstream accept/reject history, and explicit limitations. The workflow scans it for credential-shaped material.

## Local execution

Materialize the exact sibling repositories so their paths match the node's Cargo path dependencies:

```text
source/
  fiducia-node.rs/
  fiducia-routing.rs/
  fiducia-interfaces/
```

Then build and run:

```bash
cargo build --locked --manifest-path source/fiducia-node.rs/Cargo.toml --bin fiducia-node
FIDUCIA_NODE_BIN="$PWD/source/fiducia-node.rs/target/debug/fiducia-node" \
FIDUCIA_NODE_SOURCE_COMMIT=4dbccfe5bcb0007bf9405537155b289dd1136c6a \
node scripts/stale-fencing-e2e.mjs
```

The complete executable contract is maintained in `test-plan.json` and `source-pins.json`.

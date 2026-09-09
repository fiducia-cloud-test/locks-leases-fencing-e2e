# Lock-many replay peer contract

Canonical repository instructions: <https://github.com/ORESoftware/my-ai/blob/main/AGENTS.md>.

`typespec/main.tsp` and `json-schema/contract.schema.json` are independently
authored peer authorities. Neither is generated from or subordinate to the
other. Generated JSON Schema B is retained only as comparison evidence.

The contract fixes the cross-runtime JSON-lines action vocabulary for the
independent lock-many formal model: acquisition, renewal, release, partition,
time progression, and grouped fenced writes. Native Fiducia implementations
must replay this vocabulary while retaining their own persistence and
concurrency tests.

CI uses the immutable `ORESoftware/typespec-json-schema-validator` action and
fails closed on declaration, enum, requiredness, shape, reference, corpus, or
bidirectional probe disagreement.

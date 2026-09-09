import "@typespec/json-schema";
using TypeSpec.JsonSchema;

namespace Fiducia.LockMany;

@id("LockManyActionKind")
enum LockManyActionKind {
  tick: "tick",
  acquireMany: "acquire-many",
  renewMany: "renew-many",
  releaseMany: "release-many",
  partition: "partition",
  writeMany: "write-many",
}

@id("LockManyAction")
model LockManyAction {
  kind: LockManyActionKind;
  client?: string;
  value?: int32;
}

@id("LockManyReplayRequest")
model LockManyReplayRequest {
  schemaVersion: "fiducia.lock-many.replay/v1";
  actions: LockManyAction[];
}

@id("LockManyReplayOutcome")
model LockManyReplayOutcome {
  accepted: boolean;
  reason: string;
}

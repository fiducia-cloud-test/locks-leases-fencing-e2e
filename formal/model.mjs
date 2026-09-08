#!/usr/bin/env node

import fs from 'node:fs';

const CLIENTS = ['a', 'b'];
const VALUES = [1, 2];
const LOCKS = 2;
const MAX_TIME = 4;
const MAX_FENCE = 3;
const TTL = 2;

function check(condition, message, context = undefined) {
  if (!condition) {
    throw new Error(`formal model violation: ${message}${context === undefined ? '' : `\n${JSON.stringify(context)}`}`);
  }
}

function initialState() {
  return {
    now: 0,
    owner: null,
    connected: false,
    expiresAt: 0,
    grantFences: [0, 0],
    lastFences: [0, 0],
    tokens: { a: [0, 0], b: [0, 0] },
    resourceFences: [0, 0],
    resourceValues: [0, 0],
  };
}

function clone(state) {
  return {
    ...state,
    grantFences: [...state.grantFences],
    lastFences: [...state.lastFences],
    tokens: { a: [...state.tokens.a], b: [...state.tokens.b] },
    resourceFences: [...state.resourceFences],
    resourceValues: [...state.resourceValues],
  };
}

function key(state) {
  return JSON.stringify([
    state.now,
    state.owner,
    state.connected,
    state.expiresAt,
    state.grantFences,
    state.lastFences,
    state.tokens.a,
    state.tokens.b,
    state.resourceFences,
    state.resourceValues,
  ]);
}

function active(state) {
  return state.owner !== null && state.connected && state.now < state.expiresAt;
}

function expire(input) {
  const state = clone(input);
  if (state.owner !== null && state.now >= state.expiresAt) {
    state.owner = null;
    state.connected = false;
    state.expiresAt = 0;
    state.grantFences = [0, 0];
  }
  return state;
}

function step(input, action) {
  const before = expire(input);
  const next = clone(before);
  let accepted = false;
  let reason = 'precondition';

  switch (action.kind) {
    case 'tick':
      if (next.now < MAX_TIME) {
        next.now += 1;
        Object.assign(next, expire(next));
        accepted = true;
        reason = 'advanced';
      }
      break;
    case 'acquire-many':
      if (CLIENTS.includes(action.client) && next.owner === null && next.lastFences.every((fence) => fence < MAX_FENCE)) {
        next.lastFences = next.lastFences.map((fence) => fence + 1);
        next.grantFences = [...next.lastFences];
        next.tokens[action.client] = [...next.grantFences];
        next.owner = action.client;
        next.connected = true;
        next.expiresAt = Math.min(MAX_TIME + TTL, next.now + TTL);
        accepted = true;
        reason = 'granted-atomically';
      }
      break;
    case 'renew-many':
      if (CLIENTS.includes(action.client) && active(next) && next.owner === action.client) {
        next.expiresAt = Math.min(MAX_TIME + TTL, next.expiresAt + 1);
        accepted = true;
        reason = 'renewed';
      }
      break;
    case 'release-many':
      if (
        CLIENTS.includes(action.client)
        && active(next)
        && next.owner === action.client
        && next.tokens[action.client].every((token, index) => token === next.grantFences[index])
      ) {
        next.owner = null;
        next.connected = false;
        next.expiresAt = 0;
        next.grantFences = [0, 0];
        accepted = true;
        reason = 'released-atomically';
      }
      break;
    case 'partition':
      if (CLIENTS.includes(action.client) && next.owner === action.client && next.connected) {
        next.connected = false;
        accepted = true;
        reason = 'partitioned';
      }
      break;
    case 'write-many': {
      if (CLIENTS.includes(action.client) && VALUES.includes(action.value)) {
        const tokens = next.tokens[action.client];
        const owns = active(next)
          && next.owner === action.client
          && tokens.every((token, index) => token === next.grantFences[index]);
        const fresh = tokens.every((token, index) => token > next.resourceFences[index]);
        const exactReplay = tokens.every(
          (token, index) => token === next.resourceFences[index] && action.value === next.resourceValues[index],
        );
        if (owns && (fresh || exactReplay)) {
          next.resourceFences = [...tokens];
          next.resourceValues = [action.value, action.value];
          accepted = true;
          reason = exactReplay ? 'replayed' : 'committed-atomically';
        } else if (tokens.some((token, index) => token < next.resourceFences[index])) {
          reason = 'stale-fence';
        } else if (tokens.some((token, index) => token === next.resourceFences[index] && action.value !== next.resourceValues[index])) {
          reason = 'ambiguous-token-reuse';
        }
      }
      break;
    }
    default:
      reason = 'unknown-action';
  }

  const transition = { input, before, action, next, accepted, reason };
  assertState(next);
  assertTransition(transition);
  return transition;
}

function assertState(state) {
  check(state.now >= 0 && state.now <= MAX_TIME, 'time bounded', state);
  check(state.lastFences.length === LOCKS && state.grantFences.length === LOCKS, 'two-lock domain retained', state);
  check(state.lastFences.every((fence) => fence >= 0 && fence <= MAX_FENCE), 'minted fences bounded', state);
  check(state.resourceFences.every((fence, index) => fence <= state.lastFences[index]), 'resource fences cannot exceed minted fences', state);
  check(state.resourceFences[0] === state.resourceFences[1], 'lock-many write is all-or-nothing', state);
  check(state.resourceValues[0] === state.resourceValues[1], 'lock-many payload is all-or-nothing', state);
  if (state.owner === null) {
    check(!state.connected && state.expiresAt === 0 && state.grantFences.every((fence) => fence === 0), 'free group has no grant metadata', state);
  } else {
    check(CLIENTS.includes(state.owner), 'known owner', state);
    check(state.grantFences.every((fence, index) => fence === state.lastFences[index] && fence > 0), 'group grant carries newest fences', state);
    check(state.tokens[state.owner].every((token, index) => token === state.grantFences[index]), 'owner tokens match group grant', state);
  }
}

function assertTransition({ input, before, action, next, accepted }) {
  check(next.lastFences.every((fence, index) => fence >= input.lastFences[index]), 'fences never regress', { input, action, next });
  check(next.resourceFences.every((fence, index) => fence >= input.resourceFences[index]), 'persisted fences never regress', { input, action, next });
  if (action.kind === 'acquire-many' && accepted) {
    check(next.lastFences.every((fence, index) => fence === before.lastFences[index] + 1), 'acquire-many mints all fences together', { before, next });
  }
  if (action.kind === 'renew-many' && accepted) {
    check(next.grantFences.every((fence, index) => fence === before.grantFences[index]), 'renew-many preserves every fence', { before, next });
  }
  if (action.kind === 'partition' && accepted) {
    check(next.owner === before.owner && next.expiresAt === before.expiresAt, 'partition does not release or extend lease', { before, next });
  }
  if (action.kind === 'write-many' && !accepted) {
    check(
      next.resourceFences.every((fence, index) => fence === before.resourceFences[index])
        && next.resourceValues.every((value, index) => value === before.resourceValues[index]),
      'rejected grouped write has no partial effect',
      { before, action, next },
    );
  }
}

function actions() {
  const result = [{ kind: 'tick' }];
  for (const client of CLIENTS) {
    result.push(
      { kind: 'acquire-many', client },
      { kind: 'renew-many', client },
      { kind: 'release-many', client },
      { kind: 'partition', client },
      ...VALUES.map((value) => ({ kind: 'write-many', client, value })),
    );
  }
  return result;
}

function witnesses() {
  let state = initialState();
  const a = step(state, { kind: 'acquire-many', client: 'a' });
  check(a.accepted && a.next.grantFences[0] === 1 && a.next.grantFences[1] === 1, 'atomic initial grant witness');
  const firstWrite = step(a.next, { kind: 'write-many', client: 'a', value: 1 });
  check(firstWrite.accepted, 'initial grouped write witness');
  const partitioned = step(firstWrite.next, { kind: 'partition', client: 'a' });
  check(partitioned.accepted, 'partition witness');
  check(!step(partitioned.next, { kind: 'acquire-many', client: 'b' }).accepted, 'grant retained until TTL witness');
  state = step(step(partitioned.next, { kind: 'tick' }).next, { kind: 'tick' }).next;
  const b = step(state, { kind: 'acquire-many', client: 'b' });
  check(b.accepted && b.next.grantFences.every((fence) => fence === 2), 'successor fence witness');
  check(!step(b.next, { kind: 'write-many', client: 'a', value: 2 }).accepted, 'stale grouped write witness');
  const committed = step(b.next, { kind: 'write-many', client: 'b', value: 2 });
  check(committed.accepted && committed.next.resourceFences.every((fence) => fence === 2), 'atomic successor write witness');
}

function explore() {
  witnesses();
  const start = initialState();
  const seen = new Map([[key(start), start]]);
  const queue = [start];
  let cursor = 0;
  let transitions = 0;
  let accepted = 0;
  while (cursor < queue.length) {
    const state = queue[cursor++];
    assertState(state);
    for (const action of actions()) {
      const transition = step(state, action);
      transitions += 1;
      if (transition.accepted) {
        accepted += 1;
        const stateKey = key(transition.next);
        if (!seen.has(stateKey)) {
          seen.set(stateKey, transition.next);
          queue.push(transition.next);
        }
      }
    }
  }
  console.log(JSON.stringify({
    model: 'fiducia-cloud-test/lock-many-fencing-v1',
    claim: 'finite-exhaustive-abstraction',
    states: seen.size,
    transitions,
    accepted,
    rejected: transitions - accepted,
    invariants: [
      'atomic-lock-many-acquire',
      'atomic-lock-many-release',
      'atomic-fenced-write',
      'partition-retains-grant-until-expiry',
      'monotonic-per-lock-fences',
      'stale-leader-rejected',
    ],
  }));
}

function replay(document) {
  check(document && Array.isArray(document.actions), 'actions array required', document);
  let state = initialState();
  const outcomes = [];
  for (const action of document.actions) {
    const transition = step(state, action);
    outcomes.push({ accepted: transition.accepted, reason: transition.reason });
    state = transition.next;
  }
  return { ok: true, state, outcomes };
}

if (process.argv.includes('--json-stdin')) {
  for (const line of fs.readFileSync(0, 'utf8').split(/\r?\n/).filter((value) => value.trim() !== '')) {
    try {
      console.log(JSON.stringify(replay(JSON.parse(line))));
    } catch (error) {
      console.log(JSON.stringify({ ok: false, error: String(error?.message ?? error) }));
      process.exitCode = 1;
    }
  }
} else {
  explore();
}

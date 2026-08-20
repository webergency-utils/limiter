# @webergency-utils/limiter

Unopinionated promise limiters for TypeScript and Node.js. Combine concurrency, rate, interval, token-bucket, capacity, and debounce strategies on a shared FIFO, LIFO, or priority queue.

[![npm version](https://img.shields.io/npm/v/%40webergency-utils%2Flimiter)](https://www.npmjs.com/package/@webergency-utils/limiter)
[![License](https://img.shields.io/npm/l/%40webergency-utils%2Flimiter)](https://www.npmjs.com/package/@webergency-utils/limiter)
[![Maintenance](https://img.shields.io/badge/maintenance-active-brightgreen.svg)](#maintenance)
[![dependencies](https://img.shields.io/badge/dependencies-1-brightgreen.svg)](https://www.npmjs.com/package/@webergency-utils/limiter?activeTab=dependencies)
[![npm downloads](https://img.shields.io/npm/dm/%40webergency-utils%2Flimiter)](https://www.npmjs.com/package/@webergency-utils/limiter)<br>
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/webergency-utils/limiter/badge)](https://securityscorecards.dev/viewer/?uri=github.com/webergency-utils/limiter)
[![codecov](https://codecov.io/gh/webergency-utils/limiter/graph/badge.svg?token=)](https://codecov.io/gh/webergency-utils/limiter)
[![CI](https://github.com/webergency-utils/limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/webergency-utils/limiter/actions/workflows/ci.yml)
[![CodeQL](https://github.com/webergency-utils/limiter/actions/workflows/codeql.yml/badge.svg)](https://github.com/webergency-utils/limiter/actions/workflows/codeql.yml)

## TL;DR

```typescript
import Limiter from '@webergency-utils/limiter';

const limiter = new Limiter({
  concurrency: 2,
  rate: { limit: 10, window: 1000 }
});

const result = await limiter.execute(async () => fetch('/api'));
await limiter.onIdle();
```

## Installation & Setup

Install the package via npm:

```bash
npm install @webergency-utils/limiter
```

This package supports both ES Modules (ESM) and CommonJS (CJS). It depends on `@webergency-utils/heap` for priority queues; npm installs that dependency automatically. No peer dependencies, configuration files, or environment variables are required.

## Architecture & Internals

Each limiter is a queue plus one or more **gates**. `execute()` enqueues a task, then a pump starts the next ready task only when every gate can acquire the task's `difficulty` (default `1`). Time-based gates (`RateGate`, `IntervalGate`, `TokenBucketGate`) wait and reschedule the pump; `CapacityGate` is admission control and rejects immediately when the queue would exceed capacity.

- **Queue order:** `fifo` (default), `lifo`, or `priority`. Priority mode stores tasks in `@webergency-utils/heap` and sorts by `compare` (default: higher `priority` first, then insertion sequence).
- **Scheduling:** The pump never starts work synchronously inside `execute()`. It runs on `process.nextTick` when available, otherwise `queueMicrotask`, and uses `setTimeout` when a gate or `delay` says the next task is not ready yet.
- **Retries:** Failed tasks retry on the same execution slot. Delay is `retryDelay * retryBackoff ** (attempt - 1)`.
- **Combinator:** `Limiter` can attach several gates at once. If a later gate denies acquire, earlier gates are released so partial acquisition cannot leak slots.
- **Runtime:** Node.js and other environments with `setTimeout` / `AbortSignal`. ESM and CJS builds are published from `dist/`.

## Glossary

- **`Limiter`**: Combinator that can attach concurrency, rate, interval, token-bucket, and capacity gates together.
- **`BaseLimiter`**: Shared queue, events, `execute()`, `wrap()`, pause/resume, and idle handling.
- **`difficulty`**: Cost of a task. Concurrency and capacity treat it as slot usage; rate and token-bucket treat it as tokens consumed.
- **Gate**: Pluggable acquire/release policy (`ConcurrencyGate`, `RateGate`, `IntervalGate`, `TokenBucketGate`, `CapacityGate`).
- **`ExecuteHandle`**: A `Promise` with `.cancel()` for queued work.
- **`TaskAttrs`**: Per-task overrides for priority, delay, timeout, retries, `AbortSignal`, and related fields.

## API Reference

### `Limiter` (Class)

Default export. Builds a `BaseLimiter` from optional strategy options. With no strategies it uses unlimited concurrency.

```typescript
import Limiter from '@webergency-utils/limiter';
// or: import { Limiter } from '@webergency-utils/limiter';

new Limiter(options?: LimiterOptions)
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `options.concurrency` | `number \| ConcurrencyLimiterOptions` | unset | Max concurrent difficulty. A number is treated as `max`. |
| `options.rate` | `RateLimiterOptions` | unset | Sliding or fixed window rate limit. |
| `options.interval` | `number \| IntervalLimiterOptions` | unset | Minimum milliseconds between task starts. A number is treated as `interval`. |
| `options.tokenBucket` | `TokenBucketLimiterOptions` | unset | Token-bucket gate. |
| `options.capacity` | `number \| CapacityLimiterOptions` | unset | Max queued+running difficulty. Excess `execute()` calls reject with `LimiterRejectedError`. |
| `options.order` | `QueueOrder` | `'fifo'` | `'fifo'`, `'lifo'`, or `'priority'`. |
| `options.compare` | `TaskCompare` | higher priority, then sequence | Used only when `order` is `'priority'`. |
| plus `TaskAttrs` | | | Constructor defaults applied to every `execute()` unless overridden. |

**Code Example**

```typescript
const limiter = new Limiter({
  concurrency: { max: 2 },
  interval: { interval: 20 },
  rate: { limit: 10, window: 1000, windowType: 'sliding' },
  tokenBucket: { capacity: 5, refillAmount: 5, refillInterval: 1000 },
  capacity: { capacity: 20 },
  order: 'priority'
});

await limiter.execute(async () => 'ok', { priority: 10 });
```

---

### `ConcurrencyLimiter` (Class)

Limits how much difficulty may run at once.

```typescript
new ConcurrencyLimiter({ max: number } & SharedLimiterOptions)
```

**Throws:** `RangeError` if `max` is not greater than `0`.

```typescript
import { ConcurrencyLimiter } from '@webergency-utils/limiter';

const limiter = new ConcurrencyLimiter({ max: 2 });
const results = await Promise.all([
  limiter.execute(async () => 1),
  limiter.execute(async () => 2),
  limiter.execute(async () => 3)
]);
```

### `RateLimiter` (Class)

Allows at most `limit` difficulty per `window` milliseconds.

```typescript
new RateLimiter({
  limit: number
  window: number
  windowType?: 'fixed' | 'sliding'
} & SharedLimiterOptions)
```

- `windowType` defaults to `'sliding'`.
- **Throws:** `RangeError` if `limit` or `window` is not greater than `0`.

```typescript
import { RateLimiter } from '@webergency-utils/limiter';

const limiter = new RateLimiter({ limit: 3, window: 1000, windowType: 'sliding' });
await limiter.execute(async () => fetch('/api'), { difficulty: 2 });
```

### `IntervalLimiter` (Class)

Ensures at least `interval` milliseconds between task starts.

```typescript
new IntervalLimiter({ interval: number } & SharedLimiterOptions)
```

**Throws:** `RangeError` if `interval` is less than `0`.

```typescript
import { IntervalLimiter } from '@webergency-utils/limiter';

const limiter = new IntervalLimiter({ interval: 100 });
await Promise.all([
  limiter.execute(async () => ping()),
  limiter.execute(async () => ping())
]);
```

### `TokenBucketLimiter` (Class)

Consumes `difficulty` tokens per start; tokens refill by `refillAmount` every `refillInterval` milliseconds, capped at `capacity`.

```typescript
new TokenBucketLimiter({
  capacity: number
  refillAmount: number
  refillInterval: number
  initialTokens?: number
} & SharedLimiterOptions)
```

- `initialTokens` defaults to `capacity`.
- **Throws:** `RangeError` if `capacity`, `refillAmount`, or `refillInterval` is not greater than `0`.

```typescript
import { TokenBucketLimiter } from '@webergency-utils/limiter';

const limiter = new TokenBucketLimiter({
  capacity: 3,
  refillAmount: 3,
  refillInterval: 1000,
  initialTokens: 3
});
```

### `CapacityLimiter` (Class)

Rejects new work when queued plus running difficulty would exceed `capacity`. Unlike other strategies, overflow does not wait.

```typescript
new CapacityLimiter({ capacity: number } & SharedLimiterOptions)
```

**Throws:** `RangeError` if `capacity` is not greater than `0`.

**Rejected with:** `LimiterRejectedError` when `reserve(difficulty)` fails.

```typescript
import { CapacityLimiter, LimiterRejectedError } from '@webergency-utils/limiter';

const limiter = new CapacityLimiter({ capacity: 2 });
const a = limiter.execute(async () => work(), { difficulty: 1 });
const b = limiter.execute(async () => work(), { difficulty: 1 });
await limiter.execute(async () => work()).catch((error) => {
  if (error instanceof LimiterRejectedError) {
    // queue is full
  }
});
```

### `Debounce` (Class)

Collapses `execute()` calls that share the same `key` (default `''`).

```typescript
new Debounce({
  wait: number
  leading?: boolean
  trailing?: boolean
} & SharedLimiterOptions)
```

| Option | Default | Description |
| :--- | :--- | :--- |
| `wait` | required | Quiet period in milliseconds before a trailing flush. |
| `leading` | `false` | Run the first call immediately when no key is in flight. |
| `trailing` | `true` | Run the latest pending call after `wait`. |

**Throws:** `RangeError` if `wait < 0`, or if both `leading` and `trailing` are `false`.

A superseded trailing call rejects with an `Error` whose `name` is `'LimiterDebouncedError'`.

```typescript
import { Debounce } from '@webergency-utils/limiter';

const debounce = new Debounce({ wait: 30 });
const first = debounce.execute(async () => 1);
const second = debounce.execute(async () => 2);
await second; // 2; first rejects as LimiterDebouncedError
```

---

### `BaseLimiter` (Class)

Shared implementation for all limiter classes.

```typescript
new BaseLimiter(
  gates: LimiterGate[],
  options?: SharedLimiterOptions,
  capacityGate?: CapacityGate
)
```

#### Properties

| Property | Type | Description |
| :--- | :--- | :--- |
| `pending` | `number` | Queued tasks. |
| `active` | `number` | Running tasks. |
| `size` | `number` | `pending + active`. |
| `paused` | `boolean` | Whether the pump is paused. |
| `pendingDifficulty` | `number` | Sum of queued difficulty. |
| `activeDifficulty` | `number` | Sum of running difficulty. |

#### `execute(fn, attrs?)`

Enqueues `fn` and returns an `ExecuteHandle`. Does not start the function synchronously.

**Parameters**

- `fn` (`() => T | Promise<T>`) — work to run.
- `attrs` (`TaskAttrs`, optional) — per-task overrides.

**Returns:** `ExecuteHandle<Awaited<T>>` — a promise with `cancel(): boolean`. `cancel()` returns `true` only while the task is still queued.

**Throws / rejects**

- `RangeError` if `difficulty` is not greater than `0`.
- `LimiterRejectedError` if a capacity gate refuses the task.
- `LimiterCancelledError` on `cancel()`, `clear()`, or abort.
- `LimiterTimeoutError` when `timeout` or `deadline` is reached.
- The original error when `fn` fails and retries are exhausted.

```typescript
const handle = limiter.execute(async () => 1, { priority: 5, timeout: 1000 });
handle.cancel();
```

#### `wrap(fn, defaultAttrs?)`

Returns a function that runs `fn(...args)` through `execute()`.

```typescript
const add = limiter.wrap(async (a: number, b: number) => a + b);
await add(2, 3); // 5
```

#### `pause()` / `resume()`

`pause()` stops starting new tasks. `resume()` continues the pump. Both return `this` and emit `'pause'` / `'resume'` only on state change.

#### `clear()`

Cancels every queued task with `LimiterCancelledError`. Running tasks are not cancelled.

#### `onIdle()`

Resolves when `size === 0`. Resolves immediately if the limiter is already idle.

#### `on(event, handler)` / `off(event, handler)`

Subscribe or unsubscribe. Events: `'add'`, `'start'`, `'success'`, `'error'`, `'settle'`, `'reject'`, `'idle'`, `'empty'`, `'pause'`, `'resume'`. Handlers receive an optional `TaskSnapshot`.

```typescript
limiter.on('idle', () => console.log('drained'));
limiter.off('idle', handler);
```

---

### Gates

Gates implement `LimiterGate`:

```typescript
interface LimiterGate {
  tryAcquire(difficulty: number): boolean
  release(difficulty: number): void
  nextAvailableAt(difficulty: number): number | null
  reset?(): void
}
```

`nextAvailableAt` returns a timestamp when the pump should wake, or `null` when the gate cannot suggest a time (for example concurrency, which waits for a `release()`).

| Class | Constructor | Notes |
| :--- | :--- | :--- |
| `ConcurrencyGate` | `(max: number)` | `active` / `max` getters. |
| `RateGate` | `(limit, window, windowType?: 'fixed' \| 'sliding')` | Default window type is `'sliding'`. |
| `IntervalGate` | `(interval: number)` | Spaces starts by `interval` ms. |
| `TokenBucketGate` | `(capacity, refillAmount, refillInterval, initialTokens?)` | `tokens` getter refills first. |
| `CapacityGate` | `(capacity: number)` | `reserve` / `unreserve` for admission; `tryAcquire` always succeeds. `queued` / `capacity` getters. |

All listed constructors throw `RangeError` on invalid numeric options (same rules as the matching limiter classes). `reset()` clears gate state.

```typescript
import { BaseLimiter, ConcurrencyGate, RateGate } from '@webergency-utils/limiter';

const limiter = new BaseLimiter([
  new ConcurrencyGate(2),
  new RateGate(10, 1000, 'sliding')
]);
```

---

### Errors

All limiter errors extend `LimiterError`.

| Class | Default message | When |
| :--- | :--- | :--- |
| `LimiterError` | caller-provided | Base class. |
| `LimiterTimeoutError` | `'Limiter task timed out'` | `timeout` or `deadline` reached. |
| `LimiterCancelledError` | `'Limiter task cancelled'` | `cancel()`, `clear()`, or abort. `clear()` uses `'Limiter queue cleared'`; abort uses `'Limiter task aborted'`. |
| `LimiterRejectedError` | `'Limiter rejected task'` | Capacity exceeded. The rejected handle uses `'Limiter capacity exceeded'`. |

Debounce also rejects superseded calls with `Error` `{ name: 'LimiterDebouncedError', message: 'Debounced' }`. That name is not an exported class.

---

### Types

#### `TaskAttrs`

Per-task or constructor defaults:

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `priority` | `number` | `1` | Sort key in priority mode. Higher runs first unless `compare` is customized. |
| `difficulty` | `number` | `1` | Must be `> 0`. |
| `timeout` | `number` | unset | Milliseconds from enqueue. `0` times out immediately. |
| `deadline` | `Date` | unset | Absolute timeout. The earlier of `timeout` and `deadline` wins. |
| `delay` | `number` | `0` | Milliseconds to wait before the task is eligible to start. |
| `retries` | `number` | `0` | Extra attempts after failure. |
| `retryDelay` | `number` | `0` | Base delay between retries, in milliseconds. |
| `retryBackoff` | `number` | `1` | Multiplier: `retryDelay * retryBackoff ** (attempt - 1)`. |
| `retryIf` | `(error, attempt) => boolean` | `() => true` | Return `false` to stop retrying. `attempt` is 1-based. |
| `key` | `string` | unset | Stored on `TaskSnapshot`; debounce coalesces by this value (default `''`). |
| `signal` | `AbortSignal` | unset | Aborts queued tasks with `LimiterCancelledError`. |

#### `ExecuteHandle<T>`

```typescript
type ExecuteHandle<T> = Promise<T> & { cancel: () => boolean }
```

#### `TaskSnapshot`

```typescript
type TaskSnapshot = {
  priority    : number
  difficulty  : number
  sequence    : number
  enqueuedAt  : number
  key?        : string
}
```

#### Other aliases

- `QueueOrder`: `'fifo' | 'lifo' | 'priority'`
- `LimiterEvent`: `'add' | 'start' | 'success' | 'error' | 'settle' | 'reject' | 'idle' | 'empty' | 'pause' | 'resume'`
- `LimiterEventHandler`: `(task?: TaskSnapshot) => void`
- `TaskCompare`: `(a: TaskSnapshot, b: TaskSnapshot) => number`
- `RetryIf`: `(error: unknown, attempt: number) => boolean`
- `RateWindowType`: `'fixed' | 'sliding'`
- `SharedLimiterOptions`: `TaskAttrs` plus `order?` and `compare?`

Strategy option types (`ConcurrencyLimiterOptions`, `RateLimiterOptions`, `IntervalLimiterOptions`, `TokenBucketLimiterOptions`, `CapacityLimiterOptions`, `DebounceOptions`, `LimiterOptions`) match the constructors above.

## Troubleshooting

### Excess `execute()` calls reject instead of waiting

`capacity` is admission control. When queued plus running difficulty would exceed the cap, `execute()` rejects with `LimiterRejectedError`. Use `concurrency`, `rate`, `interval`, or `tokenBucket` when overflow should wait.

### Debounce constructor throws `RangeError`

`wait` must be `>= 0`, and at least one of `leading` or `trailing` must be `true`.

### `cancel()` returns `false`

`cancel()` only affects queued tasks. Running work, already settled handles, and leading debounce calls that already started cannot be cancelled this way.

## Maintenance

This package is actively maintained.

Bug reports and pull requests are welcome. Security issues and critical
regressions are prioritized. New features are considered when they align
with the package's existing scope.

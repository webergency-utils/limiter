const { FuzzedDataProvider } = require('@jazzer.js/core');
const lib = require('./dist/index.cjs');

const Limiter = lib.default || lib.Limiter;
const {
  BaseLimiter,
  CapacityGate,
  CapacityLimiter,
  ConcurrencyGate,
  ConcurrencyLimiter,
  Debounce,
  IntervalGate,
  IntervalLimiter,
  LimiterCancelledError,
  LimiterError,
  LimiterRejectedError,
  LimiterTimeoutError,
  RateGate,
  RateLimiter,
  TokenBucketGate,
  TokenBucketLimiter
} = lib;

const EVENTS = [
  'add', 'start', 'success', 'error', 'settle',
  'reject', 'idle', 'empty', 'pause', 'resume'
];

function positive(provider, min, max) {
  const value = provider.consumeNumber();
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, Math.abs(value) || min));
}

function optionalPositive(provider, min, max) {
  if (!provider.consumeBoolean()) return undefined;
  return positive(provider, min, max);
}

function sharedOptions(provider) {
  const orders = ['fifo', 'lifo', 'priority'];
  const options = {
    order: provider.pickValue(orders)
  };

  if (provider.consumeBoolean()) {
    options.priority = provider.consumeIntegralInRange(0, 20);
  }
  if (provider.consumeBoolean()) {
    options.difficulty = positive(provider, 1, 5);
  }
  if (provider.consumeBoolean()) {
    options.timeout = provider.consumeIntegralInRange(1, 50);
  }
  if (provider.consumeBoolean()) {
    options.delay = provider.consumeIntegralInRange(0, 10);
  }
  if (provider.consumeBoolean()) {
    options.retries = provider.consumeIntegralInRange(0, 2);
    options.retryDelay = provider.consumeIntegralInRange(0, 5);
    options.retryBackoff = positive(provider, 1, 3);
  }
  if (provider.consumeBoolean()) {
    options.key = provider.consumeString(8);
  }

  return options;
}

function taskAttrs(provider) {
  const attrs = {};

  if (provider.consumeBoolean()) {
    attrs.priority = provider.consumeIntegralInRange(0, 20);
  }
  if (provider.consumeBoolean()) {
    attrs.difficulty = positive(provider, 1, 4);
  }
  if (provider.consumeBoolean()) {
    attrs.timeout = provider.consumeIntegralInRange(1, 40);
  }
  if (provider.consumeBoolean()) {
    attrs.delay = provider.consumeIntegralInRange(0, 8);
  }
  if (provider.consumeBoolean()) {
    attrs.retries = provider.consumeIntegralInRange(0, 2);
  }
  if (provider.consumeBoolean()) {
    attrs.key = provider.consumeString(6);
  }
  if (provider.consumeBoolean()) {
    const controller = new AbortController();
    attrs.signal = controller.signal;
    if (provider.consumeBoolean()) {
      controller.abort();
    }
  }

  return attrs;
}

function swallow(promise) {
  if (promise && typeof promise.then === 'function') {
    promise.catch(() => {});
  }
  return promise;
}

function createLimiter(provider) {
  const kind = provider.consumeIntegralInRange(0, 6);
  const shared = sharedOptions(provider);

  switch (kind) {
    case 0:
      return new ConcurrencyLimiter({ max: positive(provider, 1, 8), ...shared });
    case 1:
      return new RateLimiter({
        limit: positive(provider, 1, 8),
        window: positive(provider, 5, 40),
        windowType: provider.pickValue(['fixed', 'sliding']),
        ...shared
      });
    case 2:
      return new IntervalLimiter({ interval: provider.consumeIntegralInRange(0, 20), ...shared });
    case 3:
      return new TokenBucketLimiter({
        capacity: positive(provider, 1, 8),
        refillAmount: positive(provider, 1, 4),
        refillInterval: positive(provider, 5, 30),
        initialTokens: optionalPositive(provider, 0, 8),
        ...shared
      });
    case 4:
      return new CapacityLimiter({ capacity: positive(provider, 1, 8), ...shared });
    case 5:
      return new Debounce({
        wait: provider.consumeIntegralInRange(0, 20),
        leading: provider.consumeBoolean(),
        trailing: provider.consumeBoolean() || true,
        ...shared
      });
    default:
      return new Limiter({
        concurrency: provider.consumeBoolean() ? positive(provider, 1, 6) : undefined,
        interval: provider.consumeBoolean() ? provider.consumeIntegralInRange(0, 15) : undefined,
        rate: provider.consumeBoolean()
          ? {
              limit: positive(provider, 1, 6),
              window: positive(provider, 5, 30),
              windowType: provider.pickValue(['fixed', 'sliding'])
            }
          : undefined,
        tokenBucket: provider.consumeBoolean()
          ? {
              capacity: positive(provider, 1, 6),
              refillAmount: positive(provider, 1, 3),
              refillInterval: positive(provider, 5, 25)
            }
          : undefined,
        capacity: provider.consumeBoolean() ? positive(provider, 1, 8) : undefined,
        ...shared
      });
  }
}

function fuzzGates(provider) {
  const max = positive(provider, 1, 10);
  const concurrency = new ConcurrencyGate(max);
  concurrency.tryAcquire(positive(provider, 1, 5));
  concurrency.active;
  concurrency.max;
  concurrency.nextAvailableAt(1);
  concurrency.release(positive(provider, 1, 5));
  concurrency.reset();

  const rate = new RateGate(
    positive(provider, 1, 8),
    positive(provider, 5, 40),
    provider.pickValue(['fixed', 'sliding'])
  );
  rate.tryAcquire(positive(provider, 1, 4));
  rate.nextAvailableAt(positive(provider, 1, 4));
  rate.release(1);
  rate.reset();

  const interval = new IntervalGate(provider.consumeIntegralInRange(0, 30));
  interval.tryAcquire(1);
  interval.nextAvailableAt(1);
  interval.release(1);
  interval.reset();

  const bucket = new TokenBucketGate(
    positive(provider, 1, 8),
    positive(provider, 1, 4),
    positive(provider, 5, 30),
    optionalPositive(provider, 0, 8)
  );
  bucket.tokens;
  bucket.tryAcquire(positive(provider, 1, 4));
  bucket.nextAvailableAt(positive(provider, 1, 4));
  bucket.release(1);
  bucket.reset();

  const capacity = new CapacityGate(positive(provider, 1, 8));
  capacity.reserve(positive(provider, 1, 4));
  capacity.queued;
  capacity.capacity;
  capacity.tryAcquire(1);
  capacity.nextAvailableAt(1);
  capacity.unreserve(positive(provider, 1, 4));
  capacity.release(1);
  capacity.reset();

  const gates = [concurrency, rate];
  const composed = new BaseLimiter(gates, sharedOptions(provider), capacity);
  swallow(composed.execute(() => provider.consumeString(8), taskAttrs(provider)));
  composed.size;
  composed.clear();
}

function fuzzErrors() {
  new LimiterError('fuzz');
  new LimiterTimeoutError();
  new LimiterCancelledError();
  new LimiterRejectedError();
}

module.exports.fuzz = function (data) {
  try {
    const provider = new FuzzedDataProvider(data);

    fuzzErrors();
    fuzzGates(provider);

    const limiter = createLimiter(provider);
    const handler = () => {};
    const event = provider.pickValue(EVENTS);
    limiter.on(event, handler);

    const operations = provider.consumeIntegralInRange(1, 12);
    const handles = [];

    for (let i = 0; i < operations; i++) {
      const op = provider.consumeIntegralInRange(0, 8);

      switch (op) {
        case 0: {
          const shouldThrow = provider.consumeBoolean();
          const handle = limiter.execute(() => {
            if (shouldThrow) throw new Error('fuzz');
            return provider.consumeString(8);
          }, taskAttrs(provider));
          swallow(handle);
          handles.push(handle);
          break;
        }
        case 1: {
          const wrapped = limiter.wrap((a, b) => a + b);
          swallow(wrapped(provider.consumeIntegralInRange(0, 10), provider.consumeIntegralInRange(0, 10)));
          break;
        }
        case 2:
          limiter.pause();
          limiter.paused;
          break;
        case 3:
          limiter.resume();
          break;
        case 4:
          if (handles.length) {
            const handle = handles[provider.consumeIntegralInRange(0, handles.length - 1)];
            if (handle && typeof handle.cancel === 'function') {
              handle.cancel();
            }
          }
          break;
        case 5:
          limiter.clear();
          break;
        case 6:
          swallow(limiter.onIdle());
          break;
        case 7:
          limiter.pending;
          limiter.active;
          limiter.size;
          limiter.pendingDifficulty;
          limiter.activeDifficulty;
          break;
        case 8:
          limiter.off(event, handler);
          limiter.on(event, handler);
          break;
      }
    }

    limiter.off(event, handler);
    limiter.clear();
  } catch (e) {
    if (e instanceof RangeError || e instanceof TypeError) {
      return;
    }
    throw e;
  }
};

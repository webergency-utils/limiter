import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import Limiter,
{
    CapacityLimiter,
    ConcurrencyLimiter,
    Debounce,
    IntervalLimiter,
    LimiterCancelledError,
    LimiterRejectedError,
    LimiterTimeoutError,
    RateLimiter,
    TokenBucketLimiter
}
from '../src/index';

const delay = ( ms: number ) => new Promise(( resolve ) => setTimeout( resolve, ms ));
const tick = () => new Promise<void>(( resolve ) => process.nextTick( resolve ));

describe( 'ConcurrencyLimiter', () =>
{
    test( 'should apply constructor defaults to execute', async () =>
    {
        const limiter = new ConcurrencyLimiter({
            max: 1,
            priority: 5,
            difficulty: 1,
            delay: 15,
            timeout: 1000,
            retries: 1,
            retryDelay: 5
        });

        const snapshots: number[] = [];
        limiter.on( 'add', ( task ) => snapshots.push( task!.priority ));

        const handle = limiter.execute( async () => 1 );
        expect( snapshots[0] ).toBe( 5 );
        await delay( 5 );
        expect( limiter.pending + limiter.active ).toBeGreaterThan( 0 );
        await handle;

        await expect( limiter.execute( async () => 2, { priority: 9 })).resolves.toBe( 2 );
    });

    test( 'should not start synchronously on execute', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        let started = false;

        const handle = limiter.execute( async () => { started = true; return 1 });

        expect( started ).toBe( false );
        expect( limiter.active ).toBe( 0 );
        expect( limiter.pending ).toBe( 1 );

        await tick();
        expect( started ).toBe( true );
        await handle;
    });

    test( 'should delay execution until delay elapses', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const order: string[] = [];

        const handle = limiter.execute( async () => { order.push( 'run' ); return 1 }, { delay: 40 });

        await tick();
        expect( order ).toStrictEqual([]);
        expect( limiter.pending ).toBe( 1 );

        await delay( 50 );
        await handle;
        expect( order ).toStrictEqual([ 'run' ]);
    });

    test( 'should allow adding a batch before any task starts', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'priority' });
        const order: number[] = [];

        for( let i = 1; i <= 3; i++ )
        {
            limiter.execute( async () => { order.push( i ) }, { priority: i, delay: 20 });
        }

        expect( limiter.pending ).toBe( 3 );
        expect( limiter.active ).toBe( 0 );

        await delay( 40 );
        await limiter.onIdle();
        expect( order ).toStrictEqual([ 3, 2, 1 ]);
    });

    test( 'should limit concurrent tasks by count', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 2 });
        let running = 0;
        let maxRunning = 0;

        const job = async () =>
        {
            running++;
            maxRunning = Math.max( maxRunning, running );
            await delay( 30 );
            running--;

            return 1;
        };

        const results = await Promise.all([
            limiter.execute( job ),
            limiter.execute( job ),
            limiter.execute( job ),
            limiter.execute( job )
        ]);

        expect( results ).toStrictEqual([ 1, 1, 1, 1 ]);
        expect( maxRunning ).toBe( 2 );
    });

    test( 'should treat max as sum of difficulties', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 5 });
        let maxActive = 0;

        limiter.on( 'start', () =>
        {
            maxActive = Math.max( maxActive, limiter.activeDifficulty );
        });

        await Promise.all([
            limiter.execute( async () => delay( 40 ), { difficulty: 3 }),
            limiter.execute( async () => delay( 40 ), { difficulty: 3 }),
            limiter.execute( async () => delay( 10 ), { difficulty: 2 })
        ]);

        expect( maxActive ).toBeLessThanOrEqual( 5 );
        expect( maxActive ).toBeGreaterThanOrEqual( 3 );
    });

    test( 'should support fifo ordering', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'fifo' });
        const order: number[] = [];

        const handles = [
            limiter.execute( async () => { await delay( 20 ); order.push( 1 ) }),
            limiter.execute( async () => { order.push( 2 ) }),
            limiter.execute( async () => { order.push( 3 ) })
        ];

        await Promise.all( handles );
        expect( order ).toStrictEqual([ 1, 2, 3 ]);
    });

    test( 'should support lifo ordering', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'lifo' });
        const seen: number[] = [];
        const block = limiter.execute( async () => delay( 40 ));
        await delay( 5 );
        const a = limiter.execute( async () => { seen.push( 1 ) });
        const b = limiter.execute( async () => { seen.push( 2 ) });
        await Promise.all([ block, a, b ]);
        expect( seen ).toStrictEqual([ 2, 1 ]);
    });

    test( 'should support priority ordering', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'priority' });
        const order: number[] = [];

        const block = limiter.execute( async () => delay( 30 ));
        await delay( 5 );
        const low = limiter.execute( async () => { order.push( 1 ) }, { priority: 1 });
        const high = limiter.execute( async () => { order.push( 10 ) }, { priority: 10 });
        await Promise.all([ block, low, high ]);
        expect( order ).toStrictEqual([ 10, 1 ]);
    });

    test( 'should cancel queued tasks only', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const started: number[] = [];

        const first = limiter.execute( async () =>
        {
            started.push( 1 );
            await delay( 40 );

            return 'a';
        });

        await delay( 5 );
        const second = limiter.execute( async () =>
        {
            started.push( 2 );

            return 'b';
        });

        expect( second.cancel() ).toBe( true );
        await expect( second ).rejects.toBeInstanceOf( LimiterCancelledError );
        expect( first.cancel() ).toBe( false );
        await expect( first ).resolves.toBe( 'a' );
        expect( started ).toStrictEqual([ 1 ]);
    });

    test( 'should timeout waiting and running tasks', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });

        const blocking = limiter.execute( async () => delay( 100 ));
        await delay( 5 );
        const waiting = limiter.execute( async () => 'x', { timeout: 20 });
        await expect( waiting ).rejects.toBeInstanceOf( LimiterTimeoutError );
        await blocking;

        const running = limiter.execute( async () => delay( 100 ), { timeout: 20 });
        await expect( running ).rejects.toBeInstanceOf( LimiterTimeoutError );
        await delay( 120 );
    });

    test( 'should reject when deadline Date is reached', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const block = limiter.execute( async () => delay( 80 ));
        await delay( 5 );
        const waiting = limiter.execute( async () => 'x', { deadline: new Date( Date.now() + 25 ) });
        await expect( waiting ).rejects.toBeInstanceOf( LimiterTimeoutError );
        await block;

        await expect( limiter.execute( async () => 1, { deadline: new Date( Date.now() - 1 ) })).rejects.toBeInstanceOf( LimiterTimeoutError );
    });

    test( 'should use earlier of timeout and deadline', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const block = limiter.execute( async () => delay( 100 ));
        await delay( 5 );
        const start = Date.now();
        const waiting = limiter.execute( async () => 'x', {
            timeout: 200,
            deadline: new Date( Date.now() + 30 )
        });
        await expect( waiting ).rejects.toBeInstanceOf( LimiterTimeoutError );
        expect( Date.now() - start ).toBeLessThan( 100 );
        await block;
    });

    test( 'should retry failed executions', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, retries: 2, retryDelay: 5 });
        let attempts = 0;

        const result = await limiter.execute( async () =>
        {
            attempts++;

            if( attempts < 3 ){ throw new Error( 'fail' ) }

            return 'ok';
        });

        expect( result ).toBe( 'ok' );
        expect( attempts ).toBe( 3 );
    });

    test( 'should respect retryIf', async () =>
    {
        const limiter = new ConcurrencyLimiter({
            max: 1,
            retries: 5,
            retryIf: ( error ) => error instanceof Error && error.message === 'retry'
        });

        let attempts = 0;

        await expect( limiter.execute( async () =>
        {
            attempts++;
            throw new Error( 'nope' );
        })).rejects.toThrow( 'nope' );

        expect( attempts ).toBe( 1 );
    });

    test( 'should pause and resume', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const order: string[] = [];

        limiter.pause();
        const handle = limiter.execute( async () => { order.push( 'run' ); return 1 });
        await delay( 20 );
        expect( order ).toStrictEqual([]);
        expect( limiter.paused ).toBe( true );
        limiter.resume();
        await expect( handle ).resolves.toBe( 1 );
        expect( order ).toStrictEqual([ 'run' ]);
    });

    test( 'should emit events and onIdle', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const events: string[] = [];

        for( const event of [ 'add', 'start', 'success', 'settle', 'empty', 'idle', 'pause', 'resume' ] as const )
        {
            limiter.on( event, () => events.push( event ));
        }

        limiter.pause();
        limiter.resume();
        await limiter.execute( async () => 1 );
        await limiter.onIdle();

        expect( events ).toContain( 'add' );
        expect( events ).toContain( 'start' );
        expect( events ).toContain( 'success' );
        expect( events ).toContain( 'settle' );
        expect( events ).toContain( 'idle' );
        expect( events ).toContain( 'pause' );
        expect( events ).toContain( 'resume' );
    });

    test( 'should wrap functions', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 2 });
        const add = limiter.wrap( async ( a: number, b: number ) => a + b );
        await expect( add( 2, 3 )).resolves.toBe( 5 );
    });

    test( 'should clear queued tasks', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const block = limiter.execute( async () => delay( 50 ));
        await delay( 5 );
        const queued = limiter.execute( async () => 1 );
        limiter.clear();
        await expect( queued ).rejects.toBeInstanceOf( LimiterCancelledError );
        await block;
    });

    test( 'should reject invalid difficulty', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        await expect( limiter.execute( async () => 1, { difficulty: 0 })).rejects.toThrow( /difficulty/ );
    });

    test( 'should support AbortSignal', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const controller = new AbortController();
        const block = limiter.execute( async () => delay( 50 ));
        await delay( 5 );
        const queued = limiter.execute( async () => 1, { signal: controller.signal });
        controller.abort();
        await expect( queued ).rejects.toBeInstanceOf( LimiterCancelledError );
        await block;

        const pre = new AbortController();
        pre.abort();
        await expect( limiter.execute( async () => 1, { signal: pre.signal })).rejects.toBeInstanceOf( LimiterCancelledError );
    });
});

describe( 'RateLimiter', () =>
{
    test( 'should enforce sliding window rate with difficulty', async () =>
    {
        const limiter = new RateLimiter({ limit: 3, window: 50, windowType: 'sliding' });
        const started: number[] = [];

        await Promise.all([
            limiter.execute( async () => { started.push( Date.now()); return 1 }, { difficulty: 2 }),
            limiter.execute( async () => { started.push( Date.now()); return 1 }, { difficulty: 1 })
        ]);

        const third = limiter.execute( async () => { started.push( Date.now()); return 1 }, { difficulty: 1 });
        await third;
        expect( started.length ).toBe( 3 );
        expect( started[2] - started[0] ).toBeGreaterThanOrEqual( 40 );
    });

    test( 'should enforce fixed window rate', async () =>
    {
        const limiter = new RateLimiter({ limit: 2, window: 40, windowType: 'fixed' });
        await limiter.execute( async () => 1 );
        await limiter.execute( async () => 1 );
        const start = Date.now();
        await limiter.execute( async () => 1 );
        expect( Date.now() - start ).toBeGreaterThanOrEqual( 30 );
    });
});

describe( 'IntervalLimiter', () =>
{
    test( 'should space starts by interval', async () =>
    {
        const limiter = new IntervalLimiter({ interval: 30 });
        const times: number[] = [];

        await Promise.all([
            limiter.execute( async () => { times.push( Date.now()) }),
            limiter.execute( async () => { times.push( Date.now()) }),
            limiter.execute( async () => { times.push( Date.now()) })
        ]);

        expect( times[1] - times[0] ).toBeGreaterThanOrEqual( 25 );
        expect( times[2] - times[1] ).toBeGreaterThanOrEqual( 25 );
    });
});

describe( 'TokenBucketLimiter', () =>
{
    test( 'should consume tokens by difficulty and refill', async () =>
    {
        const limiter = new TokenBucketLimiter({
            capacity: 3,
            refillAmount: 3,
            refillInterval: 40,
            initialTokens: 3
        });

        await limiter.execute( async () => 1, { difficulty: 2 });
        await limiter.execute( async () => 1, { difficulty: 1 });
        const start = Date.now();
        await limiter.execute( async () => 1, { difficulty: 1 });
        expect( Date.now() - start ).toBeGreaterThanOrEqual( 30 );
    });
});

describe( 'CapacityLimiter', () =>
{
    test( 'should reject when capacity exceeded', async () =>
    {
        const limiter = new CapacityLimiter({ capacity: 2 });
        const a = limiter.execute( async () => delay( 40 ), { difficulty: 1 });
        const b = limiter.execute( async () => delay( 40 ), { difficulty: 1 });
        await expect( limiter.execute( async () => 1, { difficulty: 1 })).rejects.toBeInstanceOf( LimiterRejectedError );
        await Promise.all([ a, b ]);
    });

    test( 'should combine with concurrency in Limiter', async () =>
    {
        const combined = new Limiter({ concurrency: 1, capacity: 2 });
        const block = combined.execute( async () => delay( 40 ));
        await delay( 5 );
        const queued = combined.execute( async () => 1, { difficulty: 1 });
        await expect( combined.execute( async () => 1, { difficulty: 1 })).rejects.toBeInstanceOf( LimiterRejectedError );
        await Promise.all([ block, queued ]);
    });
});

describe( 'Debounce', () =>
{
    test( 'should debounce trailing calls', async () =>
    {
        const limiter = new Debounce({ wait: 30 });
        let calls = 0;

        const p1 = limiter.execute( async () => { calls++; return 1 });
        const p2 = limiter.execute( async () => { calls++; return 2 });
        await expect( p1 ).rejects.toMatchObject({ name: 'LimiterDebouncedError' });
        await expect( p2 ).resolves.toBe( 2 );
        expect( calls ).toBe( 1 );
    });

    test( 'should support leading debounce', async () =>
    {
        const limiter = new Debounce({ wait: 40, leading: true, trailing: false });
        let calls = 0;

        const p1 = limiter.execute( async () => { calls++; return 1 });
        const p2 = limiter.execute( async () => { calls++; return 2 });
        await expect( p1 ).resolves.toBe( 1 );
        await delay( 50 );
        expect( calls ).toBe( 1 );
        // p2 may still be pending trailing disabled - cancelled or hanging?
        // with trailing false and leading already fired, p2 arms timer then flush no-ops trailing
        await delay( 50 );
    });

    test( 'should cancel pending debounced task', async () =>
    {
        const limiter = new Debounce({ wait: 50 });
        const handle = limiter.execute( async () => 1 );
        expect( handle.cancel() ).toBe( true );
        await expect( handle ).rejects.toBeInstanceOf( LimiterCancelledError );
    });
});

describe( 'Limiter combinator', () =>
{
    test( 'should combine concurrency and interval', async () =>
    {
        const limiter = new Limiter({ concurrency: 2, interval: 20 });
        const times: number[] = [];

        await Promise.all([
            limiter.execute( async () => { times.push( Date.now()); await delay( 5 ) }),
            limiter.execute( async () => { times.push( Date.now()); await delay( 5 ) }),
            limiter.execute( async () => { times.push( Date.now()) })
        ]);

        expect( times.length ).toBe( 3 );
        expect( times[1] - times[0] ).toBeGreaterThanOrEqual( 15 );
    });

    test( 'should work with sync return values', async () =>
    {
        const limiter = new Limiter({ concurrency: 1 });
        await expect( limiter.execute(() => 42 )).resolves.toBe( 42 );
    });

    test( 'should allow unlimited when no strategies set', async () =>
    {
        const limiter = new Limiter();
        const results = await Promise.all([
            limiter.execute( async () => 1 ),
            limiter.execute( async () => 2 )
        ]);
        expect( results ).toStrictEqual([ 1, 2 ]);
    });

    test( 'should off event handlers', async () =>
    {
        const limiter = new Limiter({ concurrency: 1 });
        let count = 0;
        const handler = () => { count++ };
        limiter.on( 'start', handler );
        limiter.off( 'start', handler );
        await limiter.execute( async () => 1 );
        expect( count ).toBe( 0 );
    });

    test( 'should accept object-form strategy options', async () =>
    {
        const limiter = new Limiter({
            concurrency: { max: 2 },
            interval: { interval: 5 },
            rate: { limit: 10, window: 1000 },
            tokenBucket: { capacity: 5, refillAmount: 5, refillInterval: 1000 },
            capacity: { capacity: 10 },
            order: 'priority'
        });

        await expect( limiter.execute( async () => 'ok', { priority: 1 })).resolves.toBe( 'ok' );
    });

    test( 'should roll back gates when a later gate denies', async () =>
    {
        const limiter = new Limiter({ concurrency: 2, rate: { limit: 1, window: 40, windowType: 'fixed' }});
        await limiter.execute( async () => 1 );
        const start = Date.now();
        await limiter.execute( async () => 1 );
        expect( Date.now() - start ).toBeGreaterThanOrEqual( 30 );
        expect( limiter.active ).toBe( 0 );
    });

    test( 'should support custom compare', async () =>
    {
        const limiter = new ConcurrencyLimiter({
            max: 1,
            order: 'priority',
            compare: ( a, b ) => a.priority - b.priority
        });
        const order: number[] = [];
        const block = limiter.execute( async () => delay( 30 ));
        await delay( 5 );
        const high = limiter.execute( async () => { order.push( 10 ) }, { priority: 10 });
        const low = limiter.execute( async () => { order.push( 1 ) }, { priority: 1 });
        await Promise.all([ block, high, low ]);
        expect( order ).toStrictEqual([ 1, 10 ]);
    });

    test( 'should emit error on failure', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        let errored = false;
        limiter.on( 'error', () => { errored = true });
        await expect( limiter.execute( async () => { throw new Error( 'x' ) })).rejects.toThrow( 'x' );
        expect( errored ).toBe( true );
    });

    test( 'should timeout immediately when timeout is 0', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const block = limiter.execute( async () => delay( 50 ));
        await delay( 5 );
        await expect( limiter.execute( async () => 1, { timeout: 0 })).rejects.toBeInstanceOf( LimiterTimeoutError );
        await block;
    });

    test( 'should finish after running timeout when fn completes', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const handle = limiter.execute( async () => delay( 60 ), { timeout: 20 });
        await expect( handle ).rejects.toBeInstanceOf( LimiterTimeoutError );
        await delay( 80 );
        expect( limiter.active ).toBe( 0 );
        await expect( limiter.execute( async () => 1 )).resolves.toBe( 1 );
    });

    test( 'should stop retrying after timeout while waiting to retry', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, retries: 3, retryDelay: 40 });
        const handle = limiter.execute( async () => { throw new Error( 'fail' ) }, { timeout: 30 });
        await expect( handle ).rejects.toBeInstanceOf( LimiterTimeoutError );
        await delay( 100 );
        expect( limiter.active ).toBe( 0 );
    });

    test( 'should expose size pending active getters', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        expect( limiter.size ).toBe( 0 );
        const block = limiter.execute( async () => delay( 40 ));
        await delay( 5 );
        const queued = limiter.execute( async () => 1 );
        expect( limiter.active ).toBe( 1 );
        expect( limiter.pending ).toBe( 1 );
        expect( limiter.size ).toBe( 2 );
        await Promise.all([ block, queued ]);
    });

    test( 'should reject cancel after clear', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const block = limiter.execute( async () => delay( 30 ));
        await delay( 5 );
        const queued = limiter.execute( async () => 1 );
        limiter.clear();
        await expect( queued ).rejects.toBeInstanceOf( LimiterCancelledError );
        expect( queued.cancel() ).toBe( false );
        await block;
    });
    test( 'should await onIdle while work is running', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const handle = limiter.execute( async () => delay( 40 ));
        const idle = limiter.onIdle();
        await handle;
        await idle;
        await limiter.onIdle();
    });

    test( 'should cancel priority queued task', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'priority' });
        const block = limiter.execute( async () => delay( 40 ));
        await delay( 5 );
        const queued = limiter.execute( async () => 1, { priority: 3 });
        expect( limiter.pendingDifficulty ).toBe( 1 );
        expect( queued.cancel() ).toBe( true );
        await expect( queued ).rejects.toBeInstanceOf( LimiterCancelledError );
        await block;
    });

    test( 'should clear priority queue', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'priority' });
        const block = limiter.execute( async () => delay( 40 ));
        await delay( 5 );
        const queued = limiter.execute( async () => 1, { priority: 5 });
        limiter.clear();
        await expect( queued ).rejects.toBeInstanceOf( LimiterCancelledError );
        await block;
    });

    test( 'should finish after timed out task throws', async () =>
    {
        const limiter = new ConcurrencyLimiter({ max: 1 });
        const handle = limiter.execute( async () =>
        {
            await delay( 50 );
            throw new Error( 'late' );
        }, { timeout: 15 });

        await expect( handle ).rejects.toBeInstanceOf( LimiterTimeoutError );
        await delay( 60 );
        expect( limiter.active ).toBe( 0 );
    });

    test( 'should cover difficulty larger than rate limit wake path', async () =>
    {
        const limiter = new RateLimiter({ limit: 2, window: 30, windowType: 'sliding' });
        await expect( limiter.execute( async () => 1, { difficulty: 2 })).resolves.toBe( 1 );
    });
});

describe( 'Debounce extras', () =>
{
    test( 'should support leading and trailing', async () =>
    {
        const limiter = new Debounce({ wait: 30, leading: true, trailing: true });
        let calls = 0;
        const p1 = limiter.execute( async () => { calls++; return calls });
        await expect( p1 ).resolves.toBe( 1 );
        await delay( 40 );
        const p2 = limiter.execute( async () => { calls++; return calls });
        await expect( p2 ).resolves.toBe( 2 );
        expect( calls ).toBe( 2 );
    });

    test( 'should reject leading failures', async () =>
    {
        const limiter = new Debounce({ wait: 20, leading: true, trailing: false });
        await expect( limiter.execute( async () => { throw new Error( 'lead' ) })).rejects.toThrow( 'lead' );
    });

    test( 'should no-op cancel twice', async () =>
    {
        const limiter = new Debounce({ wait: 40 });
        const handle = limiter.execute( async () => 1 );
        expect( handle.cancel() ).toBe( true );
        await expect( handle ).rejects.toBeInstanceOf( LimiterCancelledError );
        expect( handle.cancel() ).toBe( false );
    });

    test( 'should require leading or trailing', () =>
    {
        expect(() => new Debounce({ wait: 10, leading: false, trailing: false })).toThrow( RangeError );
    });
});

describe( 'Gate validation', () =>
{
    test( 'should validate constructor options', () =>
    {
        expect(() => new ConcurrencyLimiter({ max: 0 })).toThrow( RangeError );
        expect(() => new RateLimiter({ limit: 0, window: 10 })).toThrow( RangeError );
        expect(() => new RateLimiter({ limit: 1, window: 0 })).toThrow( RangeError );
        expect(() => new IntervalLimiter({ interval: -1 })).toThrow( RangeError );
        expect(() => new TokenBucketLimiter({ capacity: 0, refillAmount: 1, refillInterval: 1 })).toThrow( RangeError );
        expect(() => new CapacityLimiter({ capacity: 0 })).toThrow( RangeError );
        expect(() => new Debounce({ wait: -1 })).toThrow( RangeError );
    });
});

describe( 'Additional Coverage Tests', () =>
{
    test( 'defaultCompare sequence fallback', async () =>
    {
        // Arrange
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'priority' });
        const order: number[] = [];

        // Act
        const block = limiter.execute( async () => delay( 30 ));
        await delay( 5 );

        const first = limiter.execute( async () => { order.push( 1 ) }, { priority: 5 });
        const second = limiter.execute( async () => { order.push( 2 ) }, { priority: 5 });

        await Promise.all([ block, first, second ]);

        // Assert
        expect( order ).toStrictEqual([ 1, 2 ]);
    });

    test( 'should fall back to queueMicrotask when process.nextTick is not a function', async () =>
    {
        // Arrange
        const originalNextTick = process.nextTick;
        // @ts-ignore
        delete process.nextTick;

        try
        {
            vi.resetModules();
            // Act
            const { ConcurrencyLimiter } = await import( '../src/limiters' );
            const limiter = new ConcurrencyLimiter({ max: 1 });
            const order: number[] = [];

            await Promise.all([
                limiter.execute( async () => { order.push( 1 ); } ),
                limiter.execute( async () => { order.push( 2 ); } )
            ]);

            // Assert
            expect( order ).toStrictEqual([ 1, 2 ]);
        }
        finally
        {
            // Restore
            process.nextTick = originalNextTick;
        }
    });

    test( 'should execute heap ID getter during deletion', async () =>
    {
        // Arrange
        const limiter = new ConcurrencyLimiter({ max: 1, order: 'priority' });

        const block = limiter.execute( async () => delay( 30 ));
        await delay( 5 );

        const taskA = limiter.execute( async () => 1, { priority: 10 });
        const taskB = limiter.execute( async () => 2, { priority: 5 });

        // Act
        taskB.cancel();

        // Assert
        await expect( taskB ).rejects.toBeInstanceOf( LimiterCancelledError );
        await Promise.all([ block, taskA ]);
    });

    test( 'should return false when cancelling a debounce handle that has no active entry', async () =>
    {
        // Arrange
        const limiter = new Debounce({ wait: 20, leading: true, trailing: false });

        // Act
        const handle = limiter.execute( async () => 'result' );
        const cancelResult = handle.cancel();

        // Assert
        expect( cancelResult ).toBe( false );
        await expect( handle ).resolves.toBe( 'result' );
    });
});


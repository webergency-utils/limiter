import { describe, test, expect } from 'vitest';
import
{
    CapacityGate,
    ConcurrencyGate,
    IntervalGate,
    RateGate,
    TokenBucketGate
}
from '../src/gates';

const delay = ( ms: number ) => new Promise(( resolve ) => setTimeout( resolve, ms ));

describe( 'Gates', () =>
{
    test( 'ConcurrencyGate tracks active and resets', () =>
    {
        const gate = new ConcurrencyGate( 5 );
        expect( gate.max ).toBe( 5 );
        expect( gate.tryAcquire( 3 )).toBe( true );
        expect( gate.active ).toBe( 3 );
        expect( gate.tryAcquire( 3 )).toBe( false );
        expect( gate.nextAvailableAt( 1 )).toBeNull();
        gate.release( 3 );
        expect( gate.active ).toBe( 0 );
        gate.tryAcquire( 2 );
        gate.reset();
        expect( gate.active ).toBe( 0 );
    });

    test( 'RateGate sliding nextAvailableAt and reset', async () =>
    {
        const gate = new RateGate( 2, 30, 'sliding' );
        expect( gate.tryAcquire( 2 )).toBe( true );
        expect( gate.tryAcquire( 1 )).toBe( false );
        const at = gate.nextAvailableAt( 1 );
        expect( at ).toBeGreaterThan( Date.now() - 1 );
        gate.release( 1 );
        gate.reset();
        expect( gate.nextAvailableAt( 2 )).toBeLessThanOrEqual( Date.now());
        expect( gate.tryAcquire( 2 )).toBe( true );
        await delay( 35 );
        expect( gate.tryAcquire( 2 )).toBe( true );
    });

    test( 'RateGate fixed window nextAvailableAt when available', () =>
    {
        const gate = new RateGate( 5, 1000, 'fixed' );
        expect( gate.tryAcquire( 1 )).toBe( true );
        expect( gate.nextAvailableAt( 1 )).toBeLessThanOrEqual( Date.now());
        gate.reset();
        expect( gate.tryAcquire( 5 )).toBe( true );
        expect( gate.nextAvailableAt( 1 )).toBeGreaterThan( Date.now());
    });

    test( 'IntervalGate nextAvailableAt and reset', () =>
    {
        const gate = new IntervalGate( 50 );
        expect( gate.nextAvailableAt( 1 )).toBeLessThanOrEqual( Date.now());
        expect( gate.tryAcquire( 1 )).toBe( true );
        expect( gate.nextAvailableAt( 1 )).toBeGreaterThan( Date.now());
        gate.release( 1 );
        gate.reset();
        expect( gate.tryAcquire( 1 )).toBe( true );
    });

    test( 'TokenBucketGate tokens refill and nextAvailableAt', async () =>
    {
        const gate = new TokenBucketGate( 2, 1, 25, 0 );
        expect( gate.tryAcquire( 1 )).toBe( false );
        const at = gate.nextAvailableAt( 1 );
        expect( at ).toBeGreaterThan( Date.now() - 1 );
        await delay( 30 );
        expect( gate.tokens ).toBeGreaterThanOrEqual( 1 );
        expect( gate.tryAcquire( 1 )).toBe( true );
        expect( gate.nextAvailableAt( 1 )).toBeLessThanOrEqual( Date.now() + 30 );
        gate.reset();
        expect( gate.tokens ).toBe( 2 );
    });

    test( 'CapacityGate reserve and reset', () =>
    {
        const gate = new CapacityGate( 3 );
        expect( gate.capacity ).toBe( 3 );
        expect( gate.reserve( 2 )).toBe( true );
        expect( gate.queued ).toBe( 2 );
        expect( gate.reserve( 2 )).toBe( false );
        expect( gate.tryAcquire( 1 )).toBe( true );
        expect( gate.nextAvailableAt( 1 )).toBeNull();
        gate.release( 1 );
        gate.unreserve( 2 );
        expect( gate.queued ).toBe( 0 );
        gate.reserve( 1 );
        gate.reset();
        expect( gate.queued ).toBe( 0 );
    });

    test( 'RateGate nextAvailableAt when difficulty exceeds limit on empty window', () =>
    {
        const gate = new RateGate( 2, 100, 'sliding' );
        expect( gate.nextAvailableAt( 5 )).toBeLessThanOrEqual( Date.now());
    });

    test( 'TokenBucketGate nextAvailableAt when tokens already available', () =>
    {
        const gate = new TokenBucketGate( 5, 1, 100, 5 );
        expect( gate.nextAvailableAt( 2 )).toBeLessThanOrEqual( Date.now());
    });

    test( 'TokenBucketGate throws on invalid capacity, refillAmount, or refillInterval', () =>
    {
        // Arrange & Act & Assert
        expect( () => new TokenBucketGate( 0, 1, 100 ) ).toThrowError( RangeError );
        expect( () => new TokenBucketGate( 5, 0, 100 ) ).toThrowError( RangeError );
        expect( () => new TokenBucketGate( 5, 1, 0 ) ).toThrowError( RangeError );
    });
});

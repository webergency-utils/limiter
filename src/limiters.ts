import { BaseLimiter } from './base-limiter';
import { LimiterCancelledError } from './errors';
import { CapacityGate, ConcurrencyGate, IntervalGate, RateGate, TokenBucketGate } from './gates';
import type
{
    CapacityLimiterOptions,
    ConcurrencyLimiterOptions,
    DebounceOptions,
    ExecuteHandle,
    IntervalLimiterOptions,
    LimiterOptions,
    RateLimiterOptions,
    SharedLimiterOptions,
    TaskAttrs,
    TokenBucketLimiterOptions
}
from './types';

function sharedOf<T extends SharedLimiterOptions>( options: T ): SharedLimiterOptions
{
    return (
    {
        order           : options.order,
        compare         : options.compare,
        priority        : options.priority,
        difficulty      : options.difficulty,
        timeout         : options.timeout,
        deadline        : options.deadline,
        delay           : options.delay,
        retries         : options.retries,
        retryDelay      : options.retryDelay,
        retryBackoff    : options.retryBackoff,
        retryIf         : options.retryIf,
        key             : options.key,
        signal          : options.signal
    });
}

export class ConcurrencyLimiter extends BaseLimiter
{
    constructor( options: ConcurrencyLimiterOptions )
    {
        super([ new ConcurrencyGate( options.max ) ], sharedOf( options ));
    }
}

export class RateLimiter extends BaseLimiter
{
    constructor( options: RateLimiterOptions )
    {
        super([ new RateGate( options.limit, options.window, options.windowType ?? 'sliding' ) ], sharedOf( options ));
    }
}

export class IntervalLimiter extends BaseLimiter
{
    constructor( options: IntervalLimiterOptions )
    {
        super([ new IntervalGate( options.interval ) ], sharedOf( options ));
    }
}

export class TokenBucketLimiter extends BaseLimiter
{
    constructor( options: TokenBucketLimiterOptions )
    {
        super(
            [
                new TokenBucketGate(
                    options.capacity,
                    options.refillAmount,
                    options.refillInterval,
                    options.initialTokens
                )
            ],
            sharedOf( options )
        );
    }
}

export class CapacityLimiter extends BaseLimiter
{
    constructor( options: CapacityLimiterOptions )
    {
        const gate = new CapacityGate( options.capacity );

        super([], sharedOf( options ), gate );
    }
}

type DebounceEntry<T = unknown> =
{
    fn      : () => T | Promise<T>
    attrs   : TaskAttrs
    resolve : ( value: Awaited<T> ) => void
    reject  : ( reason: unknown ) => void
};

export class Debounce extends BaseLimiter
{
    #wait       : number;
    #leading    : boolean;
    #trailing   : boolean;
    #timers     = new Map<string, ReturnType<typeof setTimeout>>();
    #entries    = new Map<string, DebounceEntry>();
    #inflight   = new Set<string>();

    constructor( options: DebounceOptions )
    {
        if( !( options.wait >= 0 )){ throw new RangeError( 'Debounce wait must be >= 0' ) }

        const leading = options.leading ?? false;
        const trailing = options.trailing ?? true;

        if( !leading && !trailing ){ throw new RangeError( 'Debounce requires leading or trailing' ) }

        super([ new ConcurrencyGate( Number.MAX_SAFE_INTEGER ) ], sharedOf( options ));
        this.#wait = options.wait;
        this.#leading = leading;
        this.#trailing = trailing;
    }

    override execute<T>( fn: () => T | Promise<T>, attrs: TaskAttrs = {}): ExecuteHandle<Awaited<T>>
    {
        const key = attrs.key ?? '';
        let cancelled = false;

        const handle = new Promise<Awaited<T>>(( resolve, reject ) =>
        {
            const previous = this.#entries.get( key );

            if( previous )
            {
                previous.reject( Object.assign( new Error( 'Debounced' ), { name: 'LimiterDebouncedError' }));
            }

            const entry: DebounceEntry<T> =
            {
                fn,
                attrs,
                resolve,
                reject
            };

            this.#entries.set( key, entry as DebounceEntry );

            const shouldLead = this.#leading && !this.#inflight.has( key );

            if( shouldLead )
            {
                this.#inflight.add( key );
                this.#entries.delete( key );
                super.execute( fn, attrs ).then(
                    ( value ) =>
                    {
                        resolve( value );

                        if( this.#trailing )
                        {
                            this.#arm( key );
                        }
                        else
                        {
                            this.#inflight.delete( key );
                        }
                    },
                    ( error ) =>
                    {
                        reject( error );
                        this.#inflight.delete( key );
                    }
                );

                return;
            }

            if( this.#trailing )
            {
                this.#arm( key );
            }
        }) as ExecuteHandle<Awaited<T>>;

        handle.cancel = () =>
        {
            if( cancelled ){ return false }

            const entry = this.#entries.get( key );

            if( !entry ){ return false }

            cancelled = true;
            this.#clearTimer( key );
            this.#entries.delete( key );
            entry.reject( new LimiterCancelledError());

            return true;
        };

        return handle;
    }

    #arm( key: string ): void
    {
        this.#clearTimer( key );

        const timer = setTimeout(() =>
        {
            this.#timers.delete( key );
            this.#flush( key );
        }, this.#wait );

        this.#timers.set( key, timer );
    }

    #clearTimer( key: string ): void
    {
        const timer = this.#timers.get( key );

        if( timer )
        {
            clearTimeout( timer );
            this.#timers.delete( key );
        }
    }

    #flush( key: string ): void
    {
        const entry = this.#entries.get( key );
        this.#entries.delete( key );

        if( !entry )
        {
            this.#inflight.delete( key );

            return;
        }

        this.#inflight.delete( key );

        super.execute( entry.fn, entry.attrs ).then( entry.resolve, entry.reject );
    }
}

export default class Limiter extends BaseLimiter
{
    constructor( options: LimiterOptions = {})
    {
        const gates = [];
        let capacityGate: CapacityGate | undefined;
        const shared = sharedOf( options );

        if( options.concurrency !== undefined )
        {
            const max = typeof options.concurrency === 'number' ? options.concurrency : options.concurrency.max;
            gates.push( new ConcurrencyGate( max ));
        }

        if( options.rate )
        {
            gates.push( new RateGate( options.rate.limit, options.rate.window, options.rate.windowType ?? 'sliding' ));
        }

        if( options.interval !== undefined )
        {
            const interval = typeof options.interval === 'number' ? options.interval : options.interval.interval;
            gates.push( new IntervalGate( interval ));
        }

        if( options.tokenBucket )
        {
            gates.push( new TokenBucketGate(
                options.tokenBucket.capacity,
                options.tokenBucket.refillAmount,
                options.tokenBucket.refillInterval,
                options.tokenBucket.initialTokens
            ));
        }

        if( options.capacity !== undefined )
        {
            const capacity = typeof options.capacity === 'number' ? options.capacity : options.capacity.capacity;
            capacityGate = new CapacityGate( capacity );
        }

        if( !gates.length && !capacityGate )
        {
            gates.push( new ConcurrencyGate( Number.MAX_SAFE_INTEGER ));
        }

        super( gates, shared, capacityGate );
    }
}

export interface LimiterGate
{
    tryAcquire( difficulty: number ): boolean
    release( difficulty: number ): void
    nextAvailableAt( difficulty: number ): number | null
    reset?(): void
}

export class ConcurrencyGate implements LimiterGate
{
    #max        : number;
    #active     : number = 0;

    constructor( max: number )
    {
        if( !( max > 0 )){ throw new RangeError( 'ConcurrencyGate max must be > 0' ) }

        this.#max = max;
    }

    get active(): number
    {
        return this.#active;
    }

    get max(): number
    {
        return this.#max;
    }

    tryAcquire( difficulty: number ): boolean
    {
        if( this.#active + difficulty <= this.#max )
        {
            this.#active += difficulty;

            return true;
        }

        return false;
    }

    release( difficulty: number ): void
    {
        this.#active = Math.max( 0, this.#active - difficulty );
    }

    nextAvailableAt( _difficulty: number ): number | null
    {
        return null;
    }

    reset(): void
    {
        this.#active = 0;
    }
}

export class RateGate implements LimiterGate
{
    #limit      : number;
    #window     : number;
    #windowType : 'fixed' | 'sliding';
    #entries    : { time: number, cost: number }[] = [];
    #fixedStart : number = 0;
    #fixedUsed  : number = 0;

    constructor( limit: number, window: number, windowType: 'fixed' | 'sliding' = 'sliding' )
    {
        if( !( limit > 0 )){ throw new RangeError( 'RateGate limit must be > 0' ) }
        if( !( window > 0 )){ throw new RangeError( 'RateGate window must be > 0' ) }

        this.#limit = limit;
        this.#window = window;
        this.#windowType = windowType;
    }

    #pruneSliding( now: number ): void
    {
        const cutoff = now - this.#window;

        while( this.#entries.length && this.#entries[0].time <= cutoff )
        {
            this.#entries.shift();
        }
    }

    #slidingUsed(): number
    {
        let sum = 0;

        for( const entry of this.#entries )
        {
            sum += entry.cost;
        }

        return sum;
    }

    #ensureFixedWindow( now: number ): void
    {
        if( !this.#fixedStart || now - this.#fixedStart >= this.#window )
        {
            this.#fixedStart = now;
            this.#fixedUsed = 0;
        }
    }

    tryAcquire( difficulty: number ): boolean
    {
        const now = Date.now();

        if( this.#windowType === 'fixed' )
        {
            this.#ensureFixedWindow( now );

            if( this.#fixedUsed + difficulty <= this.#limit )
            {
                this.#fixedUsed += difficulty;

                return true;
            }

            return false;
        }

        this.#pruneSliding( now );

        if( this.#slidingUsed() + difficulty <= this.#limit )
        {
            this.#entries.push({ time: now, cost: difficulty });

            return true;
        }

        return false;
    }

    release( _difficulty: number ): void
    {
    }

    nextAvailableAt( difficulty: number ): number | null
    {
        const now = Date.now();

        if( this.#windowType === 'fixed' )
        {
            this.#ensureFixedWindow( now );

            if( this.#fixedUsed + difficulty <= this.#limit ){ return now }

            return this.#fixedStart + this.#window;
        }

        this.#pruneSliding( now );

        if( this.#slidingUsed() + difficulty <= this.#limit ){ return now }

        if( !this.#entries.length ){ return now }

        let used = this.#slidingUsed();
        let i = 0;

        while( i < this.#entries.length && used + difficulty > this.#limit )
        {
            used -= this.#entries[i].cost;
            i++;
        }

        const index = Math.max( 0, i - 1 );

        return this.#entries[index].time + this.#window;
    }

    reset(): void
    {
        this.#entries = [];
        this.#fixedStart = 0;
        this.#fixedUsed = 0;
    }
}

export class IntervalGate implements LimiterGate
{
    #interval   : number;
    #lastStart  : number = 0;

    constructor( interval: number )
    {
        if( !( interval >= 0 )){ throw new RangeError( 'IntervalGate interval must be >= 0' ) }

        this.#interval = interval;
    }

    tryAcquire( _difficulty: number ): boolean
    {
        const now = Date.now();

        if( !this.#lastStart || now - this.#lastStart >= this.#interval )
        {
            this.#lastStart = now;

            return true;
        }

        return false;
    }

    release( _difficulty: number ): void
    {
    }

    nextAvailableAt( _difficulty: number ): number | null
    {
        if( !this.#lastStart ){ return Date.now() }

        return this.#lastStart + this.#interval;
    }

    reset(): void
    {
        this.#lastStart = 0;
    }
}

export class TokenBucketGate implements LimiterGate
{
    #capacity       : number;
    #refillAmount   : number;
    #refillInterval : number;
    #tokens         : number;
    #lastRefill     : number;

    constructor( capacity: number, refillAmount: number, refillInterval: number, initialTokens?: number )
    {
        if( !( capacity > 0 )){ throw new RangeError( 'TokenBucketGate capacity must be > 0' ) }
        if( !( refillAmount > 0 )){ throw new RangeError( 'TokenBucketGate refillAmount must be > 0' ) }
        if( !( refillInterval > 0 )){ throw new RangeError( 'TokenBucketGate refillInterval must be > 0' ) }

        this.#capacity = capacity;
        this.#refillAmount = refillAmount;
        this.#refillInterval = refillInterval;
        this.#tokens = initialTokens ?? capacity;
        this.#lastRefill = Date.now();
    }

    get tokens(): number
    {
        this.#refill();

        return this.#tokens;
    }

    #refill(): void
    {
        const now = Date.now();
        const elapsed = now - this.#lastRefill;

        if( elapsed < this.#refillInterval ){ return }

        const steps = Math.floor( elapsed / this.#refillInterval );

        this.#tokens = Math.min( this.#capacity, this.#tokens + steps * this.#refillAmount );
        this.#lastRefill += steps * this.#refillInterval;
    }

    tryAcquire( difficulty: number ): boolean
    {
        this.#refill();

        if( this.#tokens >= difficulty )
        {
            this.#tokens -= difficulty;

            return true;
        }

        return false;
    }

    release( _difficulty: number ): void
    {
    }

    nextAvailableAt( difficulty: number ): number | null
    {
        this.#refill();

        if( this.#tokens >= difficulty ){ return Date.now() }

        const needed = difficulty - this.#tokens;
        const steps = Math.ceil( needed / this.#refillAmount );

        return this.#lastRefill + steps * this.#refillInterval;
    }

    reset(): void
    {
        this.#tokens = this.#capacity;
        this.#lastRefill = Date.now();
    }
}

export class CapacityGate implements LimiterGate
{
    #capacity   : number;
    #queued     : number = 0;

    constructor( capacity: number )
    {
        if( !( capacity > 0 )){ throw new RangeError( 'CapacityGate capacity must be > 0' ) }

        this.#capacity = capacity;
    }

    get queued(): number
    {
        return this.#queued;
    }

    get capacity(): number
    {
        return this.#capacity;
    }

    reserve( difficulty: number ): boolean
    {
        if( this.#queued + difficulty <= this.#capacity )
        {
            this.#queued += difficulty;

            return true;
        }

        return false;
    }

    unreserve( difficulty: number ): void
    {
        this.#queued = Math.max( 0, this.#queued - difficulty );
    }

    tryAcquire( _difficulty: number ): boolean
    {
        return true;
    }

    release( _difficulty: number ): void
    {
    }

    nextAvailableAt( _difficulty: number ): number | null
    {
        return null;
    }

    reset(): void
    {
        this.#queued = 0;
    }
}

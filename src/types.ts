export type QueueOrder = 'fifo' | 'lifo' | 'priority';

export type LimiterEvent =
    | 'add'
    | 'start'
    | 'success'
    | 'error'
    | 'settle'
    | 'reject'
    | 'idle'
    | 'empty'
    | 'pause'
    | 'resume';

export type RetryIf = ( error: unknown, attempt: number ) => boolean;

export type TaskAttrs =
{
    priority?     : number
    difficulty?   : number
    timeout?      : number
    deadline?     : Date
    delay?        : number
    retries?      : number
    retryDelay?   : number
    retryBackoff? : number
    retryIf?      : RetryIf
    key?          : string
    signal?       : AbortSignal
};

export type ExecuteHandle<T> = Promise<T> & { cancel: () => boolean };

export type TaskCompare = ( a: TaskSnapshot, b: TaskSnapshot ) => number;

export type TaskSnapshot =
{
    priority    : number
    difficulty  : number
    sequence    : number
    enqueuedAt  : number
    key?        : string
};

export type SharedLimiterOptions = TaskAttrs &
{
    order?   : QueueOrder
    compare? : TaskCompare
};

export type RateWindowType = 'fixed' | 'sliding';

export type ConcurrencyLimiterOptions = SharedLimiterOptions &
{
    max : number
};

export type RateLimiterOptions = SharedLimiterOptions &
{
    limit       : number
    window      : number
    windowType? : RateWindowType
};

export type IntervalLimiterOptions = SharedLimiterOptions &
{
    interval : number
};

export type TokenBucketLimiterOptions = SharedLimiterOptions &
{
    capacity        : number
    refillAmount    : number
    refillInterval  : number
    initialTokens?  : number
};

export type CapacityLimiterOptions = SharedLimiterOptions &
{
    capacity : number
};

export type DebounceOptions = SharedLimiterOptions &
{
    wait      : number
    leading?  : boolean
    trailing? : boolean
};

export type LimiterOptions = SharedLimiterOptions &
{
    concurrency?  : number | ConcurrencyLimiterOptions
    rate?         : RateLimiterOptions
    interval?     : number | IntervalLimiterOptions
    tokenBucket?  : TokenBucketLimiterOptions
    capacity?     : number | CapacityLimiterOptions
};

export type LimiterEventHandler = ( task?: TaskSnapshot ) => void;

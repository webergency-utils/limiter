import Heap from '@webergency-utils/heap';
import { LimiterCancelledError, LimiterRejectedError, LimiterTimeoutError } from './errors';
import type { CapacityGate, LimiterGate } from './gates';
import type
{
    ExecuteHandle,
    LimiterEvent,
    LimiterEventHandler,
    QueueOrder,
    RetryIf,
    SharedLimiterOptions,
    TaskAttrs,
    TaskCompare,
    TaskSnapshot
}
from './types';

type TaskState = 'queued' | 'running' | 'settled' | 'cancelled';

type InternalTask =
{
    id           : number
    fn           : () => any
    priority     : number
    difficulty   : number
    timeout?     : number
    deadline?    : number
    readyAt      : number
    retries      : number
    retryDelay   : number
    retryBackoff : number
    retryIf      : RetryIf
    key?         : string
    signal?      : AbortSignal
    sequence     : number
    enqueuedAt   : number
    attempt      : number
    state        : TaskState
    resolve      : ( value: any ) => void
    reject       : ( reason: unknown ) => void
    timer?       : ReturnType<typeof setTimeout>
    abortHandler?: () => void
};

const defaultRetryIf: RetryIf = () => true;

const defaultCompare: TaskCompare = ( a, b ) =>
{
    if( a.priority !== b.priority ){ return b.priority - a.priority }

    return a.sequence - b.sequence;
};

const nextTick = typeof process !== 'undefined' && typeof process.nextTick === 'function'
    ? ( callback: () => unknown ) => process.nextTick( callback )
    : ( callback: () => unknown ) => queueMicrotask( callback );

export class BaseLimiter
{
    #gates          : LimiterGate[];
    #capacityGate   : CapacityGate | undefined;
    #order          : QueueOrder;
    #compare        : TaskCompare;
    #defaultTimeout : number | undefined;
    #defaultDeadline: number | undefined;
    #defaultTaskDelay: number;
    #defaultPriority: number;
    #defaultDifficulty: number;
    #defaultRetries : number;
    #defaultRetryDelay: number;
    #defaultBackoff : number;
    #defaultRetryIf : RetryIf;
    #defaultKey     : string | undefined;
    #defaultSignal  : AbortSignal | undefined;
    #paused         : boolean = false;
    #sequence       : number = 0;
    #taskId         : number = 0;
    #active         : number = 0;
    #activeDifficulty: number = 0;
    #pendingDifficulty: number = 0;
    #fifo           : InternalTask[] = [];
    #heap           : Heap<InternalTask, number> | undefined;
    #listeners      = new Map<LimiterEvent, Set<LimiterEventHandler>>();
    #pumpTimer      : ReturnType<typeof setTimeout> | undefined;
    #pumpScheduled  : boolean = false;
    #draining       : boolean = false;

    constructor( gates: LimiterGate[], options: SharedLimiterOptions = {}, capacityGate?: CapacityGate )
    {
        this.#gates = gates;
        this.#capacityGate = capacityGate;
        this.#order = options.order ?? 'fifo';
        this.#compare = options.compare ?? defaultCompare;
        this.#defaultTimeout = options.timeout;
        this.#defaultDeadline = options.deadline?.getTime();
        this.#defaultTaskDelay = options.delay ?? 0;
        this.#defaultPriority = options.priority ?? 1;
        this.#defaultDifficulty = options.difficulty ?? 1;
        this.#defaultRetries = options.retries ?? 0;
        this.#defaultRetryDelay = options.retryDelay ?? 0;
        this.#defaultBackoff = options.retryBackoff ?? 1;
        this.#defaultRetryIf = options.retryIf ?? defaultRetryIf;
        this.#defaultKey = options.key;
        this.#defaultSignal = options.signal;

        if( this.#order === 'priority' )
        {
            this.#heap = new Heap<InternalTask, number>(
                ( a, b ) => this.#compare( this.#snapshot( a ), this.#snapshot( b )),
                ( task ) => task.id
            );
        }
    }

    get pending(): number
    {
        return this.#order === 'priority' ? ( this.#heap?.size ?? 0 ) : this.#fifo.length;
    }

    get active(): number
    {
        return this.#active;
    }

    get size(): number
    {
        return this.pending + this.active;
    }

    get paused(): boolean
    {
        return this.#paused;
    }

    get pendingDifficulty(): number
    {
        return this.#pendingDifficulty;
    }

    get activeDifficulty(): number
    {
        return this.#activeDifficulty;
    }

    on( event: LimiterEvent, handler: LimiterEventHandler ): this
    {
        let set = this.#listeners.get( event );

        if( !set )
        {
            set = new Set();
            this.#listeners.set( event, set );
        }

        set.add( handler );

        return this;
    }

    off( event: LimiterEvent, handler: LimiterEventHandler ): this
    {
        this.#listeners.get( event )?.delete( handler );

        return this;
    }

    pause(): this
    {
        if( !this.#paused )
        {
            this.#paused = true;
            this.#emit( 'pause' );
        }

        return this;
    }

    resume(): this
    {
        if( this.#paused )
        {
            this.#paused = false;
            this.#emit( 'resume' );
            this.#schedulePumpSoon();
        }

        return this;
    }

    clear(): this
    {
        const tasks = this.#drainQueue();

        for( const task of tasks )
        {
            this.#rejectTask( task, new LimiterCancelledError( 'Limiter queue cleared' ));
        }

        this.#emitEmptyIdle();

        return this;
    }

    async onIdle(): Promise<void>
    {
        if( this.size === 0 ){ return }

        return new Promise(( resolve ) =>
        {
            const handler = () =>
            {
                this.off( 'idle', handler );
                resolve();
            };

            this.on( 'idle', handler );
        });
    }

    wrap<A extends any[], R>( fn: ( ...args: A ) => R | Promise<R>, defaultAttrs?: TaskAttrs ): ( ...args: A ) => ExecuteHandle<Awaited<R>>
    {
        return ( ...args: A ) => this.execute(() => fn( ...args ), defaultAttrs );
    }

    execute<T>( fn: () => T | Promise<T>, attrs: TaskAttrs = {}): ExecuteHandle<Awaited<T>>
    {
        const difficulty = attrs.difficulty ?? this.#defaultDifficulty;

        if( !( difficulty > 0 ))
        {
            return this.#rejectedHandle( new RangeError( 'difficulty must be > 0' ));
        }

        if( this.#capacityGate && !this.#capacityGate.reserve( difficulty ))
        {
            return this.#rejectedHandle( new LimiterRejectedError( 'Limiter capacity exceeded' ), true );
        }

        let cancel!: () => boolean;

        const handle = new Promise<Awaited<T>>(( resolve, reject ) =>
        {
            const enqueuedAt = Date.now();
            const task: InternalTask =
            {
                id              : ++this.#taskId,
                fn,
                priority        : attrs.priority ?? this.#defaultPriority,
                difficulty,
                timeout         : attrs.timeout ?? this.#defaultTimeout,
                deadline        : attrs.deadline?.getTime() ?? this.#defaultDeadline,
                readyAt         : enqueuedAt + ( attrs.delay ?? this.#defaultTaskDelay ),
                retries         : attrs.retries ?? this.#defaultRetries,
                retryDelay      : attrs.retryDelay ?? this.#defaultRetryDelay,
                retryBackoff    : attrs.retryBackoff ?? this.#defaultBackoff,
                retryIf         : attrs.retryIf ?? this.#defaultRetryIf,
                key             : attrs.key ?? this.#defaultKey,
                signal          : attrs.signal ?? this.#defaultSignal,
                sequence        : ++this.#sequence,
                enqueuedAt,
                attempt         : 0,
                state           : 'queued',
                resolve,
                reject
            };

            cancel = () => this.#cancel( task );

            if( task.signal )
            {
                if( task.signal.aborted )
                {
                    this.#capacityGate?.unreserve( difficulty );
                    reject( new LimiterCancelledError( 'Limiter task aborted' ));
                    this.#emit( 'reject', this.#snapshot( task ));

                    return;
                }

                task.abortHandler = () => this.#cancel( task );
                task.signal.addEventListener( 'abort', task.abortHandler, { once: true });
            }

            this.#enqueue( task );
            this.#pendingDifficulty += difficulty;
            this.#emit( 'add', this.#snapshot( task ));
            this.#armTimeout( task );
            this.#schedulePumpSoon();
        }) as ExecuteHandle<Awaited<T>>;

        handle.cancel = () => cancel();

        return handle;
    }

    #rejectedHandle<T = never>( error: Error, emitReject: boolean = false ): ExecuteHandle<T>
    {
        const handle = Promise.reject( error ) as ExecuteHandle<T>;

        handle.cancel = () => false;

        if( emitReject )
        {
            queueMicrotask(() => this.#emit( 'reject' ));
        }

        return handle;
    }

    #snapshot( task: InternalTask ): TaskSnapshot
    {
        return (
        {
            priority    : task.priority,
            difficulty  : task.difficulty,
            sequence    : task.sequence,
            enqueuedAt  : task.enqueuedAt,
            key         : task.key
        });
    }

    #emit( event: LimiterEvent, task?: TaskSnapshot ): void
    {
        const set = this.#listeners.get( event );

        if( !set ){ return }

        for( const handler of set )
        {
            handler( task );
        }
    }

    #enqueue( task: InternalTask ): void
    {
        if( this.#order === 'priority' )
        {
            this.#heap!.push( task );
        }
        else
        {
            this.#fifo.push( task );
        }
    }

    #dequeue(): InternalTask | undefined
    {
        if( this.#order === 'priority' )
        {
            return this.#heap!.pop() as InternalTask | undefined;
        }

        if( this.#order === 'lifo' )
        {
            return this.#fifo.pop();
        }

        return this.#fifo.shift();
    }

    #peek(): InternalTask | undefined
    {
        if( this.#order === 'priority' )
        {
            return this.#heap!.peek() as InternalTask | undefined;
        }

        if( this.#order === 'lifo' )
        {
            return this.#fifo[this.#fifo.length - 1];
        }

        return this.#fifo[0];
    }

    #remove( task: InternalTask ): boolean
    {
        if( task.state !== 'queued' ){ return false }

        if( this.#order === 'priority' )
        {
            return this.#heap!.delete( task );
        }

        const index = this.#fifo.indexOf( task );

        if( index === -1 ){ return false }

        this.#fifo.splice( index, 1 );

        return true;
    }

    #drainQueue(): InternalTask[]
    {
        if( this.#order === 'priority' )
        {
            const tasks = [ ...this.#heap!.values() ];
            this.#heap!.clear();

            return tasks;
        }

        const tasks = this.#fifo;
        this.#fifo = [];

        return tasks;
    }

    #cancel( task: InternalTask ): boolean
    {
        if( task.state !== 'queued' ){ return false }

        if( !this.#remove( task )){ return false }

        this.#pendingDifficulty = Math.max( 0, this.#pendingDifficulty - task.difficulty );
        this.#capacityGate?.unreserve( task.difficulty );
        this.#rejectTask( task, new LimiterCancelledError());
        this.#emitEmptyIdle();
        this.#schedulePumpSoon();

        return true;
    }

    #armTimeout( task: InternalTask ): void
    {
        let due: number | undefined;

        if( task.timeout !== undefined )
        {
            due = task.enqueuedAt + task.timeout;
        }

        if( task.deadline !== undefined )
        {
            due = due === undefined ? task.deadline : Math.min( due, task.deadline );
        }

        if( due === undefined ){ return }

        const remaining = due - Date.now();

        if( remaining <= 0 )
        {
            if( task.state === 'queued' )
            {
                this.#remove( task );
                this.#pendingDifficulty = Math.max( 0, this.#pendingDifficulty - task.difficulty );
                this.#capacityGate?.unreserve( task.difficulty );
                this.#rejectTask( task, new LimiterTimeoutError());
                this.#emitEmptyIdle();
            }

            return;
        }

        task.timer = setTimeout(() =>
        {
            if( task.state === 'queued' )
            {
                this.#remove( task );
                this.#pendingDifficulty = Math.max( 0, this.#pendingDifficulty - task.difficulty );
                this.#capacityGate?.unreserve( task.difficulty );
                this.#rejectTask( task, new LimiterTimeoutError());
                this.#emitEmptyIdle();
                this.#schedulePumpSoon();
            }
            else if( task.state === 'running' )
            {
                this.#rejectTask( task, new LimiterTimeoutError());
            }
        }, remaining );
    }

    #clearTimer( task: InternalTask ): void
    {
        if( task.timer )
        {
            clearTimeout( task.timer );
            task.timer = undefined;
        }
    }

    #clearAbort( task: InternalTask ): void
    {
        if( task.signal && task.abortHandler )
        {
            task.signal.removeEventListener( 'abort', task.abortHandler );
            task.abortHandler = undefined;
        }
    }

    #rejectTask( task: InternalTask, error: unknown ): void
    {
        if( task.state === 'settled' || task.state === 'cancelled' ){ return }

        task.state = task.state === 'queued' ? 'cancelled' : 'settled';
        this.#clearTimer( task );
        this.#clearAbort( task );

        if( error instanceof LimiterCancelledError || error instanceof LimiterTimeoutError || error instanceof LimiterRejectedError )
        {
            this.#emit( 'reject', this.#snapshot( task ));
        }

        task.reject( error );
    }

    #canAcquire( difficulty: number ): boolean
    {
        const acquired: LimiterGate[] = [];

        for( const gate of this.#gates )
        {
            if( gate.tryAcquire( difficulty ))
            {
                acquired.push( gate );
            }
            else
            {
                for( const done of acquired )
                {
                    done.release( difficulty );
                }

                return false;
            }
        }

        return true;
    }

    #releaseGates( difficulty: number ): void
    {
        for( const gate of this.#gates )
        {
            gate.release( difficulty );
        }
    }

    #nextWakeAt( difficulty: number ): number | null
    {
        let wake: number | null = null;

        for( const gate of this.#gates )
        {
            const at = gate.nextAvailableAt( difficulty );

            if( at === null ){ continue }

            wake = wake === null ? at : Math.max( wake, at );
        }

        return wake;
    }

    #schedulePumpSoon(): void
    {
        if( this.#pumpScheduled ){ return }

        this.#pumpScheduled = true;

        nextTick(() =>
        {
            this.#pumpScheduled = false;
            this.#pump();
        });
    }

    #schedulePump( at: number ): void
    {
        const delay = Math.max( 0, at - Date.now());

        if( this.#pumpTimer ){ clearTimeout( this.#pumpTimer ) }

        this.#pumpTimer = setTimeout(() =>
        {
            this.#pumpTimer = undefined;
            this.#pump();
        }, delay );
    }

    #pump(): void
    {
        if( this.#draining || this.#paused ){ return }

        this.#draining = true;

        try
        {
            while( true )
            {
                const next = this.#peek();

                if( !next ){ break }

                const now = Date.now();

                if( next.readyAt > now )
                {
                    this.#schedulePump( next.readyAt );

                    break;
                }

                if( !this.#canAcquire( next.difficulty ))
                {
                    const wake = this.#nextWakeAt( next.difficulty );

                    if( wake !== null ){ this.#schedulePump( wake ) }

                    break;
                }

                const task = this.#dequeue()!;
                this.#pendingDifficulty = Math.max( 0, this.#pendingDifficulty - task.difficulty );
                this.#start( task );
            }
        }
        finally
        {
            this.#draining = false;
        }
    }

    #emitEmptyIdle(): void
    {
        if( this.pending === 0 )
        {
            this.#emit( 'empty' );
        }

        if( this.size === 0 )
        {
            this.#emit( 'idle' );
        }
    }

    #start( task: InternalTask ): void
    {
        task.state = 'running';
        this.#active++;
        this.#activeDifficulty += task.difficulty;
        this.#emit( 'start', this.#snapshot( task ));
        this.#run( task );
    }

    async #run( task: InternalTask ): Promise<void>
    {
        try
        {
            const result = await task.fn();

            if( task.state !== 'running' )
            {
                this.#finish( task, false );

                return;
            }

            task.state = 'settled';
            this.#clearTimer( task );
            this.#clearAbort( task );
            this.#emit( 'success', this.#snapshot( task ));
            this.#emit( 'settle', this.#snapshot( task ));
            task.resolve( result );
            this.#finish( task, true );
        }
        catch( error )
        {
            if( task.state !== 'running' )
            {
                this.#finish( task, false );

                return;
            }

            if( task.attempt < task.retries && task.retryIf( error, task.attempt + 1 ))
            {
                task.attempt++;
                const delayMs = task.retryDelay * Math.pow( task.retryBackoff, task.attempt - 1 );

                await new Promise(( resolve ) => setTimeout( resolve, delayMs ));

                if( task.state !== 'running' )
                {
                    this.#finish( task, false );

                    return;
                }

                this.#run( task );

                return;
            }

            task.state = 'settled';
            this.#clearTimer( task );
            this.#clearAbort( task );
            this.#emit( 'error', this.#snapshot( task ));
            this.#emit( 'settle', this.#snapshot( task ));
            task.reject( error );
            this.#finish( task, true );
        }
    }

    #finish( task: InternalTask, emitIdle: boolean ): void
    {
        this.#active = Math.max( 0, this.#active - 1 );
        this.#activeDifficulty = Math.max( 0, this.#activeDifficulty - task.difficulty );
        this.#capacityGate?.unreserve( task.difficulty );
        this.#releaseGates( task.difficulty );

        if( emitIdle )
        {
            this.#emitEmptyIdle();
        }

        this.#schedulePumpSoon();
    }
}

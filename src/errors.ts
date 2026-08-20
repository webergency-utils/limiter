export class LimiterError extends Error
{
    constructor( message: string )
    {
        super( message );
        this.name = new.target.name;
    }
}

export class LimiterTimeoutError extends LimiterError
{
    constructor( message: string = 'Limiter task timed out' )
    {
        super( message );
    }
}

export class LimiterCancelledError extends LimiterError
{
    constructor( message: string = 'Limiter task cancelled' )
    {
        super( message );
    }
}

export class LimiterRejectedError extends LimiterError
{
    constructor( message: string = 'Limiter rejected task' )
    {
        super( message );
    }
}

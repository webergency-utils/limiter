# Testing Knowledge Base

## Rules

* **AAA Pattern (Arrange, Act, Assert)**: Separate each phase of the test with visual whitespace.
* **Test Isolation**: Instantiate fresh objects in each test or inside `beforeEach`. Never mutate shared variables across test blocks.
* **Coverage Targets**: Ensure all branch conditions (if/else, throw paths, and default fallbacks) are explicitly covered.

## Anti-Patterns

* Do not leak timers or asynchronous handles: clear timeouts, wait for promises, and check that limiter queues are completely drained.
* Avoid stubbing internal states directly; test through the public API boundaries (e.g. `execute()`, `cancel()`, `reset()`).

## Mocking Conventions

* Use `vi.fn()` for callback verification where simple function spies suffice.
* For module isolation and overriding globals (e.g., `process.nextTick`), use `vi.isolateModules()` and clean up changes in `finally` blocks.

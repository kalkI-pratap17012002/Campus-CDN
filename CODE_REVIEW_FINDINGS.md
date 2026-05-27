# Code Review Findings — Campus CDN Project

## High-Effort Review Summary

**Review Scope:** Full codebase analysis with special attention to status.md  
**Effort Level:** High (recall-biased)  
**Total Findings:** 6 confirmed issues (CRITICAL × 3, HIGH × 3)  
**Finding Severity:** Critical issues affect test execution; will prevent CI/CD  

---

## Finding #1: Critical Event Loop Binding — Global Semaphore

**File:** `server/transfer/pool.py`  
**Lines:** 14, 66  
**Severity:** CRITICAL

### Summary
Asyncio Semaphore created at module import time before event loop exists, then reused globally across tests running in different event loops.

### Failure Scenario
- Test 1 runs in event loop A, calls `transfer_pool.acquire()`, semaphore's internal waiter queue binds to loop A
- Test 2 runs in event loop B (pytest-asyncio creates fresh loop), calls `transfer_pool.acquire()`
- Semaphore tries to wake waiter from loop A while in loop B context
- Result: `RuntimeError: Task <...> got Future attached to a different loop`

### Code
```python
class ConnectionPool:
    def __init__(self, max_connections: int | None = None, timeout_seconds: float = 30.0) -> None:
        self._semaphore = asyncio.Semaphore(self.max_connections)  # ← Bound to import-time loop (None)
        self._lock = asyncio.Lock()  # ← Same problem

transfer_pool = ConnectionPool()  # ← Global singleton, created at import time
```

### Root Cause
Asyncio primitives are event-loop-bound. Creating them at module load time (before any loop exists) or in a loop context makes them unusable in other loop contexts. CPython's asyncio will raise when you try to use a primitive from a closed loop.

### Fix Required
1. Don't create Semaphore/Lock at class instantiation in module scope
2. Create a new ConnectionPool per test session, or use lazy initialization
3. Alternatively: use threading.Semaphore (but then lose asyncio semantics)

---

## Finding #2: Critical Engine Lifecycle — No Disposal, Shared Across Event Loops

**File:** `server/database/connection.py`  
**Lines:** 9–14  
**Severity:** CRITICAL

### Summary
SQLAlchemy async engine created globally and never disposed. asyncpg's connection pool binds connections to the event loop that created them. When pytest-asyncio creates a new event loop for the next test, the engine's pool still holds connections from the previous loop.

### Failure Scenario
- Test 1 runs, `async_session_factory()` creates session using global engine
- Connection acquired from asyncpg pool (bound to event loop A)
- Test 1 ends, event loop A closes, but connection remains in engine pool
- Test 2 starts with event loop B
- Test 2 tries to acquire session from same engine → gets connection from closed loop A
- asyncpg detects loop mismatch and raises: `cannot perform operation: another operation is in progress` (or loop-binding error)

### Code
```python
engine = create_async_engine(settings.DATABASE_URL, future=True, echo=False)  # Line 9
# ↑ Global, created once at import; no corresponding dispose() call anywhere
async_session_factory = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)
```

### Root Cause
asyncpg connections are bound to the event loop that created them. When you create a session from `async_session_factory()` in loop B using a connection from pool created in loop A, asyncpg's internal state machine fails because it has callbacks and futures registered on the wrong loop.

### Observed Error
Matches `status.md` lines 100–112: `sqlalchemy.exc.InterfaceError: cannot perform operation: another operation is in progress`

### Fix Required
1. **Option A (Recommended):** Create and dispose engine per test session
   ```python
   @pytest_asyncio.fixture(scope="session")
   async def _engine():
       engine = create_async_engine(settings.DATABASE_URL, ...)
       yield engine
       await engine.dispose()
   ```
2. **Option B:** Configure engine pooling to handle loop changes (advanced, fragile)
3. **Option C:** Use connection string with pool options that expire connections on loop change

---

## Finding #3: Critical Fixture Event Loop Mismatch — Autouse DB Setup

**File:** `tests/conftest.py`  
**Lines:** 70–84 (cleanup_chunks fixture)  
**Severity:** CRITICAL

### Summary
The autouse fixture runs database operations (`await create_tables()`, `await _truncate_database()`) against a global async engine that was created before pytest-asyncio set up the test's event loop.

### Failure Scenario
- conftest.py imports server modules → global engine created in event loop context "None" or whatever loop was active during import
- pytest-asyncio framework sets up fresh event loop for test function
- cleanup_chunks fixture runs its setup phase, calls `await create_tables()`
- create_tables() uses the global engine → tries to use a connection from a loop that's either closed or doesn't match the current test loop
- Result: Connection error during fixture setup, before test even runs

### Code
```python
@pytest_asyncio.fixture(scope="function", autouse=True)
async def cleanup_chunks(redis_client: redis.Redis):
    ...
    await create_tables()  # ← Uses global engine with mismatched loop
    await _truncate_database()  # ← Same problem
    ...
```

### Root Cause
Fixture is async but engine is a global singleton. The engine's event loop binding decision was made at import time, not at fixture time.

### Observation
This is the **exact scenario described in status.md lines 136–148**:
> "fixture setup is triggering overlapping async DB operations on the same connection, which leads to... got Future attached to a different loop"

### Fix Required
1. Move `await create_tables()` out of the autouse fixture
2. Create a session-scoped fixture that runs `create_tables()` once at suite startup
3. Only truncate tables in tests that actually use the database
4. Remove DB setup from tests that don't need it (test_integrity.py, test_pool.py)

---

## Finding #4: High — Concurrent Database DDL Without Serialization

**File:** `tests/conftest.py:74` + `server/database/connection.py:23`  
**Lines:** 74, 23  
**Severity:** HIGH

### Summary
Multiple test functions' `cleanup_chunks` setup phases call `await create_tables()` simultaneously, causing concurrent `CREATE TABLE` DDL on the same tables.

### Failure Scenario
- Test A's fixture begins setup: calls `create_tables()` → `Base.metadata.create_all()` starts CREATE TABLE chunks
- Before Test A's transaction commits, Test B's fixture begins setup: calls `create_tables()` → tries to CREATE TABLE chunks again
- PostgreSQL table-level locks conflict: Test B waits for Test A's lock, but Test A is waiting for something else
- Result: Deadlock or table-already-exists error

### Code
```python
# conftest.py
@pytest_asyncio.fixture(scope="function", autouse=True)
async def cleanup_chunks(...):
    ...
    await create_tables()  # ← Runs for EVERY test, potentially in parallel

# connection.py
async def create_tables() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)  # ← Concurrent DDL
```

### Root Cause
Fixture is function-scoped (runs before each test) and autouse=True (runs for all tests, even those that don't use the DB). With parallel test execution or high concurrency, multiple `create_tables()` calls overlap.

### Fix Required
1. Move `create_tables()` to a session-scoped fixture (runs once per test session, not per test)
2. Don't run DB operations for tests that don't use the database
3. Ensure truncate operations have table-level lock protection

---

## Finding #5: High — Global State Mutation Without Synchronization (edge_cache counters)

**File:** `tests/conftest.py`  
**Lines:** 76–77, 83–84  
**Severity:** HIGH

### Summary
Edge cache hit/miss counters are plain integers modified in the fixture setup/teardown without locks. Concurrent tests can race on these reads/writes.

### Failure Scenario
- Test A's fixture starts: `edge_cache.hit_count = 0` (write)
- Meanwhile, Test A's async code executes: reads `edge_cache.hit_count` (read)
- Between read and use, Test B's fixture runs: writes `edge_cache.hit_count = 0` again
- Test A uses stale value, assertion fails due to observer pattern race condition

### Code
```python
async def cleanup_chunks(redis_client: redis.Redis):
    ...
    edge_cache.hit_count = 0  # ← Plain write, no lock
    edge_cache.miss_count = 0  # ← Plain write, no lock
    yield
    ...
    edge_cache.hit_count = 0  # ← Plain write, no lock
    edge_cache.miss_count = 0  # ← Plain write, no lock
```

### Root Cause
In Python, integer assignment is not atomic at the bytecode level (it's one STORE operation, but cache coherency in concurrent execution is not guaranteed without explicit synchronization).

### Fix Required
1. Use `asyncio.Lock()` to protect counter access (if tests run in single loop)
2. Use `threading.Lock()` (more robust for mixed async/sync)
3. Better: use atomic Counter from `collections` or `threading.Lock`
4. Best: Don't reset counters in fixtures; use mocking or separate cache instances per test

---

## Finding #6: High — Transfer Pool Lock Semantics Inconsistency

**File:** `server/transfer/pool.py`  
**Lines:** 44–53 (release method)  
**Severity:** HIGH

### Summary
Semaphore is released outside the critical section protected by the async lock. This creates a window where concurrent acquire/release operations see inconsistent state.

### Failure Scenario
- Thread A: calls `release(file="f1", slot=5)`
  - Acquires `_lock`, checks `_active_slots[5]`, finds entry, sets removed=True, releases `_lock`
  - Before `_semaphore.release()` is called, Thread B: calls `acquire(file="f2")`
  - Thread B acquires `_lock`, increments `_slot_counter`, adds to `_active_slots`
  - Meanwhile, Thread A calls `_semaphore.release()` (outside lock)
  - Semaphore now allows a waiter to wake up, but `_active_slots` has been modified by Thread B
  - Waiter sees stale or incorrect active slot count

### Code
```python
async def release(self, file_id: str, slot: int) -> None:
    removed = False
    async with self._lock:
        owner = self._active_slots.get(slot)
        if owner is not None:
            removed = True
            self._active_slots.pop(slot, None)
    # ↓ Lock is released here, state is no longer protected
    if removed:
        self._semaphore.release()  # ← Released outside lock!
```

### Root Cause
Lock is released before the semaphore release, violating the atomic operation principle. The `removed` flag is checked outside the lock's protection.

### Fix Required
1. Move `_semaphore.release()` inside the `async with self._lock:` block
2. Or restructure so semaphore and dict state are always consistent

---

## Summary Table

| # | File | Lines | Type | Severity | Status |
|---|------|-------|------|----------|--------|
| 1 | `server/transfer/pool.py` | 14, 66 | Event loop binding | CRITICAL | Must fix |
| 2 | `server/database/connection.py` | 9–14 | Engine lifecycle | CRITICAL | Must fix |
| 3 | `tests/conftest.py` | 70–84 | Fixture event loop mismatch | CRITICAL | Must fix |
| 4 | `tests/conftest.py:74` | 74 | Concurrent DDL | HIGH | Must fix |
| 5 | `tests/conftest.py` | 76–77, 83–84 | Unsync'd counter access | HIGH | Should fix |
| 6 | `server/transfer/pool.py` | 44–53 | Lock semantics | HIGH | Should fix |

---

## Test Output Alignment

These findings directly explain the errors in `status.md` lines 86–125:

- **1 passed, 7 errors** → First test passes because it doesn't use the DB fixture; remaining tests fail on fixture setup
- **RuntimeError: Task got Future attached to a different loop** → Finding #1, #2, #3
- **InterfaceError: cannot perform operation** → Finding #2 (engine reuse across loops)
- **Fixture errors at setup phase** → Findings #2, #3, #4 (all in `create_tables()` call)

---

## Application Logic Assessment

✅ **Core Campus CDN logic is sound:**
- Chunker splits files correctly
- Integrity verification (SHA256) is implemented correctly  
- Transfer pool semaphore concept is correct (but lifecycle is broken)
- Peer discovery UDP protocol is well-designed
- Analytics collection logic is solid

❌ **Infrastructure has critical flaws:**
- Global async singleton management
- Test fixture lifecycle misalignment
- Event loop assumptions

---

## Immediate Next Steps

1. Fix Finding #3 first (remove DB from autouse fixture)
2. Fix Finding #2 (proper engine lifecycle)  
3. Fix Finding #1 (lazy pool initialization)
4. Then re-run: `pytest tests/test_chunker.py tests/test_integrity.py tests/test_pool.py -v`

After these fixes, the test suite should pass, and the application can proceed to integration testing.

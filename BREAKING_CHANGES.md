# Breaking Changes

## Version: PR #3 Fix (2025-09-27)

### Changed Default for `proto` Parameter

The default value for the `proto` parameter in WebSocket classes has been changed from `False` to `True`.

**Before:**
```python
ws = SpotWebSocket(api_key="key", api_secret="secret")  # proto=False by default
```

**After:**
```python
ws = SpotWebSocket(api_key="key", api_secret="secret")  # proto=True by default
```

**Impact:**
- If you were explicitly setting `proto=True`, no changes needed
- If you were relying on the default `proto=False`, you must now explicitly set it:
  ```python
  ws = SpotWebSocket(api_key="key", api_secret="secret", proto=False)
  ```

**Reason for Change:**
The protobuf protocol (`proto=True`) provides better performance and is the recommended way to use MEXC WebSocket API. Making it the default reduces configuration errors and improves out-of-the-box performance.

### Context Manager Support Added (Optional)

WebSocket classes now support async context manager protocol for automatic resource cleanup. **This is optional** - you can still use the classes directly.

**Option 1: Direct Usage (traditional)**
```python
ws = SpotWebSocket(api_key="key", api_secret="secret")
await ws.connect()
await ws.depth_stream(callback, "BTCUSDT")
# Manual cleanup when done
await ws.close_all()
```

**Option 2: Context Manager (automatic cleanup)**
```python
async with SpotWebSocket(api_key="key", api_secret="secret") as ws:
    await ws.depth_stream(callback, "BTCUSDT")
    # WebSocket automatically cleaned up on exit
```

This is not a breaking change but a new **optional** feature that ensures proper resource management when used.

### Migration Guide

1. **If you use proto=False:** Add explicit `proto=False` to your WebSocket initialization
2. **For better resource management:** Consider using the new context manager pattern
3. **No other code changes required** - All other APIs remain backward compatible

## Version: ws-recovery-ownership (2026-08-23)

### Async `_connect` is documented as one attempt per call

**Before:** `_connect_locked` wrapped the handshake in
`while (infinitely_reconnect or retries > 0) and not self.is_connected():`. The loop
never decremented `retries` and a failed `ws_connect` re-raised immediately, so the
actual behavior was already a single attempt.

**After:** the dead loop is deleted; `_connect` makes exactly one attempt per call
and says so. If the setup tail (auth / subscription replay / initial ping) fails
after the socket was published, the partial socket is retired (ws + session closed,
`connected=False`) before the error is re-raised.

**Impact:**
- The `retries` constructor parameter is accepted for API compatibility but does not
  retry on the async path. Reconnect pacing belongs to the caller.
- `is_connected()` no longer reads True after a setup-phase failure.
- `subscribe()` now raises when it is waiting with no connection and no connect in flight; it previously waited indefinitely.

**Reason for Change:** honest contract (basis_fork TASK-29 D4); the retry the
parameter promised never existed on the async path.

### Async task identity split: `_keep_alive_task` is gone

**Before:** one slot (`_keep_alive_task`) held either the ping loop or the spot
listenKey renewal loop. On spot private clients the renewal task occupied it, so the
ping loop never started, and `_on_close` cancelled it, so the first socket close
permanently killed listenKey renewal.

**After:** `_ping_task` (socket-scoped: cancelled in `exit()`, `_on_close`,
`close_all()`, `__aexit__`; `exit()` skips the cancel when invoked from within the
ping task itself, as happens on ping-failure error handling) and
`_listen_key_renewal_task` (spot private client only;
account-scoped: cancelled ONLY in `close_all()` / `__aexit__`). Async spot clients now
send the protocol-valid uppercase ping `{"method": "PING"}`; futures keeps
`{"method": "ping"}`; the sync client is unchanged.

**Impact:** code touching the private `_keep_alive_task` attribute must switch to the
new names.

### Migration Guide

1. Drop any reliance on `retries` for reconnection on the async client; pace
   reconnects in your application.
2. Rename `_keep_alive_task` references: ping loop -> `_ping_task`, spot listenKey
   renewal -> `_listen_key_renewal_task`.
3. If you depended on a socket close stopping listenKey renewal, call `close_all()`
   (or use the async context manager), which now cancels both tasks.

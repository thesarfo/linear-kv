# Linearizable Key-Value Store

Simple in-memory key-value store with linearizable semantics on a single node.

## Features

- **Linearizable Operations**: All operations serialized with a single mutex
- **Idempotent Writes**: Duplicate request IDs detected and not re-executed
- **Timeline Visualization**: Visual representation of operation timing
- **Linearizability Checker**: Validates operation histories for consistency

## API

- PUT /kv
  - Body JSON:
    ```json
    {
      "requestId":"<unique>", 
      "key":"k", "value":"v"
    }
    ```
  - Idempotent: repeating the same `requestId` returns `{ "result": "duplicate" }`

- GET /kv?key=k
  - Optional header `X-Request-ID: <id>` included in history
  - Response:
    ```json
    {
      "key":"k",
      "value":"v",
      "found":true,
      "result":"ok"
    }
    ```

- GET /history - JSON list of all operations with start/end/duration

- GET /timeline - ASCII timeline visualization of operations

- GET /check - Validates current history for linearizability violations

## Examples

```bash
# Write value
curl -sX PUT localhost:8080/kv -H 'Content-Type: application/json' \
  -d '{"requestId":"1","key":"a","value":"x"}'

# Retry same write (duplicate)
curl -sX PUT localhost:8080/kv -H 'Content-Type: application/json' \
  -d '{"requestId":"1","key":"a","value":"x"}'

# Read
curl -s 'localhost:8080/kv?key=a'

# View timeline
curl -s localhost:8080/timeline

# Check linearizability
curl -s localhost:8080/check | jq .
```

## Implementation

- A single `sync.Mutex` serializes all operations; each operation's linearization point is within the critical section.
- Reads never observe stale data because they acquire the same lock before reading.
- Duplicate suppression ensures retried writes are not applied twice.
- Every operation is logged with start/end timestamps for reasoning about histories.



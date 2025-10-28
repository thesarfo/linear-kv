# Linearizable Key-Value Store

Simple in-memory key-value store with linearizable semantics on a single node.

## API

- PUT /kv
  - Body JSON:
    ```json
    {
      "requestId":"<unique>", 
      "key":"k", "value":"v"
    }
    ```
  - Idempotent: repeating the same `requestId` will not re-apply the write and returns `{ "result": "duplicate" }`.

- GET /kv?key=k
  - Optional header `X-Request-ID: <id>` included in history only.
  - Response:
    ```json
    {
      "key":"k",
      "value":"v",
      "found":true,
      "result":"ok"
    }
    ```

- GET /history
  - JSON list of all operations with start/end/duration.

## Examples

```bash
# write value
curl -sX PUT localhost:8080/kv -H 'Content-Type: application/json' \
  -d '{"requestId":"1","key":"a","value":"x"}'

# retry same write (duplicate)
curl -sX PUT localhost:8080/kv -H 'Content-Type: application/json' \
  -d '{"requestId":"1","key":"a","value":"x"}'

# read
curl -s 'localhost:8080/kv?key=a'

# history
curl -s localhost:8080/history | jq .
```

## Other

- A single `sync.Mutex` serializes all ops; each op's linearization point is within the critical section.
- Reads never observe stale data because they acquire the same lock before reading.
- Duplicate suppression ensures retried writes are not applied twice.
- Every operation is logged with start/end timestamps for reasoning about histories.



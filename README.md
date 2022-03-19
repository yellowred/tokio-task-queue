# Tokio Task Queue
[![CI](https://github.com/yellowred/tokio-task-queue/actions/workflows/github-actions.yml/badge.svg)](https://github.com/yellowred/tokio-task-queue/actions/workflows/github-actions.yml)

## Getting started
Start a server:
```
cargo build --release
TASKQUEUE_LOG=info target/release/taskqueue
```

Publish a task:
```
grpcurl -H 'correlation-id: b7b054ca-0d37-418b-ab16-ebe8aa409285' -H 'client-id: b7b054ca-0d37-418b-ab16-ebe8aa409285' -d '{"name": "service", "stream": "service.v1", "parameters": {"url": "http://localhost", "method": "GET", "protocol": "http"}}' -proto ./proto/taskqueue.proto -plaintext localhost:50051 taskqueue.v1.TaskQueue/publish
```

See the result:
```
grpcurl -H 'correlation-id: b7b054ca-0d37-418b-ab16-ebe8aa409285' -d '{"status":"SUCCESS"}' -proto ./proto/taskqueue.proto -plaintext localhost:50051 taskqueue.v1.TaskQueue/list
```

### Tests

Run tests:
```
cargo test
```

### Benchmark (Mac M1)

```
ghz  --insecure --proto ./proto/taskqueue.proto --call taskqueue.v1.TaskQueue/publish -m '{"correlation-id": "b7b054ca-0d37-418b-ab16-ebe8aa409285"}' -d "{\"name\": \"service\", \"stream\": \"loadtest.v1\", \"parameters\": {\"url\": \"http://localhost:3001/bump_counter\", \"method\": \"GET\"}}" -n 100000 -c 100 --connections=100 localhost:8001

Summary:
  Count:	100000
  Total:	5.57 s
  Slowest:	50.40 ms
  Fastest:	0.06 ms
  Average:	1.67 ms
  Requests/sec:	17941.79

Response time histogram:
  0.059  [1]     |
  5.093  [92061] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  10.128 [4585]  |∎∎
  15.162 [2245]  |∎
  20.196 [688]   |
  25.230 [267]   |
  30.264 [100]   |
  35.298 [31]    |
  40.333 [14]    |
  45.367 [5]     |
  50.401 [3]     |

Latency distribution:
  10 % in 0.18 ms
  25 % in 0.30 ms
  50 % in 0.60 ms
  75 % in 1.43 ms
  90 % in 4.18 ms
  95 % in 7.44 ms
  99 % in 15.61 ms

Status code distribution:
  [OK]   100000 responses
```

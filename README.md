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

### Benchmark

```
 ghz  --insecure --proto ./proto/taskqueue.proto --call taskqueue.v1.TaskQueue/publish -m '{"correlation-id": "b7b054ca-0d37-418b-ab16-ebe8aa409285"}' -d "{\"name\": \"service\", \"stream\": \"loadtest.v1\", \"parameters\": {\"url\": \"http://localhost:3001/bump_counter\", \"method\": \"GET\"}}" -n 100000 -c 16 --connections=10 localhost:50051

Summary:
  Count:	100000
  Total:	6.69 s
  Slowest:	13.08 ms
  Fastest:	0.04 ms
  Average:	0.35 ms
  Requests/sec:	14944.19

Response time histogram:
  0.044  [1]     |
  1.348  [97113] |∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
  2.651  [2236]  |∎
  3.955  [351]   |
  5.259  [139]   |
  6.563  [36]    |
  7.866  [16]    |
  9.170  [25]    |
  10.474 [23]    |
  11.777 [48]    |
  13.081 [12]    |

Latency distribution:
  10 % in 0.10 ms
  25 % in 0.13 ms
  50 % in 0.21 ms
  75 % in 0.38 ms
  90 % in 0.70 ms
  95 % in 1.03 ms
  99 % in 2.18 ms

Status code distribution:
  [OK]   100000 responses
```

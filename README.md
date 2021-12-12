# Tokio Task Queue

## Getting started
Start a server:
```
TASKQUEUE_LOG=info cargo run
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

Run tests including E2E:
```
cargo test --features e2e
```

Run only E2E tests:
```
cargo test e2e_tests::tests --features e2e
```


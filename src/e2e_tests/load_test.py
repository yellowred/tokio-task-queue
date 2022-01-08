import subprocess
from random import randint
import time

tt_start = time.perf_counter_ns()
for i in range(1):
    subprocess.run(["grpcurl", "-H", "correlation-id: b7b054ca-0d37-418b-ab16-ebe8aa409285", "-H", "client-id: b7b054ca-0d37-418b-ab16-ebe8aa409285", "-d", "{\"name\": \"service\", \"stream\": \"iris.loadtest.v1\", \"parameters\": {\"url\": \"http://host.docker.internal:3001/bump_counter\", \"method\": \"GET\"}}", "-proto", "../../proto/taskqueue.proto", "-plaintext", "0.0.0.0:50051", "taskqueue.v1.TaskQueue/publish"])

tt_stop = time.perf_counter_ns()
print("Elapsed time:",(tt_stop-tt_start)/1000000000)
# V10 Full-Chain Load Test: 6,000 WebSocket Connections

## Summary

On 2026-08-10, V10 was tested locally with three server processes, 2,000
WebSocket clients per process, and the Redis, Kafka, MySQL, and consumer path
enabled. The run sustained 6,000 concurrent connections and approximately
2,182 inbound danmaku messages per second without client errors, rate limits,
server overload responses, WebSocket ingress drops, or Redis errors.

Kafka persistence remained healthy at the broker/protocol level, but the
non-blocking producer admission policy dropped approximately 6.0% of messages
before enqueueing them. This is the first observed bottleneck for this workload.

## Test topology

- Host: Windows 11, AMD Ryzen 9 8945HX, 16 cores / 32 threads, approximately
  32 GiB RAM
- Load generator: WSL2 Ubuntu using `v10/bench-v10-linux`
- Servers: three V10 processes on ports `18081`, `18082`, and `18083`
- Connections: 2,000 per server, 6,000 total
- Rooms: 2,000 room IDs per load-generator process
- Active client ratio: 20%
- Mean send interval for active clients: 500 ms
- Test duration: 30 seconds
- Client ramp interval: 2 ms
- Shared middleware: Redis, Kafka, MySQL, and two consumers
- AI functionality: excluded

Each server was started with Redis and Kafka enabled and both connection limits
set to 12,000. The benchmark command for each port was equivalent to:

```bash
./v10/bench-v10-linux \
  -port=18081 \
  -clients=2000 \
  -rooms=2000 \
  -active=0.2 \
  -interval=500ms \
  -duration=30s \
  -ramp=2ms
```

## Results

| Server | Connections | Sent | Average inbound QPS | Received | P50 | P95 | P99 | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `18081` | 2,000 | 20,683 | 689 | 65,460 | 1.51 ms | 2.45 ms | 4.13 ms | 0 |
| `18082` | 2,000 | 21,924 | 731 | 65,460 | 1.50 ms | 2.44 ms | 4.10 ms | 0 |
| `18083` | 2,000 | 22,855 | 762 | 65,460 | 1.51 ms | 2.45 ms | 4.24 ms | 0 |
| **Total** | **6,000** | **65,462** | **2,182** | **196,380** | - | - | **4.24 ms max** | **0** |

`Received` counts client-visible deliveries. Redis distributes messages across
the three server processes, so a single inbound danmaku may be observed by load
generators connected to more than one server. The aggregate delivery rate was
approximately 6,546 deliveries per second.

No benchmark instance reported rate limiting or server overload. Server metrics
also reported:

- WebSocket ingress drops: 0
- Broadcast job drops: 0
- Slow-client disconnects: 0
- Redis publish errors: 0
- Redis queue backlog after the run: 0
- Kafka producer errors: 0
- Kafka status: healthy

## Kafka persistence bottleneck

The stage added approximately 3,938 Kafka admission drops across the three
servers:

| Server | Kafka admission drops during stage |
| --- | ---: |
| `18081` | 1,173 |
| `18082` | 1,343 |
| `18083` | 1,422 |
| **Total** | **3,938** |

Compared with 65,462 inbound messages, this is an observed persistence drop
rate of approximately **6.0%**. The broker reported no producer errors; the
drops occurred at the application's bounded, non-blocking producer input path.
This behavior is intentional for the tested realtime-first policy: WebSocket
delivery and Redis fan-out are protected from Kafka backpressure, while Kafka
persistence is best effort under sustained overload.

## Interpretation

This run demonstrates stable realtime handling at 6,000 concurrent WebSocket
connections, approximately 2.18K inbound message QPS, approximately 6.55K
client-visible deliveries per second, and a worst observed P99 of 4.24 ms on
the tested machine. It does not demonstrate lossless Kafka persistence. For a
lossless requirement, producer backpressure, durable buffering, or additional
Kafka capacity must be introduced and tested separately.


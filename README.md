# Teton Challenge, Real-time Streaming Backend

## Stack and storage

**Compute:** Plain async Python on uvicorn and uvloop. With five routes I skipped the web framework. One worker fell behind at 50k/s, so four share the port.

**Storage:** One append-only JSONL file, a format I've used at Meow Wolf. Every worker appends to it and reads new lines every 5ms. The OS appends each line in one piece, so writers never collide. The state we serve covers the last hour and fits in memory. A database would buy durable acks and alarm history beyond an hour. Neither is needed yet, and a database would load from the log.

**Transport:** JSON over HTTP for ingest. Parsing is under a tenth of the cost per event, so protobuf would save little. SSE for the alarm feed, so clients can resume with `Last-Event-ID`.

## Ordering and late events

Health and occupancy keep state sorted by `ts`, so a late event fixes history on the next read. We reject events more than an hour from our clock.

- **Health:** Each device keeps a sorted list of seconds with a heartbeat in the last 5 minutes. The last heartbeat only changes for a newer `ts`.
- **Occupancy:** Each room keeps its presence changes sorted by `ts`, inserted with a binary search. For a window we add up the milliseconds the room was occupied.
- **Alarms:** A dict keyed on device id and `ts` drops repeated falls. The feed stays in arrival order. Clients resume from a `Last-Event-ID` bookmark, and a bookmark only works on a list that grows at the end.

## Backpressure

A POST only parses the event, appends it to the log and returns 202. State catches up within a few ms. Above about 59k/s requests wait but aren't dropped. No 202 means not logged, so the client retries. I didn't prioritize `fall_warn` over heartbeats: a 10x burst from 5,000 devices is 50k/s, so nothing waits.

## Restart correctness

The log is written before the 202, so a kill loses nothing we acknowledged. Every minute one worker snapshots state and its log position. Startup loads the snapshot and replays only the log after it, so a 4M-event log restarts in 0.3s instead of 8s. We don't fsync, so a power cut could lose the last moments. SSE clients reconnect with `Last-Event-ID` and get every alarm they missed. A browser's `EventSource` does this for free.

## How to run it locally

```bash
# needs uv: https://docs.astral.sh/uv/
rm -rf data                      # start empty
make run                         # 4 workers on :8080
make adversarial DEVICES=5000    # in a second terminal; or smoke, baseline, burst, offline
```

## Reported metrics

Measured on an M1 Max laptop with our load tester in `loadgen/`.

- Sustained ingest rate: 50k events/s for 90s with 0 failed. About 59k/s flat out.
- Alarm feed latency p50 / p95: 5ms / 13ms at 50k/s.
- Behavior under hard kill + restart: back in 0.3s after kill -9, with no acknowledged event lost. SSE clients resume and miss no alarms. POSTs sent during those 0.3s get no 202 and need a retry.
- Aggregation correctness on replayed events: all five provided scenarios at 5,000 devices return one alarm per distinct fall. After a kill -9, every room and device on all four workers matches a brute-force recount of the log (`eval/recount.py`).

## With another week

At hundreds of thousands of devices I'd give each region its own deployment, so patient data stays in the country. Within a region I'd partition by facility, since alarms are watched per facility. Kafka partitioned by facility would replace the shared file, with a database behind it for alarm history.

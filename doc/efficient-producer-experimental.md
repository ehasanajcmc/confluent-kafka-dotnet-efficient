# Efficient experimental producer: allocation notes

This note records the caller-side requirements for using `ProduceEfficient` with no per-message managed allocations in the payload path. The API is experimental and synchronous.

## Caller responsibilities

- Configure the efficient delivery-report handler and slot capacity once when building the producer.
- Keep background polling enabled when using `ProduceEfficient` (the default). The producer rejects efficient delivery-report configuration when `dotnet.producer.enable.background.poll` is `false`. With manual polling, `ProduceEfficient` can block waiting for a free report slot while the sending thread is unable to call `Poll` and dispatch the callback that frees that slot.
- Serialize key and value into reusable caller-owned buffers, then pass nullable `ReadOnlyMemory<byte>` values to `ProduceEfficient`. Pass `null` for a null key or value (a null value is a Kafka tombstone); empty memory means a present, zero-length field. Avoid per-message `ToByteArray()`, new arrays, or other serializers that allocate on every call.
- Keep the topic string reusable as well; do not construct it for each message.
- Do not add headers on this path. `SafeKafkaHandle.marshalHeaders` converts each header key with `UTF8Encoding.UTF8.GetBytes`, which creates a managed byte array. It also creates and destroys a native librdkafka headers list. The efficient API currently sends no headers.
- The current API also uses Kafka's default partition and timestamp. It has no headers argument.
- Do not use efficient delivery reports with dependent producers created from the same producer handle. Both producers share the delivery-report callback, which interprets message opaque values differently.

## What the Kafka path does

- `SafeKafkaHandle.Produce` pins each non-null memory span with `fixed` for the native produce call. A null memory passes a null pointer; empty but present memory passes a non-null pointer with length zero. This does not create a per-message `GCHandle` or copy the payload into a managed array.
- The call uses librdkafka's `MSG_F_COPY` flag, so librdkafka copies the payload before the call returns. The caller may reuse its buffers as soon as `ProduceEfficient` returns.
- `Librdkafka.produceva` converts the topic string using `Marshal.StringToHGlobalAnsi` and frees that unmanaged memory after the call. This is a native allocation, not a managed-heap allocation that causes a .NET GC cycle. Therefore the path's goal is zero per-message managed allocations, not literally zero allocations of every kind.
- The delivery report is a callback-scoped `ref struct`; use its fields during the callback and do not retain it. The typed delivery-report key is retained in the preallocated slot until that callback completes.

## Efficient-only lifecycle

Slot shutdown and cleanup should be conditional on efficient mode being configured. The legacy producer path should not acquire the efficient lifecycle lock or check efficient shutdown state on every send.

## Serial-send benchmark

Measured with real data over 10,000,000 sends per approach. Efficient sending was 1.375x faster by total duration and allocated no managed bytes in this run. This benchmark covers serial sending only; parallel multi-thread sending has not been tested, and its added synchronization may affect results.

```text
Metric                                         Legacy              Efficient
----------------------------------------------------------------------------
Total duration (ms)                        49,853.444             36,258.466
Produce calls (ms)                          16,890.525             14,013.552
Delivery polling (ms)                       32,962.075             22,244.108
Messages / second                              200,588                275,798
Allocated bytes (total)                  4,800,079,664                      0
  Sending thread bytes                     800,000,000                      0
  Delivery thread bytes                  4,000,079,664                      0
    Produce calls bytes                    800,000,000                      0
    Sender wait bytes                                0                      0
Allocated bytes / message                      480.008                  0.000
Allocated objects (exact)        N/A - profiler needed  N/A - profiler needed
GC collections: Gen 0                              382                      0
GC collections: Gen 1                               86                      0
GC collections: Gen 2                                0                      0
Delivered                                   10,000,000             10,000,000
Delivery errors                                      0                      0
Legacy / efficient duration: 1.375x
```

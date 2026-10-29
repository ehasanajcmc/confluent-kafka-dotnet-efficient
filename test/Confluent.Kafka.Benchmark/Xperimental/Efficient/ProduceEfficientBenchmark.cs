// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.
//
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;

namespace Confluent.Kafka.Benchmark.Xperimental.Efficient
{
    internal static class ProduceEfficientBenchmark
    {
        private const int WARMUP_MESSAGE_COUNT = 10_000;
        private const int MAX_IN_FLIGHT = 10_000;
        private static readonly CultureInfo _culture = CultureInfo.InvariantCulture;

        public static void Run(
            string bootstrapServers,
            string topic,
            int[] messageCounts,
            int messageSize,
            string username,
            string password,
            string outputPath,
            bool legacyFirst = false)
        {
            if (messageCounts == null || messageCounts.Length == 0)
            {
                throw new ArgumentException("At least one message count is required.", nameof(messageCounts));
            }

            foreach (int count in messageCounts)
            {
                if (count <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(messageCounts), "Message counts must be positive.");
                }
            }

            if (messageSize < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(messageSize));
            }

            // Serialization and buffer creation are outside every measurement. Both approaches reuse these arrays.
            byte[] keyBytes = new byte[messageSize];
            byte[] valueBytes = new byte[messageSize];
            for (int i = 0; i < messageSize; ++i)
            {
                keyBytes[i] = (byte)i;
                valueBytes[i] = (byte)(i + 1);
            }

            using var csv = outputPath == null ? null : new StreamWriter(outputPath, false);
            if (csv != null)
            {
                csv.WriteLine("Messages,Approach,KeyBytes,ValueBytes,DurationSeconds,ProduceSeconds,DeliveryWaitSeconds,MessagesPerSecond,AllocatedBytes,ProduceAllocatedBytes,DeliveryAllocatedBytes,BytesPerMessage,Gen0,Gen1,Gen2,Delivered,Errors,SenderAllocatedBytes,SenderWaitAllocatedBytes,AllocatedObjects,Pattern,MaxInFlight");
                csv.Flush();
            }

            Console.WriteLine($"Topic: {topic}; reused key/value: {messageSize:N0} bytes each; maximum in flight: {MAX_IN_FLIGHT:N0}.");
            Console.WriteLine("Continuous sending with the same sliding in-flight limit for both approaches; no batch drains, one final drain.");
            Console.WriteLine("Produce time measures the send loop excluding explicit capacity waits; it includes loop and capacity-check overhead.");
            Console.WriteLine($"Each approach warms up with {WARMUP_MESSAGE_COUNT:N0} sends before each measured run.");
            Console.WriteLine("Duration includes sending and waiting for all background delivery reports; setup, warmup, and printing are excluded.");
            Console.WriteLine("Managed allocation bytes combine the sending thread and the dedicated background delivery thread; GC counts cover the process.");
            Console.WriteLine("Object counts are not calculated by this benchmark. Optional profiler traces require manual analysis; native allocations are not measured.");
            Console.WriteLine();

            for (int index = 0; index < messageCounts.Length; ++index)
            {
                int messageCount = messageCounts[index];
                BenchmarkResult legacyResult;
                BenchmarkResult efficientResult;

                // Alternate execution order across scenarios to reduce a consistent first-run advantage.
                if ((index % 2 == 0) != legacyFirst)
                {
                    Console.WriteLine($"Running {messageCount:N0} sends per approach: efficient, then legacy...");
                    efficientResult = RunEfficient(bootstrapServers, topic, messageCount, keyBytes, valueBytes, username, password);
                    legacyResult = RunLegacy(bootstrapServers, topic, messageCount, keyBytes, valueBytes, username, password);
                }
                else
                {
                    Console.WriteLine($"Running {messageCount:N0} sends per approach: legacy, then efficient...");
                    legacyResult = RunLegacy(bootstrapServers, topic, messageCount, keyBytes, valueBytes, username, password);
                    efficientResult = RunEfficient(bootstrapServers, topic, messageCount, keyBytes, valueBytes, username, password);
                }

                PrintComparison(messageCount, legacyResult, efficientResult);
                if (csv != null)
                {
                    WriteCsv(csv, messageCount, messageSize, "Legacy", legacyResult);
                    WriteCsv(csv, messageCount, messageSize, "Efficient", efficientResult);
                    csv.Flush();
                }
            }

            if (outputPath != null)
            {
                Console.WriteLine($"CSV saved to: {Path.GetFullPath(outputPath)}");
            }
        }

        public static void RunAllocationProfile(
            string bootstrapServers, string topic, int messageCount, int messageSize, string username, string password,
            string approach, string resultPath)
        {
            try
            {
                WriteProfileResult(resultPath + ".started", Environment.ProcessId.ToString(_culture));
                if (messageCount <= 0 || messageSize < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(messageCount), "A positive count and nonnegative message size are required.");
                }

                byte[] keyBytes = new byte[messageSize];
                byte[] valueBytes = new byte[messageSize];
                for (int i = 0; i < messageSize; ++i)
                {
                    keyBytes[i] = (byte)i;
                    valueBytes[i] = (byte)(i + 1);
                }

                BenchmarkResult result;
                string measuredFunction;
                if (approach == "legacy")
                {
                    measuredFunction = nameof(AllocationProfileLegacy);
                    result = RunLegacy(bootstrapServers, topic, messageCount, keyBytes, valueBytes, username, password, true);
                }
                else if (approach == "efficient")
                {
                    measuredFunction = nameof(AllocationProfileEfficient);
                    result = RunEfficient(bootstrapServers, topic, messageCount, keyBytes, valueBytes, username, password, true);
                }
                else
                {
                    throw new ArgumentException("Approach must be legacy or efficient.", nameof(approach));
                }

                // Write completion only after the producer is disposed and all measured delivery reports have arrived.
                string json = JsonSerializer.Serialize(new
                {
                    Succeeded = true,
                    Approach = approach,
                    Messages = messageCount,
                    MeasuredFunction = measuredFunction,
                    Delivered = result.Delivered,
                    Errors = result.Errors,
                    AllocatedBytes = result.AllocatedBytes,
                    SenderAllocatedBytes = result.SendingThreadAllocatedBytes,
                    DeliveryThreadAllocatedBytes = result.DeliveryThreadAllocatedBytes,
                    ObjectCounts = "Not automatically calculated. Sender wrapper totals exclude background delivery allocations; analyze both threads in the trace.",
                    Timing = "Profiler run; use the normal efficient benchmark for timing comparisons."
                });
                WriteProfileResult(resultPath, json);
            }
            catch (Exception exception)
            {
                WriteProfileResult(resultPath, JsonSerializer.Serialize(new { Succeeded = false, Error = exception.ToString() }));
                throw;
            }
        }

        private static void WriteProfileResult(string resultPath, string json)
        {
            string pendingPath = resultPath + ".pending";
            File.WriteAllText(pendingPath, json);
            File.Move(pendingPath, resultPath);
        }

        // Preserve profiler root frames; warmup calls the underlying passes directly and is excluded from these call trees.
        [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
        private static PhaseMeasurements AllocationProfileEfficient(
            IProducer<int, byte[]> producer, string topic, byte[] keyBytes, byte[] valueBytes, int messageCount, DeliveryStatistics statistics)
        {
            return RunEfficientPass(producer, topic, keyBytes, valueBytes, messageCount, statistics);
        }

        [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
        private static PhaseMeasurements AllocationProfileLegacy(
            IProducer<byte[], byte[]> producer, string topic, Message<byte[], byte[]> message,
            Action<DeliveryReport<byte[], byte[]>> handler, int messageCount, DeliveryStatistics statistics)
        {
            return RunLegacyPass(producer, topic, message, handler, messageCount, statistics);
        }

        private static BenchmarkResult RunEfficient(
            string bootstrapServers, string topic, int messageCount, byte[] keyBytes, byte[] valueBytes, string username, string password, bool allocationProfile = false)
        {
            var statistics = new DeliveryStatistics();
            using var producer = new ProducerBuilder<int, byte[]>(CreateProducerConfig(bootstrapServers, username, password))
                .SetDeliveryReportEfficient(MAX_IN_FLIGHT, statistics.HandleEfficient)
                .Build();

            RunEfficientPass(producer, topic, keyBytes, valueBytes, WARMUP_MESSAGE_COUNT, statistics);
            EnsureSuccessfulPass(statistics, WARMUP_MESSAGE_COUNT, "efficient warmup");
            statistics.Reset();

            PrepareForMeasurement();
            int generation0Before = GC.CollectionCount(0);
            int generation1Before = GC.CollectionCount(1);
            int generation2Before = GC.CollectionCount(2);
            long deliveryAllocatedBytesBefore = statistics.CallbackAllocatedBytes;
            long allocatedBytesBefore = GC.GetAllocatedBytesForCurrentThread();
            long startedAt = Stopwatch.GetTimestamp();

            PhaseMeasurements phases = allocationProfile
                ? AllocationProfileEfficient(producer, topic, keyBytes, valueBytes, messageCount, statistics)
                : RunEfficientPass(producer, topic, keyBytes, valueBytes, messageCount, statistics);

            long elapsedTicks = Stopwatch.GetTimestamp() - startedAt;
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBytesBefore;
            long deliveryAllocatedBytes = statistics.CallbackThreadId == Environment.CurrentManagedThreadId
                ? 0
                : statistics.CallbackAllocatedBytes - deliveryAllocatedBytesBefore;
            var result = new BenchmarkResult(
                elapsedTicks, allocatedBytes, deliveryAllocatedBytes, phases,
                GC.CollectionCount(0) - generation0Before,
                GC.CollectionCount(1) - generation1Before,
                GC.CollectionCount(2) - generation2Before,
                statistics.Completed, statistics.ErrorCount);

            EnsureSuccessfulPass(statistics, messageCount, "efficient measured run");
            return result;
        }

        private static BenchmarkResult RunLegacy(
            string bootstrapServers, string topic, int messageCount, byte[] keyBytes, byte[] valueBytes, string username, string password, bool allocationProfile = false)
        {
            var statistics = new DeliveryStatistics();
            var message = new Message<byte[], byte[]> { Key = keyBytes, Value = valueBytes, Headers = new Headers() };
            Action<DeliveryReport<byte[], byte[]>> handler = statistics.HandleLegacy;
            using var producer = new ProducerBuilder<byte[], byte[]>(CreateProducerConfig(bootstrapServers, username, password))
                .SetKeySerializer(Serializers.ByteArray)
                .SetValueSerializer(Serializers.ByteArray)
                .Build();

            RunLegacyPass(producer, topic, message, handler, WARMUP_MESSAGE_COUNT, statistics);
            EnsureSuccessfulPass(statistics, WARMUP_MESSAGE_COUNT, "legacy warmup");
            statistics.Reset();

            PrepareForMeasurement();
            int generation0Before = GC.CollectionCount(0);
            int generation1Before = GC.CollectionCount(1);
            int generation2Before = GC.CollectionCount(2);
            long deliveryAllocatedBytesBefore = statistics.CallbackAllocatedBytes;
            long allocatedBytesBefore = GC.GetAllocatedBytesForCurrentThread();
            long startedAt = Stopwatch.GetTimestamp();

            PhaseMeasurements phases = allocationProfile
                ? AllocationProfileLegacy(producer, topic, message, handler, messageCount, statistics)
                : RunLegacyPass(producer, topic, message, handler, messageCount, statistics);

            long elapsedTicks = Stopwatch.GetTimestamp() - startedAt;
            long allocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBytesBefore;
            long deliveryAllocatedBytes = statistics.CallbackThreadId == Environment.CurrentManagedThreadId
                ? 0
                : statistics.CallbackAllocatedBytes - deliveryAllocatedBytesBefore;
            var result = new BenchmarkResult(
                elapsedTicks, allocatedBytes, deliveryAllocatedBytes, phases,
                GC.CollectionCount(0) - generation0Before,
                GC.CollectionCount(1) - generation1Before,
                GC.CollectionCount(2) - generation2Before,
                statistics.Completed, statistics.ErrorCount);

            EnsureSuccessfulPass(statistics, messageCount, "legacy measured run");
            return result;
        }

        private static ProducerConfig CreateProducerConfig(string bootstrapServers, string username, string password)
        {
            return new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                QueueBufferingMaxMessages = 2000000,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 500,
                LingerMs = 5,
                DeliveryReportFields = "none",
                EnableBackgroundPoll = true,
                SaslUsername = username,
                SaslPassword = password,
                SecurityProtocol = username == null ? SecurityProtocol.Plaintext : SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain
            };
        }

        private static PhaseMeasurements RunEfficientPass(
            IProducer<int, byte[]> producer, string topic, byte[] keyBytes, byte[] valueBytes, int messageCount, DeliveryStatistics statistics)
        {
            var phases = new PhaseMeasurements();
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            long startedAt = Stopwatch.GetTimestamp();

            for (int sent = 0; sent < messageCount; ++sent)
            {
                // Release one credit per completed report instead of draining a whole batch.
                // Use the same gate on both paths, outside the measured produce work.
                if (sent - statistics.Completed >= MAX_IN_FLIGHT)
                {
                    WaitUntilCompleted(statistics, sent - MAX_IN_FLIGHT + 1, ref phases);
                }

                producer.ProduceEfficient(topic, sent, keyBytes, valueBytes);
            }

            phases.ProduceTicks = Stopwatch.GetTimestamp() - startedAt - phases.DeliveryTicks;
            phases.ProduceAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore - phases.DeliveryAllocatedBytes;
            WaitUntilCompleted(statistics, messageCount, ref phases);
            return phases;
        }

        private static PhaseMeasurements RunLegacyPass(
            IProducer<byte[], byte[]> producer, string topic, Message<byte[], byte[]> message,
            Action<DeliveryReport<byte[], byte[]>> handler, int messageCount, DeliveryStatistics statistics)
        {
            var phases = new PhaseMeasurements();
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            long startedAt = Stopwatch.GetTimestamp();

            for (int sent = 0; sent < messageCount; ++sent)
            {
                if (sent - statistics.Completed >= MAX_IN_FLIGHT)
                {
                    WaitUntilCompleted(statistics, sent - MAX_IN_FLIGHT + 1, ref phases);
                }

                producer.Produce(topic, message, handler);
            }

            phases.ProduceTicks = Stopwatch.GetTimestamp() - startedAt - phases.DeliveryTicks;
            phases.ProduceAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore - phases.DeliveryAllocatedBytes;
            WaitUntilCompleted(statistics, messageCount, ref phases);
            return phases;
        }

        private static void WaitUntilCompleted(
            DeliveryStatistics statistics, int targetCount, ref PhaseMeasurements phases)
        {
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            long startedAt = Stopwatch.GetTimestamp();
            long timeoutTicks = 120 * Stopwatch.Frequency;
            var spinner = new SpinWait();
            while (statistics.Completed < targetCount)
            {
                // Callbacks remain exclusively on the background delivery thread.
                // Spin briefly, then yield/sleep using SpinWait's adaptive policy.
                spinner.SpinOnce();
                if (Stopwatch.GetTimestamp() - startedAt >= timeoutTicks)
                {
                    throw new TimeoutException($"Only {statistics.Completed} of {targetCount} delivery reports arrived within 120 seconds.");
                }
            }

            phases.DeliveryTicks += Stopwatch.GetTimestamp() - startedAt;
            phases.DeliveryAllocatedBytes += GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
        }

        private static void EnsureSuccessfulPass(DeliveryStatistics statistics, int expectedCount, string passName)
        {
            if (statistics.CallbackThreadChanged)
            {
                throw new InvalidOperationException("The delivery callback changed threads; allocation counters cannot be combined reliably.");
            }

            if (statistics.Completed != expectedCount || statistics.ErrorCount != 0)
            {
                throw new InvalidOperationException(
                    $"{passName}: {statistics.Completed}/{expectedCount} reports, {statistics.ErrorCount} errors; first error: {statistics.FirstError}.");
            }
        }

        private static void PrepareForMeasurement()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        private static void PrintComparison(int messageCount, BenchmarkResult legacy, BenchmarkResult efficient)
        {
            Console.WriteLine();
            Console.WriteLine($"{messageCount:N0} sends per approach");
            Console.WriteLine($"{"Metric",-30} {"Legacy",22} {"Efficient",22}");
            Console.WriteLine(new string('-', 76));
            PrintRow("Total duration (ms)", legacy.ElapsedSeconds * 1000, efficient.ElapsedSeconds * 1000, "N3");
            PrintRow("Produce calls (ms)", legacy.Phases.ProduceTicks * 1000.0 / Stopwatch.Frequency, efficient.Phases.ProduceTicks * 1000.0 / Stopwatch.Frequency, "N3");
            PrintRow("Capacity wait + drain (ms)", legacy.Phases.DeliveryTicks * 1000.0 / Stopwatch.Frequency, efficient.Phases.DeliveryTicks * 1000.0 / Stopwatch.Frequency, "N3");
            PrintRow("Messages / second", messageCount / legacy.ElapsedSeconds, messageCount / efficient.ElapsedSeconds, "N0");
            PrintRow("Allocated bytes (total)", legacy.AllocatedBytes, efficient.AllocatedBytes, "N0");
            PrintRow("  Sending thread bytes", legacy.SendingThreadAllocatedBytes, efficient.SendingThreadAllocatedBytes, "N0");
            PrintRow("  Delivery thread bytes", legacy.DeliveryThreadAllocatedBytes, efficient.DeliveryThreadAllocatedBytes, "N0");
            PrintRow("    Produce calls bytes", legacy.Phases.ProduceAllocatedBytes, efficient.Phases.ProduceAllocatedBytes, "N0");
            PrintRow("    Sender wait bytes", legacy.Phases.DeliveryAllocatedBytes, efficient.Phases.DeliveryAllocatedBytes, "N0");
            PrintRow("Allocated bytes / message", legacy.AllocatedBytes / (double)messageCount, efficient.AllocatedBytes / (double)messageCount, "N3");
            Console.WriteLine($"{"Allocated objects (exact)",-30} {"N/A - profiler needed",22} {"N/A - profiler needed",22}");
            PrintRow("GC collections: Gen 0", legacy.Generation0Collections, efficient.Generation0Collections, "N0");
            PrintRow("GC collections: Gen 1", legacy.Generation1Collections, efficient.Generation1Collections, "N0");
            PrintRow("GC collections: Gen 2", legacy.Generation2Collections, efficient.Generation2Collections, "N0");
            PrintRow("Delivered", legacy.Delivered, efficient.Delivered, "N0");
            PrintRow("Delivery errors", legacy.Errors, efficient.Errors, "N0");
            Console.WriteLine($"Legacy / efficient duration: {legacy.ElapsedSeconds / efficient.ElapsedSeconds:F3}x");
            Console.WriteLine();
        }

        private static void PrintRow(string label, double legacy, double efficient, string format)
        {
            Console.WriteLine($"{label,-30} {legacy.ToString(format, _culture),22} {efficient.ToString(format, _culture),22}");
        }

        private static void WriteCsv(StreamWriter writer, int messageCount, int messageSize, string approach, BenchmarkResult result)
        {
            writer.WriteLine(string.Join(",", new[]
            {
                messageCount.ToString(_culture), approach,
                messageSize.ToString(_culture), messageSize.ToString(_culture),
                result.ElapsedSeconds.ToString("R", _culture),
                (result.Phases.ProduceTicks / (double)Stopwatch.Frequency).ToString("R", _culture),
                (result.Phases.DeliveryTicks / (double)Stopwatch.Frequency).ToString("R", _culture),
                (messageCount / result.ElapsedSeconds).ToString("R", _culture),
                result.AllocatedBytes.ToString(_culture),
                result.Phases.ProduceAllocatedBytes.ToString(_culture),
                result.DeliveryThreadAllocatedBytes.ToString(_culture),
                (result.AllocatedBytes / (double)messageCount).ToString("R", _culture),
                result.Generation0Collections.ToString(_culture),
                result.Generation1Collections.ToString(_culture),
                result.Generation2Collections.ToString(_culture),
                result.Delivered.ToString(_culture), result.Errors.ToString(_culture),
                result.SendingThreadAllocatedBytes.ToString(_culture),
                result.Phases.DeliveryAllocatedBytes.ToString(_culture), "N/A", "Streaming", MAX_IN_FLIGHT.ToString(_culture)
            }));
        }

        private struct PhaseMeasurements
        {
            public long ProduceTicks;
            public long DeliveryTicks;
            public long ProduceAllocatedBytes;
            public long DeliveryAllocatedBytes;
        }

        private readonly struct BenchmarkResult
        {
            public BenchmarkResult(
                long elapsedTicks, long allocatedBytes, long deliveryThreadAllocatedBytes, PhaseMeasurements phases,
                int generation0Collections, int generation1Collections, int generation2Collections, int delivered, int errors)
            {
                ElapsedSeconds = elapsedTicks / (double)Stopwatch.Frequency;
                SendingThreadAllocatedBytes = allocatedBytes;
                DeliveryThreadAllocatedBytes = deliveryThreadAllocatedBytes;
                Phases = phases;
                Generation0Collections = generation0Collections;
                Generation1Collections = generation1Collections;
                Generation2Collections = generation2Collections;
                Delivered = delivered;
                Errors = errors;
            }

            public double ElapsedSeconds { get; }
            public long SendingThreadAllocatedBytes { get; }
            public long DeliveryThreadAllocatedBytes { get; }
            public long AllocatedBytes => SendingThreadAllocatedBytes + DeliveryThreadAllocatedBytes;
            public PhaseMeasurements Phases { get; }
            public int Generation0Collections { get; }
            public int Generation1Collections { get; }
            public int Generation2Collections { get; }
            public int Delivered { get; }
            public int Errors { get; }
        }

        private sealed class DeliveryStatistics
        {
            private int _completed;
            private int _callbackThreadId;
            private bool _callbackThreadChanged;
            private long _callbackAllocatedBytes;

            public int Completed => Volatile.Read(ref _completed);
            public int CallbackThreadId => Volatile.Read(ref _callbackThreadId);
            public bool CallbackThreadChanged => Volatile.Read(ref _callbackThreadChanged);
            public long CallbackAllocatedBytes => Volatile.Read(ref _callbackAllocatedBytes);
            public int ErrorCount { get; private set; }
            public ErrorCode FirstError { get; private set; }

            public void HandleEfficient(DeliveryReportEfficient<int> report)
            {
                if (report.ErrorCode != ErrorCode.NoError)
                {
                    if (ErrorCount == 0)
                    {
                        FirstError = report.ErrorCode;
                    }

                    ++ErrorCount;
                }

                CompleteReport();
            }

            public void HandleLegacy(DeliveryReport<byte[], byte[]> report)
            {
                if (report.Error.IsError)
                {
                    if (ErrorCount == 0)
                    {
                        FirstError = report.Error.Code;
                    }

                    ++ErrorCount;
                }

                CompleteReport();
            }

            private void CompleteReport()
            {
                int threadId = Environment.CurrentManagedThreadId;
                if (_callbackThreadId == 0)
                {
                    _callbackThreadId = threadId;
                }
                else if (_callbackThreadId != threadId)
                {
                    _callbackThreadChanged = true;
                }

                // The cumulative counter includes allocations made before entering this handler,
                // including construction of legacy delivery reports. Keep the warmup snapshot on Reset.
                Volatile.Write(ref _callbackAllocatedBytes, GC.GetAllocatedBytesForCurrentThread());
                Interlocked.Increment(ref _completed);
            }

            public void Reset()
            {
                ErrorCount = 0;
                FirstError = ErrorCode.NoError;
                Volatile.Write(ref _completed, 0);
            }
        }
    }
}

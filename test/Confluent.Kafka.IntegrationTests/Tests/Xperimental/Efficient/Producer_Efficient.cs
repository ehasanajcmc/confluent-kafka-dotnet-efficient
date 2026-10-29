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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        private const int EfficientAllocationBatchSize = 64;
        private const int EfficientAllocationMessageCount = 512;

        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Efficient_SendsBytesAndReports(string bootstrapServers)
        {
            using var testTopic = new TemporaryTopic(bootstrapServers, 1);
            string[] messageKeys =
            {
                "efficient-key-0",
                "efficient-key-1",
                string.Empty
            };
            byte[][] keyBytes =
            {
                Encoding.UTF8.GetBytes(messageKeys[0]),
                Encoding.UTF8.GetBytes(messageKeys[1]),
                Array.Empty<byte>()
            };
            byte[][] valueBytes =
            {
                new byte[] { 1, 2, 3 },
                new byte[] { 4, 5, 6, 7 },
                Array.Empty<byte>()
            };

            var reports = new EfficientDeliveryReportCapture<string>(messageKeys.Length);
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableBackgroundPoll = true,
                EnableDeliveryReports = false,
                DeliveryReportFields = "none"
            };

            using (var producer = new ProducerBuilder<string, byte[]>(producerConfig)
                .SetDeliveryReportEfficient(messageKeys.Length, reports.Handle)
                .Build())
            {
                for (int i = 0; i < messageKeys.Length; ++i)
                {
                    producer.ProduceEfficient(testTopic.Name, messageKeys[i], keyBytes[i], valueBytes[i]);
                }

                WaitForEfficientDeliveryReports(
                    reports,
                    messageKeys.Length,
                    TimeSpan.FromSeconds(15));
            }

            Assert.Equal(messageKeys.Length, reports.Count);
            Assert.False(reports.Overflowed);

            for (int i = 0; i < messageKeys.Length; ++i)
            {
                Assert.Equal(messageKeys[i], reports.KeyAt(i));
                Assert.Equal(ErrorCode.NoError, reports.ErrorCodeAt(i));
                Assert.False(reports.IsFatalAt(i));
                Assert.Equal(0, reports.ErrorReasonLengthAt(i));
                Assert.Equal(testTopic.Name, reports.TopicAt(i));
                Assert.Equal((Partition)0, reports.PartitionAt(i));
                Assert.Equal(valueBytes[i].Length, reports.PayloadLengthAt(i));
            }

            Assert.True(reports.OffsetAt(0) >= 0);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false
            };

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                .SetKeyDeserializer(Deserializers.ByteArray)
                .SetValueDeserializer(Deserializers.ByteArray)
                .Build())
            {
                consumer.Assign(new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset(testTopic.Name, 0, Offset.Beginning)
                });

                for (int i = 0; i < messageKeys.Length; ++i)
                {
                    var consumed = consumer.Consume(TimeSpan.FromSeconds(10));
                    Assert.NotNull(consumed);

                    Assert.Equal(keyBytes[i], consumed.Message.Key);
                    Assert.Equal(valueBytes[i], consumed.Message.Value);
                }
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Efficient_PreservesNullAndEmptyFields(string bootstrapServers)
        {
            using var testTopic = new TemporaryTopic(bootstrapServers, 1);
            using var reportReceived = new ManualResetEventSlim();
            var reports = new EfficientDeliveryReportCapture<string>(3, reportReceived);
            var keyBytes = Encoding.UTF8.GetBytes("non-null-key");
            var valueBytes = new byte[] { 1, 2, 3 };
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableBackgroundPoll = true,
                EnableDeliveryReports = false,
                DeliveryReportFields = "none"
            };

            using (var producer = new ProducerBuilder<string, byte[]>(producerConfig)
                .SetDeliveryReportEfficient(3, reports.Handle)
                .Build())
            {
                producer.ProduceEfficient(testTopic.Name, "tombstone", keyBytes, null);
                Assert.True(reportReceived.Wait(TimeSpan.FromSeconds(15)));
                reportReceived.Reset();

                producer.ProduceEfficient(testTopic.Name, "null-key", null, valueBytes);
                Assert.True(reportReceived.Wait(TimeSpan.FromSeconds(15)));
                reportReceived.Reset();

                producer.ProduceEfficient(
                    testTopic.Name,
                    "empty-fields",
                    ReadOnlyMemory<byte>.Empty,
                    ReadOnlyMemory<byte>.Empty);
                Assert.True(reportReceived.Wait(TimeSpan.FromSeconds(15)));
            }

            Assert.Equal(3, reports.Count);
            Assert.False(reports.Overflowed);
            Assert.Equal("tombstone", reports.KeyAt(0));
            Assert.Equal("null-key", reports.KeyAt(1));
            Assert.Equal("empty-fields", reports.KeyAt(2));
            Assert.Equal(ErrorCode.NoError, reports.ErrorCodeAt(0));
            Assert.Equal(ErrorCode.NoError, reports.ErrorCodeAt(1));
            Assert.Equal(ErrorCode.NoError, reports.ErrorCodeAt(2));
            Assert.Equal(0, reports.PayloadLengthAt(0));
            Assert.Equal(valueBytes.Length, reports.PayloadLengthAt(1));
            Assert.Equal(0, reports.PayloadLengthAt(2));

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false
            };

            using (var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                .SetKeyDeserializer(Deserializers.ByteArray)
                .SetValueDeserializer(Deserializers.ByteArray)
                .Build())
            {
                consumer.Assign(new List<TopicPartitionOffset>
                {
                    new TopicPartitionOffset(testTopic.Name, 0, Offset.Beginning)
                });

                var tombstone = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(tombstone);
                Assert.Equal(keyBytes, tombstone.Message.Key);
                Assert.Null(tombstone.Message.Value);

                var nullKey = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(nullKey);
                Assert.Null(nullKey.Message.Key);
                Assert.Equal(valueBytes, nullKey.Message.Value);

                var emptyFields = consumer.Consume(TimeSpan.FromSeconds(10));
                Assert.NotNull(emptyFields);
                Assert.NotNull(emptyFields.Message.Key);
                Assert.Empty(emptyFields.Message.Key);
                Assert.NotNull(emptyFields.Message.Value);
                Assert.Empty(emptyFields.Message.Value);
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Efficient_UsesBackgroundPollingWhenLegacyReportsAreDisabled(string bootstrapServers)
        {
            using var testTopic = new TemporaryTopic(bootstrapServers, 1);
            using var reportReceived = new ManualResetEventSlim();
            var reports = new EfficientDeliveryReportCapture<string>(1, reportReceived);
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableBackgroundPoll = true,
                EnableDeliveryReports = false,
                DeliveryReportFields = "all"
            };

            using (var producer = new ProducerBuilder<string, byte[]>(producerConfig)
                .SetDeliveryReportEfficient(1, reports.Handle)
                .Build())
            {
                producer.ProduceEfficient(
                    testTopic.Name,
                    "background-poll-key",
                    new byte[] { 1, 2, 3 },
                    new byte[] { 4, 5, 6 });

                Assert.True(reportReceived.Wait(TimeSpan.FromSeconds(15)));
            }

            Assert.Equal(1, reports.Count);
            Assert.Equal("background-poll-key", reports.KeyAt(0));
            Assert.Equal(ErrorCode.NoError, reports.ErrorCodeAt(0));
            Assert.Equal(testTopic.Name, reports.TopicAt(0));
            Assert.Equal(3, reports.PayloadLengthAt(0));
        }
        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Efficient_RequiresConfigurationAndRejectsLegacyProduce(string bootstrapServers)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableBackgroundPoll = true
            };

            using (var producer = new ProducerBuilder<int, byte[]>(producerConfig).Build())
            {
                Assert.Throws<InvalidOperationException>(() =>
                    producer.ProduceEfficient(
                        singlePartitionTopic,
                        1,
                        new byte[] { 1 },
                        new byte[] { 2 }));
            }

            var reports = new EfficientDeliveryReportCapture<int>(1);
            using (var producer = new ProducerBuilder<int, byte[]>(producerConfig)
                .SetDeliveryReportEfficient(1, reports.Handle)
                .Build())
            {
                Assert.Throws<InvalidOperationException>(() =>
                    producer.Produce(
                        singlePartitionTopic,
                        new Message<int, byte[]> { Key = 1, Value = new byte[] { 2 } }));
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public void Producer_Efficient_ProduceEfficientDoesNotAllocateOnCallingThread(string bootstrapServers)
        {
            const int warmupMessageCount = EfficientAllocationBatchSize;
            var reports = new EfficientDeliveryReportCapture<int>(
                warmupMessageCount + EfficientAllocationMessageCount);
            var keyBytes = new byte[] { 1, 2, 3, 4 };
            var valueBytes = new byte[] { 5, 6, 7, 8 };
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                EnableBackgroundPoll = true,
                DeliveryReportFields = "none",
                LingerMs = 0
            };

            using (var producer = new ProducerBuilder<int, byte[]>(producerConfig)
                .SetDeliveryReportEfficient(EfficientAllocationBatchSize, reports.Handle)
                .Build())
            {
                for (int i = 0; i < warmupMessageCount; ++i)
                {
                    producer.ProduceEfficient(singlePartitionTopic, i, keyBytes, valueBytes);
                }

                WaitForEfficientDeliveryReports(
                    reports,
                    warmupMessageCount,
                    TimeSpan.FromSeconds(15));
                Assert.Equal(warmupMessageCount, reports.Count);

                reports.Reset();
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                long allocatedBytes = 0;

                for (int batchStart = 0;
                    batchStart < EfficientAllocationMessageCount;
                    batchStart += EfficientAllocationBatchSize)
                {
                    // Measure only producer submission on this thread. Delivery callbacks run on the background poll thread.
                    long allocatedBytesBeforeBatch = GC.GetAllocatedBytesForCurrentThread();
                    int batchEnd = Math.Min(
                        batchStart + EfficientAllocationBatchSize,
                        EfficientAllocationMessageCount);

                    for (int i = batchStart; i < batchEnd; ++i)
                    {
                        producer.ProduceEfficient(singlePartitionTopic, i, keyBytes, valueBytes);
                    }

                    allocatedBytes += GC.GetAllocatedBytesForCurrentThread() - allocatedBytesBeforeBatch;

                    WaitForEfficientDeliveryReports(
                        reports,
                        batchEnd,
                        TimeSpan.FromSeconds(15));
                }

                Assert.Equal(EfficientAllocationMessageCount, reports.Count);
                Assert.False(reports.Overflowed);

                for (int i = 0; i < EfficientAllocationMessageCount; ++i)
                {
                    Assert.Equal(i, reports.KeyAt(i));
                    Assert.Equal(ErrorCode.NoError, reports.ErrorCodeAt(i));
                }

                Assert.Equal(0L, allocatedBytes);
            }
        }

        [Theory, MemberData(nameof(SaslPlainKafkaParameters))]
        public void Producer_Efficient_PreservesKeyOnDeliveryFailure(
            string bootstrapServers,
            string admin,
            string adminSecret,
            string user,
            string userSecret)
        {
            string topicName = "dotnet_efficient_failure_" + Guid.NewGuid().ToString("N");
            var reports = new EfficientDeliveryReportCapture<string>(1);
            var acl = new AclBinding
            {
                Pattern = new ResourcePattern
                {
                    Type = ResourceType.Topic,
                    Name = topicName,
                    ResourcePatternType = ResourcePatternType.Literal
                },
                Entry = new AccessControlEntry
                {
                    Principal = $"User:{user}",
                    Host = "*",
                    Operation = AclOperation.Describe,
                    PermissionType = AclPermissionType.Allow
                }
            };

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = admin,
                SaslPassword = adminSecret
            }).Build())
            {
                bool topicCreated = false;
                bool aclCreated = false;

                try
                {
                    adminClient.CreateTopicsAsync(new[]
                    {
                        new TopicSpecification
                        {
                            Name = topicName,
                            NumPartitions = 1,
                            ReplicationFactor = 1
                        }
                    }).Wait();
                    topicCreated = true;

                    adminClient.CreateAclsAsync(new[] { acl }).Wait();
                    aclCreated = true;

                    var producerConfig = new ProducerConfig
                    {
                        BootstrapServers = bootstrapServers,
                        SecurityProtocol = SecurityProtocol.SaslPlaintext,
                        SaslMechanism = SaslMechanism.Plain,
                        SaslUsername = user,
                        SaslPassword = userSecret,
                        EnableBackgroundPoll = true,
                        MessageTimeoutMs = 10000
                    };

                    using (var producer = new ProducerBuilder<string, byte[]>(producerConfig)
                        .SetDeliveryReportEfficient(1, reports.Handle)
                        .Build())
                    {
                        producer.ProduceEfficient(
                            topicName,
                            "rejected-key",
                            Encoding.UTF8.GetBytes("rejected-key"),
                            new byte[] { 9, 8, 7 });

                        WaitForEfficientDeliveryReports(
                            reports,
                            1,
                            TimeSpan.FromSeconds(20));
                    }

                    Assert.Equal(1, reports.Count);
                    Assert.Equal("rejected-key", reports.KeyAt(0));
                    Assert.NotEqual(ErrorCode.NoError, reports.ErrorCodeAt(0));
                    Assert.False(reports.IsFatalAt(0));
                    Assert.Equal(topicName, reports.TopicAt(0));
                    Assert.Equal(3, reports.PayloadLengthAt(0));
                    Assert.True(reports.ErrorReasonLengthAt(0) > 0);
                    Assert.True(reports.ErrorReasonLengthAt(0) > 3);
                }
                finally
                {
                    if (aclCreated)
                    {
                        adminClient.DeleteAclsAsync(new[] { acl.ToFilter() }).Wait();
                    }

                    if (topicCreated)
                    {
                        adminClient.DeleteTopicsAsync(new[] { topicName }).Wait();
                    }
                }
            }
        }

        private static void WaitForEfficientDeliveryReports<TKey>(
            EfficientDeliveryReportCapture<TKey> reports,
            int expectedCount,
            TimeSpan timeout)
        {
            Assert.True(
                reports.WaitForCount(expectedCount, timeout),
                $"Timed out waiting for {expectedCount} efficient delivery reports; received {reports.Count}.");
        }

        private sealed class EfficientDeliveryReportCapture<TKey>
        {
            private readonly TKey[] keys;
            private readonly ErrorCode[] errorCodes;
            private readonly bool[] fatalFlags;
            private readonly string[] topics;
            private readonly Partition[] partitions;
            private readonly Offset[] offsets;
            private readonly int[] payloadLengths;
            private readonly int[] errorReasonLengths;
            private readonly ManualResetEventSlim completed;
            private readonly object sync = new object();
            private int count;
            private bool overflowed;

            public EfficientDeliveryReportCapture(int capacity, ManualResetEventSlim completed = null)
            {
                this.keys = new TKey[capacity];
                this.errorCodes = new ErrorCode[capacity];
                this.fatalFlags = new bool[capacity];
                this.topics = new string[capacity];
                this.partitions = new Partition[capacity];
                this.offsets = new Offset[capacity];
                this.payloadLengths = new int[capacity];
                this.errorReasonLengths = new int[capacity];
                this.completed = completed;
            }

            public int Count => Volatile.Read(ref this.count);

            public bool Overflowed
            {
                get
                {
                    lock (this.sync)
                    {
                        return this.overflowed;
                    }
                }
            }

            public void Handle(DeliveryReportEfficient<TKey> report)
            {
                lock (this.sync)
                {
                    int index = this.count;
                    if (index >= this.keys.Length)
                    {
                        this.overflowed = true;
                        return;
                    }

                    this.keys[index] = report.Key;
                    this.errorCodes[index] = report.ErrorCode;
                    this.fatalFlags[index] = report.IsFatal;
                    this.topics[index] = report.Topic;
                    this.partitions[index] = report.Partition;
                    this.offsets[index] = report.Offset;
                    this.payloadLengths[index] = report.PayloadLength;
                    this.errorReasonLengths[index] = report.ErrorReason.Length;
                    Volatile.Write(ref this.count, index + 1);
                    this.completed?.Set();
                    Monitor.PulseAll(this.sync);
                }
            }

            public void Reset()
            {
                lock (this.sync)
                {
                    Volatile.Write(ref this.count, 0);
                    this.overflowed = false;
                }
            }

            public bool WaitForCount(int expectedCount, TimeSpan timeout)
            {
                long deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);
                lock (this.sync)
                {
                    while (this.count < expectedCount)
                    {
                        long remainingTicks = deadline - Stopwatch.GetTimestamp();
                        if (remainingTicks <= 0)
                        {
                            return false;
                        }

                        TimeSpan remaining = TimeSpan.FromSeconds((double)remainingTicks / Stopwatch.Frequency);
                        Monitor.Wait(this.sync, remaining);
                    }

                    return true;
                }
            }

            public TKey KeyAt(int index) => this.keys[index];

            public ErrorCode ErrorCodeAt(int index) => this.errorCodes[index];

            public bool IsFatalAt(int index) => this.fatalFlags[index];

            public string TopicAt(int index) => this.topics[index];

            public Partition PartitionAt(int index) => this.partitions[index];

            public Offset OffsetAt(int index) => this.offsets[index];

            public int PayloadLengthAt(int index) => this.payloadLengths[index];

            public int ErrorReasonLengthAt(int index) => this.errorReasonLengths[index];

        }
    }
}

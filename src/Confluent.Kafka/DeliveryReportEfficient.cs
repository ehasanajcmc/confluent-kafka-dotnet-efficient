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

using System;

namespace Confluent.Kafka
{
#region Efficient Experimental
    /// <summary>
    ///     Handles an efficient producer delivery report. The report is valid only for the duration of the callback.
    /// </summary>
    /// <typeparam name="TKey">The application key type associated with the produced message.</typeparam>
    /// <param name="deliveryReport">The callback-scoped delivery report.</param>
    public delegate void DeliveryReportEfficientHandler<TKey>(DeliveryReportEfficient<TKey> deliveryReport);

    /// <summary>
    ///     A callback-scoped delivery report for <see cref="IProducer{TKey,TValue}.ProduceEfficient" />.
    ///     Successful reports avoid managed allocations.
    /// </summary>
    /// <typeparam name="TKey">The application key type associated with the produced message.</typeparam>
    public readonly ref struct DeliveryReportEfficient<TKey>
    {
        /// <summary>
        ///     Gets the application key supplied to <see cref="IProducer{TKey,TValue}.ProduceEfficient" />.
        /// </summary>
        public TKey Key { get; }

        /// <summary>
        ///     Gets the librdkafka error code, or <see cref="ErrorCode.NoError" /> on success.
        /// </summary>
        public ErrorCode ErrorCode { get; }

        /// <summary>
        ///     Gets whether the error is fatal.
        /// </summary>
        public bool IsFatal { get; }

        /// <summary>
        ///     Gets the raw UTF-8 error reason. The span is valid only during the delivery report callback.
        /// </summary>
        public ReadOnlySpan<byte> ErrorReason { get; }

        /// <summary>
        ///     Gets the topic to which the message was sent.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        ///     Gets the partition assigned to the message.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        ///     Gets the offset assigned to the message.
        /// </summary>
        public Offset Offset { get; }

        /// <summary>
        ///     Gets the original payload byte length, regardless of delivery outcome.
        /// </summary>
        public int PayloadLength { get; }

        internal DeliveryReportEfficient(
            TKey key,
            ErrorCode errorCode,
            bool isFatal,
            ReadOnlySpan<byte> errorReason,
            string topic,
            Partition partition,
            Offset offset,
            int payloadLength)
        {
            Key = key;
            ErrorCode = errorCode;
            IsFatal = isFatal;
            ErrorReason = errorReason;
            Topic = topic;
            Partition = partition;
            Offset = offset;
            PayloadLength = payloadLength;
        }
    }
#endregion
}

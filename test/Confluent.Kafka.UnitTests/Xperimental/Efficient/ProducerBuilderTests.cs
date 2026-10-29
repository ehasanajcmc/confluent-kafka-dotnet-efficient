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
using Xunit;

namespace Confluent.Kafka.UnitTests.Xperimental.Efficient
{
    public class ProducerBuilderTests
    {
        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void SetDeliveryReportEfficient_RejectsNonPositiveCapacity(int capacity)
        {
            var builder = new ProducerBuilder<int, byte[]>(new ProducerConfig());
            DeliveryReportEfficientHandler<int> handler = _ => { };

            Assert.Throws<ArgumentOutOfRangeException>(() =>
                builder.SetDeliveryReportEfficient(capacity, handler));
        }

        [Fact]
        public void SetDeliveryReportEfficient_RejectsNullHandler()
        {
            var builder = new ProducerBuilder<int, byte[]>(new ProducerConfig());

            Assert.Throws<ArgumentNullException>(() =>
                builder.SetDeliveryReportEfficient(1, null));
        }

        [Fact]
        public void SetDeliveryReportEfficient_ReturnsBuilderForChaining()
        {
            var builder = new ProducerBuilder<int, byte[]>(new ProducerConfig());
            DeliveryReportEfficientHandler<int> handler = _ => { };

            Assert.Same(builder, builder.SetDeliveryReportEfficient(4, handler));
        }
    }
}

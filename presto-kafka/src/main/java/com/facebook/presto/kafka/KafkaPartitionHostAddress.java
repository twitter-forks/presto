/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka;

import com.facebook.presto.spi.HostAddress;

import java.util.Objects;

public class KafkaPartitionHostAddress
{
    public final int partitionId;
    public final HostAddress hostAddress;

    public KafkaPartitionHostAddress(
            int partitionId,
            HostAddress hostAddress)
    {
        this.partitionId = partitionId;
        this.hostAddress = hostAddress;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(
                hostAddress.toString().length() + Integer.toString(partitionId).length() + 11);
        builder.append(String.format("%d-%s", partitionId, hostAddress.getHostText()));
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final KafkaPartitionHostAddress other = (KafkaPartitionHostAddress) obj;
        return Objects.equals(this.partitionId, other.partitionId) &&
                Objects.equals(this.hostAddress, other.hostAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId, hostAddress.getHostText(), hostAddress.getPort());
    }
}

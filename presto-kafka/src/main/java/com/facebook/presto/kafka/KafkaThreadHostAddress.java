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

public class KafkaThreadHostAddress
{
    public final String threadName;
    public final HostAddress hostAddress;

    public KafkaThreadHostAddress(
            String threadName,
            HostAddress hostAddress)
    {
        this.threadName = threadName;
        this.hostAddress = hostAddress;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(hostAddress.toString().length() + threadName.length() + 11);
        builder.append(String.format("%s-%s", threadName, hostAddress.getHostText()));
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
        final KafkaThreadHostAddress other = (KafkaThreadHostAddress) obj;
        return Objects.equals(this.threadName, other.threadName) &&
                Objects.equals(this.hostAddress, other.hostAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(threadName, hostAddress.getHostText(), hostAddress.getPort());
    }
}

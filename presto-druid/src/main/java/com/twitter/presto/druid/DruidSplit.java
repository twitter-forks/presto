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
package com.twitter.presto.druid;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;

import java.util.List;

import static java.lang.String.format;

public class DruidSplit
        implements ConnectorSplit
{
    @Override
    public boolean isRemotelyAccessible()
    {
        throw new UnsupportedOperationException(format("Unimplemented method: %s", new Object().getClass().getEnclosingClass().getName()));
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        throw new UnsupportedOperationException(format("Unimplemented method: %s", new Object().getClass().getEnclosingClass().getName()));
    }

    @Override
    public Object getInfo()
    {
        throw new UnsupportedOperationException(format("Unimplemented method: %s", new Object().getClass().getEnclosingClass().getName()));
    }
}

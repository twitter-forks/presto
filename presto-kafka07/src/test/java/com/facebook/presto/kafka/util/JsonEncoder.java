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

package com.facebook.presto.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import kafka.message.Message;
import kafka.serializer.Encoder;

import java.io.IOException;

public class JsonEncoder
        implements Encoder<Object>
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message toMessage(Object o)
    {
        try {
            return new Message(objectMapper.writeValueAsBytes(o));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}

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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleSessionProperty;

public final class KafkaSessionProperties
{
    /**
     * If we further research into how sampling could work better(for example, jumping randomly in the segment instead of just sampling first few percent),
     * then sampling_only is a global feature flag, where sampling_percent or another new session var sampling_jump could be minor feature flag
     */
    private static final String SAMPLING_ONLY = "sampling_only";
    private static final String SAMPLING_PERCENT = "sampling_percent";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KafkaSessionProperties()
    {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(
                        SAMPLING_ONLY,
                        "Sample kafka segments instead of scanning them entirely",
                        false,
                        false),
                doubleSessionProperty(
                        SAMPLING_PERCENT,
                        "The first x percent will be sampled, and the rest of the data will be discarded",
                        0.1,
                        false
                ));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isSamplingOnly(ConnectorSession session)
    {
        return session.getProperty(SAMPLING_ONLY, Boolean.class);
    }

    public static double getSamplingPercent(ConnectorSession session)
    {
        return session.getProperty(SAMPLING_PERCENT, Double.class);
    }
}

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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;

public class BlackHoleConnector
        implements Connector
{
    public static final String SPLIT_COUNT_PROPERTY = "split_count";
    public static final String PAGES_PER_SPLIT_PROPERTY = "pages_per_split";
    public static final String ROWS_PER_PAGE_PROPERTY = "rows_per_page";

    private final BlackHoleMetadata metadata;
    private final BlackHoleHandleResolver connectorHandleResolver;
    private final BlackHoleSplitManager splitManager;
    private final BlackHolePageSourceProvider pageSourceProvider;
    private final BlackHolePageSinkProvider pageSinkProvider;

    public BlackHoleConnector(BlackHoleMetadata metadata,
            BlackHoleHandleResolver connectorHandleResolver,
            BlackHoleSplitManager splitManager,
            BlackHolePageSourceProvider pageSourceProvider,
            BlackHolePageSinkProvider pageSinkProvider)
    {
        this.metadata = metadata;
        this.connectorHandleResolver = connectorHandleResolver;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.pageSinkProvider = pageSinkProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return connectorHandleResolver;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return ImmutableList.of(
                integerSessionProperty(SPLIT_COUNT_PROPERTY, "Number of splits generated by this table", 0, false),
                integerSessionProperty(PAGES_PER_SPLIT_PROPERTY, "Number of pages per each split generated by this table", 0, false),
                integerSessionProperty(ROWS_PER_PAGE_PROPERTY, "Number of rows per each page generated by this table", 0, false));
    }
}

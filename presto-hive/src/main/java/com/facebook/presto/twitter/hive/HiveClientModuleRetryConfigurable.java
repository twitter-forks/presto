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
package com.facebook.presto.twitter.hive;

import com.facebook.presto.hive.HiveClientModule;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HiveClientModuleRetryConfigurable
        extends HiveClientModule
{
    private final String connectorId;
    private final HiveMetastore metastore;

    public HiveClientModuleRetryConfigurable(
        String connectorId, HiveMetastore metastore, TypeManager typeManager, PageIndexerFactory pageIndexerFactory, NodeManager nodeManager)
    {
        super(connectorId, metastore, typeManager, pageIndexerFactory, nodeManager);
        this.connectorId = connectorId;
        this.metastore = metastore;
    }

    @Override
    protected void bindMetastore(Binder binder)
    {
        if (metastore != null) {
            binder.bind(HiveMetastore.class).toInstance(metastore);
        }
        else {
            configBinder(binder).bindConfig(HiveClientRetryConfig.class);
            binder.bind(HiveMetastore.class).to(CachingHiveMetastoreRetryConfigurable.class).in(Scopes.SINGLETON);
            newExporter(binder).export(HiveMetastore.class)
                    .as(generatedNameOf(CachingHiveMetastoreRetryConfigurable.class, connectorId));
        }
    }
}

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
package com.facebook.presto.twitter.hive.util;

import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Utility class to handle creating and caching the UserGroupInformation object.
 */
public class TTransportPool
{
    private static final Logger log = Logger.get(TTransportPool.class);
    private final ConcurrentMap<String, ObjectPool<TTransport>> pools = new ConcurrentHashMap();

    public TTransportPool(){}

    private void add(String remote, PooledObjectFactory transportFactory)
    {
        log.debug("Added new pool for destination: %s", remote);
        pools.put(remote, new GenericObjectPool<TTransport>(transportFactory));
    }

    protected TTransport get(String remote, PooledObjectFactory transportFactory)
        throws Exception
    {
        ObjectPool<TTransport> pool = pools.get(remote);
        if (pool == null) {
            add(remote, transportFactory);
            pool = pools.get(remote);
        }
        log.debug("Fetched transport pool for : %s", remote);
        return pool.borrowObject();
    }

    protected TTransport get(String remote)
        throws Exception
    {
        ObjectPool<TTransport> pool = pools.get(remote);
        if (pool == null) {
            log.debug("Doesn't have transport for : %s", remote);
            return null;
        }

        log.debug("Fetched transport pool for : %s", remote);
        return pool.borrowObject();
    }

    public TTransport borrowObject(String host, int port, PooledObjectFactory transportFactory)
        throws Exception
    {
        return get(HostAndPort.fromParts(host, port).toString(), transportFactory);
    }

    public TTransport borrowObject(String host, int port)
        throws Exception
    {
        return get(HostAndPort.fromParts(host, port).toString());
    }

    public void returnObject(String remote, TSocket socket)
    {
        log.debug("Return a socket to: %s", remote);
        if (remote == null) {
            socket.close();
            log.debug("Remote is null");
            return;
        }
        ObjectPool<TTransport> pool = pools.get(remote);
        if (pool == null) {
            socket.close();
            log.debug("Cannot find pool");
            return;
        }
        try {
            pool.returnObject(socket);
        }
        catch (Exception e) {
            log.debug("Got an error when return to pool: %s", e.getMessage());
        }
    }

    public void returnObject(String remote, TTransport transport)
    {
        returnObject(transport);
    }

    public void returnObject(TTransport transport)
    {
        log.debug("Return a ttransport, close directly");
        transport.close();
    }
}

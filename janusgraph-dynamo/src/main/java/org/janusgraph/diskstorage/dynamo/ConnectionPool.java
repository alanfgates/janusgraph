// Copyright 2019 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.janusgraph.diskstorage.dynamo;

import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;


class ConnectionPool {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPool.class);

    private final Deque<DynamoConnection> readyToUse;
    private final Set<DynamoConnection> inUse;
    private final Configuration conf;
    private final int maxConnections;
    private int connectionsCreated;
    private boolean closed;

    /**
     * This should only be called by DynamoStoreManager.
     * @param conf configuration object
     */
    ConnectionPool(Configuration conf) {
        this.conf = conf;
        maxConnections = conf.get(ConfigConstants.DYNAMO_MAX_CONNECTIONS);
        readyToUse = new ArrayDeque<>(maxConnections);
        inUse = new HashSet<>(maxConnections);
        connectionsCreated = 0;
        closed = false;
    }

    synchronized DynamoConnection getConnection() throws TemporaryBackendException {
        if (closed) throw new IllegalStateException("Attempt to get connection from closed pool");
        do {
            if (readyToUse.size() > 0) {
                DynamoConnection conn = readyToUse.pop();
                inUse.add(conn);
                return conn;
            }
            if (connectionsCreated < maxConnections) {
                connectionsCreated++;
                DynamoConnection conn = new DynamoConnection(this, conf);
                inUse.add(conn);
                return conn;
            }
            try {
                LOG.info("Out of connections, waiting for one to free up");
                wait();
            } catch (InterruptedException e) {
                throw new TemporaryBackendException("Interrupted waiting for connection", e);
            }
        } while (true);
    }

    synchronized void putBack(DynamoConnection conn) {
        if (closed) return; // just ignore, as some may be putting back their connections during shutdown
        boolean wasOut = inUse.remove(conn);
        assert wasOut;
        readyToUse.push(conn);
        notify();
    }

    synchronized void closeAll() {
        for (DynamoConnection conn : readyToUse) conn.destroy();
        for (DynamoConnection conn : inUse) conn.destroy();
        closed = true;
    }


}

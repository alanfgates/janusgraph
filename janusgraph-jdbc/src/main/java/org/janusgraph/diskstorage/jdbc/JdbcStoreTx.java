// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage.jdbc;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentMap;

public class JdbcStoreTx extends AbstractStoreTransaction {

    private final Connection jdbcConn;
    // Don't mess with this, other than add ourselves on creation and remove ourselves on commit
    // or rollback.
    private final ConcurrentMap<Integer, JdbcStoreTx> txs;
    private final int txId;

    public JdbcStoreTx(BaseTransactionConfig config, Connection jdbcConn,
                       ConcurrentMap<Integer, JdbcStoreTx> txs, int txId) {
        super(config);
        this.jdbcConn = jdbcConn;
        this.txs = txs;
        this.txId = txId;
        txs.put(txId, this);
    }

    @Override
    public void commit() throws BackendException {
        try {
            jdbcConn.commit();
            jdbcConn.close();
        } catch (SQLException e) {
            throw new PermanentBackendException("Commit failed", e);
        } finally {
            txs.remove(txId);
        }
    }

    @Override
    public void rollback() throws BackendException {
        try {
            jdbcConn.rollback();
            jdbcConn.close();
        } catch (SQLException e) {
            throw new PermanentBackendException("Rollback failed", e);
        } finally {
            txs.remove(txId);
        }
    }

    Connection getJdbcConn() {
        return jdbcConn;
    }
}

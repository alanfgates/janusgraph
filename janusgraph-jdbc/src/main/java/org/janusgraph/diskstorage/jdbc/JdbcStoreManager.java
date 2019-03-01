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
package org.janusgraph.diskstorage.jdbc;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

abstract class JdbcStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(JdbcStoreManager.class);

    static final String STORES_TABLE = "jg_stores";

    static final String STORES_NAME = "st_name";
    private static final String STORES_SCHEMA = "(" + STORES_NAME + " varchar(128) primary key)";

    private final DataSource connPool;
    // List of our transactions, so we know what to shutdown at close.  Kept in a
    // ConcurrentHashMap only for the synchronization.
    private final ConcurrentMap<Integer, JdbcStoreTx> txs;
    private final AtomicInteger nextTxId;
    private boolean closed = false;

    private final StoreFeatures features;

    final ConcurrentMap<String, JdbcStore> stores;

    protected abstract JdbcStore buildStore(String storeName);
    protected abstract String getIndexDrop(String storeName);

    // This is for testing so that the tests can pass in an embedded Postgres source.
    @VisibleForTesting
    protected JdbcStoreManager(Configuration conf, DataSource dataSource) throws BackendException {
        super(conf);

        if (dataSource == null) {
            // If the caller has not passed in a dataSource, then create a connection pool.
            Preconditions.checkArgument(conf.has(ConfigConstants.JDBC_URL) && conf.has(ConfigConstants.JDBC_USER) &&
                    conf.has(ConfigConstants.JDBC_PASSWORD),
                "Please supply configuration parameters " + ConfigConstants.JDBC_URL + ", " + ConfigConstants.JDBC_USER
                    + ", " + ConfigConstants.JDBC_PASSWORD);

            String driverUrl = conf.get(ConfigConstants.JDBC_URL);
            String user = conf.get(ConfigConstants.JDBC_USER);
            String passwd = conf.get(ConfigConstants.JDBC_PASSWORD);
            int maxPoolSize = conf.get(ConfigConstants.JDBC_POOL_SIZE);

            Duration connectionTimeout = conf.get(ConfigConstants.JDBC_TIMEOUT);
            HikariConfig config;
            try {
                config = new HikariConfig();
            } catch (Exception e) {
                throw new PermanentBackendException("Cannot create HikariCP configuration: ", e);
            }
            config.setMaximumPoolSize(maxPoolSize);
            config.setJdbcUrl(driverUrl);
            config.setUsername(user);
            config.setPassword(passwd);
            config.setConnectionTimeout(connectionTimeout.toMillis());
            log.info("Setting up connection pool with JDBC address " + driverUrl + " and user " +
                    user);
            connPool = new HikariDataSource(config);
        } else {
            log.debug("DataSource passed in, skipping setup of connection pool.");
            connPool = dataSource;
        }

        stores = new ConcurrentHashMap<>();
        txs = new ConcurrentHashMap<>();
        nextTxId = new AtomicInteger(0);
        try (Connection conn = getJdbcConn()) {
            createSysTableIfNotExist(conn);
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to close JDBC connection", e);
        }

        // Not 100% sure these are complete
        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .unorderedScan(true)
            .multiQuery(true)
            .transactional(true)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .keyOrdered(true)
            .batchMutation(true)
            .build();

    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        Preconditions.checkState(!closed);
        try {
            return new JdbcStoreTx(config, getJdbcConn(), txs, nextTxId.incrementAndGet());
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to connect to database", e);
        }
    }

    @Override
    public void close() throws BackendException {
        if (!closed) {
            for (JdbcStoreTx tx : txs.values()) tx.rollback();
            for (JdbcStore store : stores.values()) store.close();
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        Preconditions.checkState(!closed);
        // I'm not sure this is right
        if (!stores.isEmpty()) {
            throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet().toString());
        }

        Set<String> allStores = findAllStoresInDatabase();
        try (Connection conn = getJdbcConn()) {
            for (String store : allStores) {
                try (Statement stmt = conn.createStatement()) {
                    String sql = "drop table " + store;
                    log.debug("Going to execute " + sql);
                    stmt.execute(sql);
                    sql = "delete from " + STORES_TABLE + " where " + STORES_NAME + " = '" + store + "'";
                    log.debug("Going to execute " + sql);
                    stmt.execute(sql);
                    // Commit after each drop to avoid overloading the WAL
                    conn.commit();

                    // Drop the associated index on JCOL_COLUMN
                    sql = getIndexDrop(store);
                    log.debug("Going to execute " + sql);
                    stmt.execute(sql);
                    conn.commit();
                }
            }
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to connect to database or drop tables", e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        Preconditions.checkState(!closed);
        return !findAllStoresInDatabase().isEmpty();
    }

    @Override
    public StoreFeatures getFeatures() {
        Preconditions.checkState(!closed);
        return features;
    }

    @Override
    public String getName() {
        Preconditions.checkState(!closed);
        return getClass().getSimpleName();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) {
        return getStore(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            JdbcStore store = getStore(entry.getKey());
            store.deleteMultipleJKeys(entry.getValue(), txh);
            store.insertMultipleJKeys(entry.getValue(), txh);
        }
    }

    static String storeNameToColIndexName(String storeName) {
        return storeName + "_colindex";
    }

    /**
     * Get a JDBC connection.  Package permissions because it's used by JdbcStore.
     * @return a JDBC connection
     * @throws SQLException if a connection could not be made.
     */
    Connection getJdbcConn() throws SQLException {
        Connection conn = connPool.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    private JdbcStore getStore(String name) {
        Preconditions.checkState(!closed);
        Preconditions.checkNotNull(name);
        return stores.computeIfAbsent(name.toLowerCase(), this::buildStore);
    }

    /**
     * Get all of the existing store names.  This is not store objects we have instantiated but
     * all stores in the database.
     * @return all existing stores or an empty set if there are no existing stores
     * @throws BackendException if the attempt to communicate with the database fails.
     */
    private Set<String> findAllStoresInDatabase() throws BackendException {
        Set<String> allStores = new HashSet<>();

        try (Connection conn = getJdbcConn()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "select " + STORES_NAME + " from " + STORES_TABLE;
                log.debug("Going to run query " + sql);
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    allStores.add(rs.getString(STORES_NAME));
                }
            }

        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to connect to database or query system tables.");
        }

        return allStores;
    }

    private void createSysTableIfNotExist(Connection conn) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        ResultSet rs = md.getTables(null, null, STORES_TABLE, null);
        if (!rs.next()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "create table " + STORES_TABLE + " " + STORES_SCHEMA;
                log.debug("Going to execute " + sql);
                stmt.execute(sql);
                log.debug("Committing");
                conn.commit();
            }
        }
        rs.close();
    }
}

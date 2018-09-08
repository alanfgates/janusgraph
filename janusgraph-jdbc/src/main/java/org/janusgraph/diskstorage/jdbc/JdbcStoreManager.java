// Copyright 2018 JanusGraph Authors
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


import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

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

public class JdbcStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager {

    public static final ConfigNamespace JDBC_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "jdbc", "SQL JDBC backend " +
            "storage configuration options");

    public static final ConfigOption<String> JDBC_URL = new ConfigOption<>(JDBC_NS,
        "jdbc-url", "URL for JDBC connection for backends that use JDBC",
        ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<String> JDBC_USER = new ConfigOption<>(JDBC_NS,
        "jdbc-user", "User for JDBC connection for backends that use JDBC",
        ConfigOption.Type.LOCAL, String.class);

    // TODO - this should not be stored in the open, is there some way to encrypt this or fetch
    // it securely?
    public static final ConfigOption<String> JDBC_PASSWORD = new ConfigOption<>(JDBC_NS,
        "jdbc-password", "Password for JDBC connection for backends that use JDBC",
        ConfigOption.Type.LOCAL, String.class);

    public static final ConfigOption<Integer> JDBC_POOL_SIZE = new ConfigOption<>(JDBC_NS,
        "jdbc-pool-size", "JDBC connection pool size for backends that use JDBC",
        ConfigOption.Type.MASKABLE, 10, ConfigOption.positiveInt());

    public static final ConfigOption<Duration> JDBC_TIMEOUT = new ConfigOption<>(JDBC_NS,
        "jdbc-timeout", "JDBC connection timeout in seconds for backends that use JDBC",
        ConfigOption.Type.MASKABLE, Duration.ofSeconds(30L));

    static final String STORES_TABLE = "jg_stores";

    static final String STORES_NAME = "st_name";
    static final String STORES_SCHEMA =
        "(" + STORES_NAME + " varchar(128) primary key)";

    private final DataSource connPool;
    private final ConcurrentMap<String, JdbcKeyValueStore> stores;
    // List of our transactions, so we know what to shutdown at close.  Kept in a
    // ConcurrentHashMap only for hte synchronization.
    private final ConcurrentMap<Integer, JdbcStoreTx> txs;
    private final AtomicInteger nextTxId;
    private boolean closed = false;

    private final StoreFeatures features;

    public JdbcStoreManager(Configuration conf) throws BackendException {
        super(conf);

        Preconditions.checkArgument(conf.has(JDBC_URL) && conf.has(JDBC_USER) &&
                conf.has(JDBC_PASSWORD),
            "Please supply configuration parameters " + JDBC_URL + ", " + JDBC_USER
            + ", " + JDBC_PASSWORD);

        String driverUrl = conf.get(JDBC_URL);
        String user = conf.get(JDBC_USER);
        String passwd = conf.get(JDBC_PASSWORD);
        int maxPoolSize = conf.get(JDBC_POOL_SIZE);

        // Not sure if I need something like this.
        //Properties properties = replacePrefix(
            //DataSourceProvider.getPrefixedProperties(hdpConfig, HIKARI));
        Duration connectionTimeout = conf.get(JDBC_TIMEOUT);
        HikariConfig config = null;
        try {
            config = new HikariConfig();
        } catch (Exception e) {
            throw new PermanentBackendException("Cannot create HikariCP configuration: ", e);
        }
        config.setMaximumPoolSize(maxPoolSize);
        config.setJdbcUrl(driverUrl);
        config.setUsername(user);
        config.setPassword(passwd);
        //https://github.com/brettwooldridge/HikariCP
        config.setConnectionTimeout(connectionTimeout.toMillis());
        connPool = new HikariDataSource(config);

        stores = new ConcurrentHashMap<>();
        txs = new ConcurrentHashMap<>();
        nextTxId = new AtomicInteger(0);
        try (Connection conn = getJdbcConn()) {
            createSysTableIfNotExit(conn);
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to close JDBC connection", e);
        }

        // TODO - Not sure all these are right, just copied from BerkeleyDb
        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .transactional(transactional)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .locking(false)
            .keyOrdered(true)
            .supportsInterruption(false)
            .optimisticLocking(false)
            .build();

    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        Preconditions.checkState(!closed);
        try {
            JdbcStoreTx tx =
                new JdbcStoreTx(config, getJdbcConn(), txs, nextTxId.incrementAndGet());
            return tx;
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to connect to database", e);
        }
    }

    @Override
    public void close() throws BackendException {
        if (!closed) {
            for (JdbcStoreTx tx : txs.values()) tx.rollback();
            for (JdbcKeyValueStore store : stores.values()) store.close();
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        Preconditions.checkState(!closed);
        if (!stores.isEmpty()) {
            throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet().toString());
        }

        Set<String> allStores = findAllStoresInDatabase();
        for (String store : allStores) {
            try (Connection conn = getJdbcConn()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("delete from " + STORES_TABLE);
                }
            } catch (SQLException e) {
                throw new TemporaryBackendException("Unable to connect to database or drop tables", e);
            }
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
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        Preconditions.checkState(!closed);
        throw new UnsupportedOperationException();
    }

    @Override
    public OrderedKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkState(!closed);
        Preconditions.checkNotNull(name);
        return stores.computeIfAbsent(name, s -> new MysqlKeyValueStore(s, JdbcStoreManager.this));
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        for (Map.Entry<String,KVMutation> mutation : mutations.entrySet()) {
            OrderedKeyValueStore store = openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();

            /*
            if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }
            */

            if (mutationValue.hasAdditions()) {
                for (KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    //log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }
            if (mutationValue.hasDeletions()) {
                for (StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del,txh);
                    //log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }

    /**
     * Get a JDBC connection.  Package permissions because it's used by JdbcKeyValueStore.
     * @return a JDBC connection
     * @throws BackendException if a connection could not be made.
     */
    Connection getJdbcConn() throws SQLException {
        Connection conn = connPool.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * Get all of the existing store names.  This is not store objects we have instantiated but
     * all stores in the database.
     * @return all existing stores or an empty set if there are no existing stores
     * @throws BackendException if the attempt to communicate with the database fails.
     */
    Set<String> findAllStoresInDatabase() throws BackendException {
        Set<String> allStores = new HashSet<>();

        try (Connection conn = getJdbcConn()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("select " + STORES_NAME + " from " + STORES_TABLE);
                while (rs.next()) {
                    allStores.add(rs.getString(STORES_NAME));
                }
            }

        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to connect to database or query system " +
                "tables.");
        }

        return allStores;
    }

    private void createSysTableIfNotExit(Connection conn) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        ResultSet rs = md.getTables(null, null, STORES_TABLE, null);
        if (!rs.next()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("create table " + STORES_TABLE + STORES_SCHEMA);
                conn.commit();
            }
        }
        rs.close();
    }
}

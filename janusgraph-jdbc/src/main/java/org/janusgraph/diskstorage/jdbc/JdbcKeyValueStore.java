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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.janusgraph.diskstorage.jdbc.JdbcStoreManager.STORES_NAME;
import static org.janusgraph.diskstorage.jdbc.JdbcStoreManager.STORES_TABLE;

abstract class JdbcKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcKeyValueStore.class);
    private static final Pattern badStoreNameChars = Pattern.compile(".*[\\s/\\-'\"].*");

    private static final String KEY_VALUE_TABLE_KEY = "kv_key";
    private static final String KEY_VALUE_TABLE_VALUE = "kv_value";

    private final String storeName;
    private final JdbcStoreManager mgr;
    private final String keyValueStoreSchema;
    private boolean closed;

    abstract protected String getKeyType();
    abstract protected String getValueType();

    JdbcKeyValueStore(String name, JdbcStoreManager mgr) {
        Preconditions.checkArgument(storeNameIsOk(name),
            "Store name is not valid, no spaces, dashes, or slashes are allowed");
        this.storeName = name;
        this.mgr = mgr;
        keyValueStoreSchema = "(" + KEY_VALUE_TABLE_KEY + " " + getKeyType() +" primary key, " +
            KEY_VALUE_TABLE_VALUE + " " + getValueType() + ")";
        closed = false;
        createStoreInfoIfNotExists();

    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        String sql = "delete from " + storeName + " where " + KEY_VALUE_TABLE_KEY + " = ?";
        log.debug("Going to execute " + sql + " with ? set to " + key.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql)) {
            stmt.setBytes(1, key.getBytes(0, key.length()));
            stmt.execute();
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to delete record", e);
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        String sql = "select " + KEY_VALUE_TABLE_VALUE + " from " + storeName +
                " where " + KEY_VALUE_TABLE_KEY + " = ?";
        log.debug("Going to execute " + sql + " with ? set to " + key.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql)) {
            stmt.setBytes(1, key.getBytes(0, key.length()));
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? new StaticArrayBuffer(rs.getBytes(KEY_VALUE_TABLE_VALUE)) : null;
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to fetch record", e);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        String sql = "select 1 from " + storeName + " where " + KEY_VALUE_TABLE_KEY + " = ?";
        log.debug("Going to execute " + sql + " with ? set to " + key.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql)) {
            stmt.setBytes(1, key.getBytes(0, key.length()));
            ResultSet rs = stmt.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to fetch key", e);
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue,
                            StoreTransaction txh) throws BackendException {
        // We don't lock
    }

    @Override
    public String getName() {
        Preconditions.checkState(!closed);
        return storeName;
    }

    @Override
    public void close() throws BackendException {
        if (!closed) {
            closed = true;
            // Remove it from the list of valid stores.
            mgr.stores.remove(storeName);
        }
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        String sql = "insert into " + storeName +
                " (" + KEY_VALUE_TABLE_KEY + ", " + KEY_VALUE_TABLE_VALUE + ") " +
                " values (?, ?)";
        log.debug("Going to execute " + sql + " with ?, ? set to <" + key.toString() + ">, <" +
            value.toString() + ">");
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql)) {
            stmt.setBytes(1, key.getBytes(0, key.length()));
            stmt.setBytes(2, value.getBytes(0, value.length()));
            stmt.execute();
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to insert record", e);
        }
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        List<KeyValueEntry> result = new ArrayList<>();
        String sql = "select " + KEY_VALUE_TABLE_KEY + ", " + KEY_VALUE_TABLE_VALUE +
                " from " + storeName +
                // TODO don't know if this is inclusive or exclusive on begin and end
                " where " + KEY_VALUE_TABLE_KEY + " >= ? and " + KEY_VALUE_TABLE_KEY + " < ?";
        log.debug("Going to execute " + sql + " with ?, ? set to <" + query.getStart().toString()
              + ">, <" + query.getEnd().toString() + ">");
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql)) {
            stmt.setBytes(1, query.getStart().getBytes(0, query.getStart().length()));
            stmt.setBytes(2, query.getEnd().getBytes(0, query.getEnd().length()));
            ResultSet rs = stmt.executeQuery();
            // Don't call this again, as getKeySelector creates a new KeySelector every time
            // (brilliant, eh?)
            KeySelector selector = query.getKeySelector();
            while (rs.next()) {
                StaticBuffer key = new StaticArrayBuffer(rs.getBytes(KEY_VALUE_TABLE_KEY));
                if (selector.include(key)) {
                    result.add(new KeyValueEntry(key,
                        new StaticArrayBuffer(rs.getBytes(KEY_VALUE_TABLE_VALUE))));
                }
                if (selector.reachedLimit()) break;
            }
        } catch (SQLException e) {
            throw new TemporaryBackendException("Unable to select", e);
        }

        return new RecordIterator<KeyValueEntry>() {
            private final Iterator<KeyValueEntry> entries = result.iterator();
            @Override
            public void close() throws IOException {
            }

            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public KeyValueEntry next() {
                return entries.next();
            }
        };
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries,
                                                                 StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        Map<KVQuery, RecordIterator<KeyValueEntry>> result = new HashMap<>(queries.size());
        for (KVQuery query : queries) {
            result.put(query, getSlice(query, txh));
        }
        return result;
    }

    private boolean storeNameIsOk(String name) {
        Matcher m = badStoreNameChars.matcher(name);
        return !m.matches();

    }

    private void createStoreInfoIfNotExists() {
        try (Connection conn = mgr.getJdbcConn()) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "select 1 " +
                    " from " + STORES_TABLE +
                    " where " + STORES_NAME + " = '" + storeName + "'";
                log.debug("Going to execute " + sql);
                ResultSet rs = stmt.executeQuery(sql);
                if (!rs.next()) {
                    // storename is checked at object creation time to make sure it doesn't have
                    // any spaces, dashes, or slashes so it should be safe to operate on it
                    // directly rather than using a prepared statement.
                    sql = "insert into " + STORES_TABLE +
                        " (" + STORES_NAME + ") " +
                        " values ('" + storeName + "')";
                    log.debug("Going to execute " + sql);
                    stmt.execute(sql);
                    sql = "create table " + storeName + " " + keyValueStoreSchema;
                    log.debug("Going to execute " + sql);
                    stmt.execute(sql);
                }
            }
            log.debug("Committing");
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to connect to database or create tables", e);
        }
    }
}

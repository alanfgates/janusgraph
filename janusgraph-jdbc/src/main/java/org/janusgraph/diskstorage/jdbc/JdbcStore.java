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
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.janusgraph.diskstorage.jdbc.JdbcStoreManager.STORES_NAME;
import static org.janusgraph.diskstorage.jdbc.JdbcStoreManager.STORES_TABLE;

abstract class JdbcStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcStore.class);
    private static final Pattern badStoreNameChars = Pattern.compile(".*[\\s/\\-'\"].*");

    private static final String JKEY_COLUMN = "janus_key";
    private static final String JCOL_COLUMN = "janus_column";
    private static final String JVAL_COLUMN = "janus_value";

    private final String storeName;
    private final JdbcStoreManager mgr;
    private boolean closed;

    abstract protected String getBinaryType();
    abstract protected String getIndexDefinition(String storeName, String colName);

    JdbcStore(String name, JdbcStoreManager mgr) {
        Preconditions.checkArgument(storeNameIsOk(name),
            "Store name is not valid, no spaces, dashes, or slashes are allowed");
        this.storeName = name;
        this.mgr = mgr;
        closed = false;
        createStoreInfoIfNotExists();

    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        StringBuilder sql = new StringBuilder("select ").append(JCOL_COLUMN).append(", ").append(JVAL_COLUMN)
            .append(" from ").append(storeName)
            .append(" where ").append(JKEY_COLUMN).append(" = ? and ").append(JCOL_COLUMN).append(" >= ? and ").append(JCOL_COLUMN).append(" < ? ")
            .append("order by ").append(JCOL_COLUMN);
        if (query.hasLimit()) {
            sql.append(" limit ").append(query.getLimit());
        }
        log.debug("Going to execute " + sql.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql.toString())) {
            int psval = 1;
            stmt.setBytes(psval++, query.getKey().as(StaticBuffer.ARRAY_FACTORY));
            stmt.setBytes(psval++, query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY));
            stmt.setBytes(psval++, query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY));
            ResultSet rs = stmt.executeQuery();
            SortedMap<StaticBuffer, StaticBuffer> vals = new TreeMap<>();
            while (rs.next()) {
                vals.put(StaticArrayBuffer.of(rs.getBytes(JCOL_COLUMN)), StaticArrayBuffer.of(rs.getBytes(JVAL_COLUMN)));
            }
            return StaticArrayEntryList.ofStaticBuffer(vals.entrySet(), rsGetter);
        } catch (SQLException e) {
            log.error("Unable to select records", e);
            throw new TemporaryBackendException("Unable to select records", e);
        }
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        StringBuilder sql = new StringBuilder("select ").append(JKEY_COLUMN).append(", ").append(JCOL_COLUMN).append(", ").append(JVAL_COLUMN)
            .append(" from ").append(storeName)
            .append(" where ").append(JKEY_COLUMN).append(" in (");
        boolean first = true;
        for (int i = 0; i < keys.size(); i++) {
            if (first) first = false;
            else sql.append(", ");
            sql.append("?");
        }
        sql.append(") and ").append(JCOL_COLUMN).append(" >= ? and ").append(JCOL_COLUMN).append(" < ? ")
            .append("order by ").append(JKEY_COLUMN).append(", ").append(JCOL_COLUMN);
        log.debug("Going to execute " + sql.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql.toString())) {
            int psval = 1;
            for (StaticBuffer key : keys) {
                stmt.setBytes(psval++, key.as(StaticBuffer.ARRAY_FACTORY));
            }
            stmt.setBytes(psval++, query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY));
            stmt.setBytes(psval++, query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY));
            ResultSet rs = stmt.executeQuery();
            Map<StaticBuffer, EntryList> results = new HashMap<>();
            StaticBuffer currKey = null;
            Map<StaticBuffer, StaticBuffer> currCols = null;
            while (rs.next()) {
                StaticBuffer thisKey = StaticArrayBuffer.of(rs.getBytes(JKEY_COLUMN));
                if (!thisKey.equals(currKey)) {
                    if (currKey != null) {
                        results.put(currKey, StaticArrayEntryList.ofStaticBuffer(currCols.entrySet(), rsGetter));
                    }
                    currKey = thisKey;
                    currCols = new HashMap<>();
                }
                if (query.hasLimit() && currCols.size() >= query.getLimit()) continue;
                currCols.put(StaticArrayBuffer.of(rs.getBytes(JCOL_COLUMN)), StaticArrayBuffer.of(rs.getBytes(JVAL_COLUMN)));
            }
            return results;
        } catch (SQLException e) {
            log.error("Unable to select records", e);
            throw new TemporaryBackendException("Unable to select records", e);
        }
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        if (deletions != null && !deletions.isEmpty()) deleteMultipleJKeys(Collections.singletonMap(key, new KCVMutation(Collections.emptyList(), deletions)), txh);
        if (additions != null && !additions.isEmpty()) insertMultipleJKeys(Collections.singletonMap(key, new KCVMutation(additions, Collections.emptyList())), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return getKeys(query, query.getKeyStart(), query.getKeyEnd(), txh);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return getKeys(query, null, null, txh);
    }

    @Override
    public String getName() {
        Preconditions.checkState(!closed);
        return storeName;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            // Remove it from the list of valid stores.
            mgr.stores.remove(storeName);
        }
    }

    void deleteMultipleJKeys(Map<StaticBuffer, KCVMutation> deletes, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);
        // There may not be any deletes in what we've been passed.  If not, bail here.
        int numDeletes = totalSize(deletes, false);
        if (numDeletes == 0) return;

        StringBuilder sql = new StringBuilder("delete from ").append(storeName)
            .append(" where ");
        boolean first = true;
        for (int i = 0; i < numDeletes; i++) {
            if (first) first = false;
            else sql.append(" or ");
            sql.append("(").append(JKEY_COLUMN).append(" = ? and ").append(JCOL_COLUMN).append(" = ?)");
        }
        log.debug("Going to execute " + sql.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql.toString())) {
            int psval = 1;
            for (Map.Entry<StaticBuffer, KCVMutation> keyEntry : deletes.entrySet()) {
                byte[] jkey = keyEntry.getKey().as(StaticBuffer.ARRAY_FACTORY);
                for (StaticBuffer col : keyEntry.getValue().getDeletions()) {
                    stmt.setBytes(psval++, jkey);
                    stmt.setBytes(psval++, col.as(StaticBuffer.ARRAY_FACTORY));
                }
            }
            stmt.execute();
        } catch (SQLException e) {
            log.error("Unable to delete records", e);
            throw new TemporaryBackendException("Unable to delete records", e);
        }
    }

    void insertMultipleJKeys(Map<StaticBuffer, KCVMutation> inserts, StoreTransaction txh) throws BackendException {
        Preconditions.checkState(!closed);

        // If there are no inserts, bail
        int numInserts = totalSize(inserts, true);
        if (numInserts == 0) return;

        // Janus does inserts with the assumption that they are upserts.  So blindly delete in front of
        // inserts as this isn't an error.
        Map<StaticBuffer, KCVMutation> preDeletes = new HashMap<>(inserts.size());
        for (Map.Entry<StaticBuffer, KCVMutation> keyEntry : inserts.entrySet()) {
            List<StaticBuffer> deletes = new ArrayList<>(keyEntry.getValue().getAdditions().size());
            for (Entry entry : keyEntry.getValue().getAdditions()) {
                deletes.add(entry.getColumn());
            }
            preDeletes.put(keyEntry.getKey(), new KCVMutation(Collections.emptyList(), deletes));
        }
        deleteMultipleJKeys(preDeletes, txh);

        StringBuilder sql = new StringBuilder("insert into ").append(storeName)
            .append(" (").append(JKEY_COLUMN).append(", ").append(JCOL_COLUMN).append(", ").append(JVAL_COLUMN).append(") ")
            .append(" values ");
        boolean first = true;
        for (int i = 0; i < numInserts; i++) {
            if (first) first = false;
            else sql.append(", ");
            sql.append("(?, ?, ?)");
        }
        log.debug("Going to execute " + sql.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql.toString())) {
            int psval = 1;
            for (Map.Entry<StaticBuffer, KCVMutation> keyEntry : inserts.entrySet()) {
                byte[] jkey = keyEntry.getKey().as(StaticBuffer.ARRAY_FACTORY);
                for (Entry colEntry : keyEntry.getValue().getAdditions()) {
                    stmt.setBytes(psval++, jkey);
                    stmt.setBytes(psval++, colEntry.getColumn().as(StaticBuffer.ARRAY_FACTORY));
                    stmt.setBytes(psval++, colEntry.getValue().as(StaticBuffer.ARRAY_FACTORY));
                }
            }
            stmt.execute();
        } catch (SQLException e) {
            log.error("Unable to insert records", e);
            throw new TemporaryBackendException("Unable to insert records", e);
        }
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
                    sql = "create table " + storeName + " ( " +
                        JKEY_COLUMN + " " + getBinaryType() + " not null, " +
                        JCOL_COLUMN + " " + getBinaryType() + " not null, " +
                        JVAL_COLUMN + " " + getBinaryType() + ", " +
                        "primary key (" + JKEY_COLUMN + ", " + JCOL_COLUMN + "))";
                    log.debug("Going to execute " + sql);
                    stmt.execute(sql);

                    // Build a second index on JCOL_COLUMN for getKeys calls
                    sql = getIndexDefinition(storeName, JCOL_COLUMN);
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

    private int totalSize(Map<StaticBuffer, KCVMutation> mutations, boolean countInserts) {
        int cnt = 0;
        for (Map.Entry<StaticBuffer, KCVMutation> entry : mutations.entrySet()) {
            cnt += countInserts ? entry.getValue().getAdditions().size() : entry.getValue().getDeletions().size();
        }
        return cnt;
    }

    private KeyIterator getKeys(SliceQuery query, StaticBuffer startKey, StaticBuffer endKey, StoreTransaction txh) throws BackendException {
        StringBuilder sql = new StringBuilder("select ").append(JKEY_COLUMN).append(", ").append(JCOL_COLUMN).append(", ").append(JVAL_COLUMN)
            .append(" from ").append(storeName)
            .append(" where ");
        if (startKey != null) {
            sql.append(JKEY_COLUMN).append(" >= ? and ").append(JKEY_COLUMN).append(" < ? and ");
        }
        sql.append(JCOL_COLUMN).append(" >= ? and ").append(JCOL_COLUMN).append(" < ? ")
            .append("order by ").append(JKEY_COLUMN).append(", ").append(JCOL_COLUMN);
        // Can't use limit in the SQL query, because the limit applies to columns in each key, not to the query as a whole.
        log.debug("Going to execute " + sql.toString());
        try (PreparedStatement stmt = ((JdbcStoreTx)txh).getJdbcConn().prepareStatement(sql.toString())) {
            int psval = 1;
            if (startKey != null) {
                stmt.setBytes(psval++, startKey.as(StaticBuffer.ARRAY_FACTORY));
                stmt.setBytes(psval++, endKey.as(StaticBuffer.ARRAY_FACTORY));
            }
            stmt.setBytes(psval++, query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY));
            stmt.setBytes(psval++, query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY));
            ResultSet rs = stmt.executeQuery();
            // Not sure if I can return an iterator that doesn't immediately materialize the results or not.  Not sure
            // if JDBC will close the result set on me based on what the user does with the connection.
            final Map<StaticBuffer, Map<StaticBuffer, StaticBuffer>> materialized = new HashMap<>();
            while (rs.next()) {
                StaticBuffer key = StaticArrayBuffer.of(rs.getBytes(JKEY_COLUMN));
                Map<StaticBuffer, StaticBuffer> cols = materialized.computeIfAbsent(key, k -> new HashMap<>());
                if (query.hasLimit() && cols.size() >= query.getLimit()) continue;
                cols.put(StaticArrayBuffer.of(rs.getBytes(JCOL_COLUMN)), StaticArrayBuffer.of(rs.getBytes(JVAL_COLUMN)));
            }
            return new KeyIterator() {
                boolean outerClosed = false;
                Iterator<Map.Entry<StaticBuffer, Map<StaticBuffer, StaticBuffer>>> outerIter = materialized.entrySet().iterator();
                Map<StaticBuffer, StaticBuffer> currCols;

                @Override
                public RecordIterator<Entry> getEntries() {
                    return new RecordIterator<Entry>() {
                        boolean innerClosed = false;
                        Iterator<Map.Entry<StaticBuffer, StaticBuffer>> innerIter = currCols.entrySet().iterator();
                        @Override
                        public void close() {
                            innerClosed = true;
                        }

                        @Override
                        public boolean hasNext() {
                            if (innerClosed) throw new IllegalStateException("Attempt to read closed iterator");
                            return innerIter.hasNext();
                        }

                        @Override
                        public Entry next() {
                            if (innerClosed) throw new IllegalStateException("Attempt to read closed iterator");
                            return StaticArrayEntry.ofStaticBuffer(innerIter.next(), rsGetter);
                        }
                    };
                }

                @Override
                public void close() {
                    outerClosed = true;
                }

                @Override
                public boolean hasNext() {
                    if (outerClosed) throw new IllegalStateException("Attempt to read closed iterator");
                    return outerIter.hasNext();
                }

                @Override
                public StaticBuffer next() {
                    if (outerClosed) throw new IllegalStateException("Attempt to read closed iterator");
                    Map.Entry<StaticBuffer, Map<StaticBuffer, StaticBuffer>> entry = outerIter.next();
                    currCols = entry.getValue();
                    return entry.getKey();
                }
            };
        } catch (SQLException e) {
            log.error("Unable to select records", e);
            throw new TemporaryBackendException("Unable to select records", e);
        }
    }

    private final StaticArrayEntry.GetColVal<Map.Entry<StaticBuffer, StaticBuffer>, StaticBuffer> rsGetter =
        new StaticArrayEntry.GetColVal<Map.Entry<StaticBuffer, StaticBuffer>, StaticBuffer>() {
            @Override
            public StaticBuffer getColumn(Map.Entry<StaticBuffer, StaticBuffer> element) {
                return element.getKey();
            }

            @Override
            public StaticBuffer getValue(Map.Entry<StaticBuffer, StaticBuffer> element) {
                return element.getValue();
            }

            @Override
            public EntryMetaData[] getMetaSchema(Map.Entry<StaticBuffer, StaticBuffer> element) {
                return mgr.getMetaDataSchema(storeName);
            }

            @Override
            public Object getMetaData(Map.Entry<StaticBuffer, StaticBuffer> element, EntryMetaData meta) {
                throw new UnsupportedOperationException();
            }
    };

}

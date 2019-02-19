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

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryFilter;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DynamoStore implements KeyColumnValueStore {

    private static final String DEFAULT_DYNAMO_TABLE_BASE = DynamoStoreManager.NAME + "-";
    public static final ConfigOption<String> DYNAMO_TABLE_BASE = new ConfigOption<>(Utils.DYNAMO_NS,
        "dynamo-table-base", "Basename for tables to be created in dynamoDB", ConfigOption.Type.FIXED, // TODO not sure if this should be FIXED or LOCAL
        DEFAULT_DYNAMO_TABLE_BASE);
    // TODO - no idea if these are reasonable values or not
    private static final long DEFAULT_READ_THROUGHPUT = 10L;
    private static final long DEFAULT_WRITE_THROUGHPUT = 10L;

    public static final ConfigOption<Long> DYNAMO_READ_THROUGHPUT = new ConfigOption<>(Utils.DYNAMO_NS,
        "dynamo-read-throughput", "Dynamo read capacity throughput", ConfigOption.Type.LOCAL, DEFAULT_READ_THROUGHPUT);
    public static final ConfigOption<Long> DYNAMO_WRITE_THROUGHPUT = new ConfigOption<>(Utils.DYNAMO_NS,
        "dynamo-write-throughput", "Dynamo write capacity throughput", ConfigOption.Type.LOCAL, DEFAULT_WRITE_THROUGHPUT);

    static final String PARTITION_KEY = "januskey"; // Column name of partition key in Dynamo, uses the JG key
    static final String SORT_KEY = "januscol"; // Column name of sort key in Dynamo, uses the JG column name
    static final String COL_NAME = "c"; // Column name we use in Dynamo, not related to anything in JG

    private static final Logger LOG = LoggerFactory.getLogger(DynamoStore.class);

    private final DynamoStoreManager manager;
    private final String storeName;
    private final String tableName;
    private final Configuration conf;
    private Table table; // don't access directly, call getTable()

    public DynamoStore(DynamoStoreManager mgr, Configuration conf, String storeName) {
        manager = mgr;
        this.conf = conf;
        this.storeName = storeName;
        this.tableName = conf.get(DYNAMO_TABLE_BASE) + storeName;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        try {
            // The query spec doesn't allow more than one operation on a range key, thus I can't pass a filter like
            // range key >= begin and range key < end, which is what I need since JG queries are inclusive of the
            // slice start but exclusive of the slice end.  So, I use between to get inclusive on the sort key on
            // both start and end.  Dynamo also won't let you run a filter on a primary key, so you can't just
            // append a query filter to the query to get rid of any key values that are equal to the end of the slice
            // To get around these insane and irritating limitations I run the query as a between and then
            // do a quick != check on the sort key of the returned values.
            QuerySpec querySpec = new QuerySpec()
                .withHashKey(PARTITION_KEY, query.getKey().asByteBuffer())
                .withRangeKeyCondition(new RangeKeyCondition(SORT_KEY).between(query.getSliceStart().asByteBuffer(),
                    query.getSliceEnd().asByteBuffer()));

            if (LOG.isTraceEnabled()) {
                LOG.trace("Running getSlice query with key " + query.getKey().toString() +
                    " beginning slice at " + query.getSliceStart().toString() + " and ending slice at " +
                    query.getSliceEnd().toString());
            }
            ItemCollection<QueryOutcome> results = getTable().query(querySpec);
            return StaticArrayEntryList.ofByteBuffer(pruneOutEndKey(results, query.getSliceEnd().asByteBuffer()), dynamoEntryGetter);
        } catch (Exception e) {
            LOG.error("Failed to run query", e);
            throw new TemporaryBackendException("Failed to run query", e);
        }
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh)
        throws BackendException {
        manager.mutateMany(
            Collections.singletonMap(storeName,
                Collections.singletonMap(key, new KCVMutation(additions, deletions))),
            txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();

    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        // This method is painful because it does a full scan and has to fetch back all the results before
        // it can return the iterator to the caller.  If this is called frequently we might want to create
        // a secondary table that maps janus columns to janus keys so we can do this efficiently.
        try {
            Map<String, Object> valMap = new HashMap<>();
            valMap.put(":lowerbound", query.getSliceStart().asByteBuffer());
            valMap.put(":upperbound", query.getSliceEnd().asByteBuffer());
            ScanSpec scanSpec = new ScanSpec()
                .withFilterExpression("#sk >= :lowerbound and #sk < :upperbound")
                .withNameMap(Collections.singletonMap("#sk", SORT_KEY))
                .withValueMap(valMap);

            ItemCollection<ScanOutcome> results = getTable().scan(scanSpec);
            final Map<ByteBuffer, List<Item>> byKey = new HashMap<>();
            for (Item item : results) {
                ByteBuffer hashKey = item.getByteBuffer(PARTITION_KEY);
                List<Item> itemsForThisKey = byKey.computeIfAbsent(hashKey, byteBuffer -> new ArrayList<>());
                itemsForThisKey.add(item);
            }
            return new KeyIterator() {
                Iterator<Map.Entry<ByteBuffer, List<Item>>> iter = byKey.entrySet().iterator();
                Map.Entry<ByteBuffer, List<Item>> curVal;
                boolean closed = false;

                @Override
                public RecordIterator<Entry> getEntries() {
                    return new RecordIterator<Entry>() {
                        Iterator<Item> innerIter = curVal.getValue().iterator();
                        boolean innerClosed = false;

                        // The HBase implementation closes the outer iterator if the inner iterator is closed, but
                        // that doesn't seem like it can be right.
                        @Override
                        public void close() throws IOException {
                            innerClosed = true;
                        }

                        @Override
                        public boolean hasNext() {
                            if (innerClosed) throw new IllegalStateException("Attempt to access closed iterator");
                            return innerIter.hasNext();
                        }

                        @Override
                        public Entry next() {
                            if (innerClosed) throw new IllegalStateException("Attempt to access closed iterator");
                            return StaticArrayEntry.ofByteBuffer(innerIter.next(), dynamoEntryGetter);
                        }
                    };
                }

                @Override
                public void close() throws IOException {
                    closed = true;
                }

                @Override
                public boolean hasNext() {
                    if (closed) throw new IllegalStateException("Attempt to access closed iterator");
                    return iter.hasNext();
                }

                @Override
                public StaticBuffer next() {
                    if (closed) throw new IllegalStateException("Attempt to access closed iterator");
                    curVal = iter.next();
                    return StaticArrayBuffer.of(curVal.getKey());
                }
            };

        } catch (Exception e) {
            LOG.error("Failed to run scan", e);
            throw new TemporaryBackendException("Failed to run scan e");
        }
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() throws BackendException {
        manager.getDynamo().shutdown();
    }

    String getTableName() {
        return tableName;
    }

    /**
     * Drop the data in this store.
     */
    void drop() {
        try {
            getTable().delete();
        } catch (Exception e) {
            LOG.error("Failed to drop table: " + e.getMessage(), e);
        }
    }

    Table getTable() throws BackendException {
        if (table == null) {
            // So when you call DynamoDB.getTable() it doesn't actually get the table.  It just gives you a handle
            // based on the table name.  So you can't use it to tell whether the table exists.  Instead, you have
            // to get a listing of the existing tables and look for your table name.  Who writes APIs like this?
            TableCollection<ListTablesResult> existingTables =
                manager.getDynamo().listTables(conf.get(DYNAMO_TABLE_BASE));
            for (Table maybe : existingTables) {
                if (maybe.getTableName().equals(tableName)) {
                    table = maybe;
                    break;
                }
            }
            // We didn't find it in the list, so we need to create it.
            if (table == null) {
                LOG.info("Unable to find table " + tableName + ", will create it");
                table = manager.getDynamo().createTable(tableName,
                    Arrays.asList(
                        new KeySchemaElement(PARTITION_KEY, KeyType.HASH),
                        new KeySchemaElement(SORT_KEY, KeyType.RANGE)
                    ), Arrays.asList(
                        new AttributeDefinition(PARTITION_KEY, ScalarAttributeType.B),
                        new AttributeDefinition(SORT_KEY, ScalarAttributeType.B)
                    ), new ProvisionedThroughput(conf.get(DYNAMO_READ_THROUGHPUT), conf.get(DYNAMO_WRITE_THROUGHPUT)));
                try {
                    table.waitForActive();
                } catch (InterruptedException e) {
                    throw new TemporaryBackendException("Interupted waiting for table to be active", e);
                }
            }
        }
        return table;
    }

    private final StaticArrayEntry.GetColVal<Item, ByteBuffer> dynamoEntryGetter = new StaticArrayEntry.GetColVal<Item, ByteBuffer>() {
        @Override
        public ByteBuffer getColumn(Item element) {
            return element.getByteBuffer(SORT_KEY);
        }

        @Override
        public ByteBuffer getValue(Item element) {
            return element.getByteBuffer(COL_NAME);
        }

        @Override
        public EntryMetaData[] getMetaSchema(Item element) {
            return manager.getMetaDataSchema(storeName);
        }

        @Override
        public Object getMetaData(Item element, EntryMetaData meta) {
            throw new UnsupportedOperationException();
        }
    };

    private Collection<Item> pruneOutEndKey(ItemCollection<QueryOutcome> results, ByteBuffer end) {
        List<Item> pruned = new ArrayList<>();
        for (Item item : results) {
            if (!item.getByteBuffer(SORT_KEY).equals(end)) pruned.add(item);
        }
        return pruned;
    }

}

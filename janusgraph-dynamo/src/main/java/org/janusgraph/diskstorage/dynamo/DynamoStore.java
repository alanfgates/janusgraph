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
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.ScanFilter;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.janusgraph.diskstorage.dynamo.Utils.unsignedBytesFactory;
import static org.janusgraph.diskstorage.dynamo.Utils.unsignedBytesToBytes;

public class DynamoStore implements KeyColumnValueStore {

    static final String PARTITION_KEY = "januskey"; // Column name of partition key in Dynamo, uses the JG key
    static final String SORT_KEY = "januscol"; // Column name of sort key in Dynamo, uses the JG column name
    static final String COL_NAME = "c"; // Column name we use in Dynamo, not related to anything in JG

    private static final Logger LOG = LoggerFactory.getLogger(DynamoStore.class);

    private final DynamoStoreManager manager;
    private final String storeName;
    private final String tableName;
    private final CreateTableRequest tableDef;
    //private Table table; // don't access directly, call getTable()

    public DynamoStore(DynamoStoreManager mgr, Configuration conf, String storeName) {
        manager = mgr;
        this.storeName = storeName;
        this.tableName = conf.get(Utils.DYNAMO_TABLE_BASE) + storeName;
        tableDef = new CreateTableRequest()
            .withTableName(tableName)
            .withKeySchema(
                new KeySchemaElement(PARTITION_KEY, KeyType.HASH),
                new KeySchemaElement(SORT_KEY, KeyType.RANGE))
            .withAttributeDefinitions(
                new AttributeDefinition(PARTITION_KEY, ScalarAttributeType.B),
                new AttributeDefinition(SORT_KEY, ScalarAttributeType.B))
            .withProvisionedThroughput(new ProvisionedThroughput(conf.get(Utils.DYNAMO_READ_THROUGHPUT), conf.get(Utils.DYNAMO_WRITE_THROUGHPUT)));
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        try (DynamoTable table = manager.tableMgr.getTable(tableName, tableDef)){
            // The query spec doesn't allow more than one operation on a range key, thus I can't pass a filter like
            // range key >= begin and range key < end, which is what I need since JG queries are inclusive of the
            // slice start but exclusive of the slice end.  So, I use between to get inclusive on the sort key on
            // both start and end.  Dynamo also won't let you run a filter on a primary key, so you can't just
            // append a query filter to the query to get rid of any key values that are equal to the end of the slice
            // To get around these insane and irritating limitations I run the query as a between and then
            // do a quick != check on the sort key of the returned values.
            byte[] sliceEnd = query.getSliceEnd().as(unsignedBytesFactory);
            QuerySpec querySpec = new QuerySpec()
                .withHashKey(PARTITION_KEY, query.getKey().as(StaticBuffer.ARRAY_FACTORY))
                .withRangeKeyCondition(new RangeKeyCondition(SORT_KEY).between(query.getSliceStart().as(unsignedBytesFactory),
                    sliceEnd));

            if (LOG.isTraceEnabled()) {
                LOG.trace("Running getSlice query with key " + query.getKey().toString() +
                    " beginning slice at " + query.getSliceStart().toString() + " and ending slice at " +
                    query.getSliceEnd().toString());
            }
            ItemCollection<QueryOutcome> results = table.get().query(querySpec);
            return StaticArrayEntryList.ofBytes(applyLimit(pruneOutEndKey(results, sliceEnd), query), dynamoEntryGetter);
        } catch (Exception e) {
            LOG.error("Failed to run query", e);
            throw new TemporaryBackendException("Failed to run query", e);
        }
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) {
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
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) {
        throw new UnsupportedOperationException();

    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        // This method is painful because it does a full scan and has to fetch back all the results before
        // it can return the iterator to the caller.  If this is called frequently we might want to create
        // a secondary table that maps janus columns to janus keys so we can do this efficiently.
        try (DynamoTable table = manager.tableMgr.getTable(tableName, tableDef)){
            byte[] sliceEnd = query.getSliceEnd().as(unsignedBytesFactory);
            ScanSpec scanSpec = new ScanSpec()
                .withScanFilters(new ScanFilter(SORT_KEY).between(query.getSliceStart().as(unsignedBytesFactory),
                    sliceEnd));

            ItemCollection<ScanOutcome> results = table.get().scan(scanSpec);
            // We wrap the partition key in a ByteBuffer before sticking it in the map because ByteBuffers return
            // reasonable equals() and hashCode() values for use in hashing.
            final Map<ByteBuffer, List<Item>> byKey = new HashMap<>();
            for (Item item : pruneOutEndKey(results, sliceEnd)) {
                byte[] hashKey = item.getBinary(PARTITION_KEY);
                List<Item> itemsForThisKey = byKey.computeIfAbsent(ByteBuffer.wrap(hashKey), byteBuffer -> new ArrayList<>());
                itemsForThisKey.add(item);
            }
            return new KeyIterator() {
                Iterator<Map.Entry<ByteBuffer, List<Item>>> iter = byKey.entrySet().iterator();
                Map.Entry<ByteBuffer, List<Item>> curVal;
                boolean closed = false;

                @Override
                public RecordIterator<Entry> getEntries() {
                    return new RecordIterator<Entry>() {
                        // AFAIK there's no guarantee that the items comes back from the Dynamo scan in any order
                        // I'm guessing JanusGraph expects them to come back in colname order.  So sort them here.
                        // And apply the limit if there is one.
                        Iterator<Item> innerIter = applyLimit(sortColNames(curVal.getValue()), query).iterator();
                        boolean innerClosed = false;

                        // The HBase implementation closes the outer iterator if the inner iterator is closed, but
                        // that doesn't seem like it can be right.
                        @Override
                        public void close() {
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
                            return StaticArrayEntry.ofBytes(innerIter.next(), dynamoEntryGetter);
                        }
                    };
                }

                @Override
                public void close() {
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
                    return StaticArrayBuffer.of(curVal.getKey().array());
                }
            };

        } catch (Exception e) {
            LOG.error("Failed to run scan, query start was " + query.getSliceStart().toString() +
                " and query end was " + query.getSliceEnd().toString(), e);
            throw new TemporaryBackendException("Failed to run scan e");
        }
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() {
    }

    String getTableName() {
        return tableName;
    }

    /**
     * Drop the data in this store.
     */
    void drop() {
        try (DynamoTable table = manager.tableMgr.getTable(tableName, tableDef)) {
            table.get().delete();
        } catch (Exception e) {
            LOG.error("Failed to drop table: " + e.getMessage(), e);
        }
    }

    /**
     * Make sure a table exists so that data can be inserted into it.
     * @throws BackendException if we fail to connect to DynamoDB or to create the table
     */
    void makeSureTableExists() throws BackendException {
        try (DynamoTable table = manager.tableMgr.getTable(tableName, tableDef)) {
            LOG.debug("Table exists: " + table.get().getTableName());
        }
    }

    private final StaticArrayEntry.GetColVal<Item, byte[]> dynamoEntryGetter = new StaticArrayEntry.GetColVal<Item, byte[]>() {
        @Override
        public byte[] getColumn(Item element) {
            return unsignedBytesToBytes(element.getBinary(SORT_KEY));
        }

        @Override
        public byte[] getValue(Item element) {
            return element.getBinary(COL_NAME);
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

    private List<Item> pruneOutEndKey(ItemCollection<?> results, byte[] end) {
        List<Item> pruned = new ArrayList<>();
        for (Item item : results) {
            if (!Arrays.equals(item.getBinary(SORT_KEY), end)) pruned.add(item);
        }
        return pruned;
    }

    private List<Item> applyLimit(List<Item> results, SliceQuery query) {
        return query.hasLimit() && query.getLimit() < results.size() ? results.subList(0, query.getLimit()) : results;
    }

    private List<Item> sortColNames(List<Item> results) {
        results.sort(Comparator.comparing(o -> o.getByteBuffer(SORT_KEY)));
        return results;
    }


}

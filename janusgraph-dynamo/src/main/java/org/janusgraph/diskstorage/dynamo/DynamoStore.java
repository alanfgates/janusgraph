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

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.EMPTY_BYTE_ARRAY;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.NULL_BYTE_ARRAY_VALUE;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.bytesToStrMinusOneFactory;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.logBytes;
import static org.janusgraph.diskstorage.dynamo.ConfigConstants.PARTITION_KEY;
import static org.janusgraph.diskstorage.dynamo.ConfigConstants.SORT_KEY;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.bytesToStrFactory;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.strToBytes;

public class DynamoStore implements KeyColumnValueStore {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoStore.class);

    private final DynamoStoreManager manager;
    private final String storeName;

    public DynamoStore(DynamoStoreManager mgr, String storeName) {
        manager = mgr;
        this.storeName = storeName;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        // Dynamo doesn't support conditions on attribute names in the item.  So this fetches back the entire
        // item and then returns only columns in the slice.
        try (DynamoTable table = getTable()) {
            Item item = table.get().getItem(PARTITION_KEY, storeName, SORT_KEY, query.getKey().as(bytesToStrFactory));
            if (item == null) return StaticArrayEntryList.of(Collections.emptyList());
            SortedMap<String, Object> sortedItem = sortFilterLimit(query, item);
            return StaticArrayEntryList.ofBytes(sortedItem.entrySet(), itemEntryGetter);
        } catch (Exception e) {
            LOG.error("Failed to fetch item in store " + storeName + " for key " + query.getKey().as(bytesToStrFactory), e);
            throw new TemporaryBackendException("Failed to fetch item", e);
        }
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) {
        // TODO could implement this as range scan, but if teh keys are few and far apart we'll end up reading
        // a lot of values for little benefit
        throw new UnsupportedOperationException();
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        try (DynamoTable table = getTable()) {
            List<AttributeUpdate> updates = new ArrayList<>();
            for (StaticBuffer colToDrop : deletions) {
                updates.add(new AttributeUpdate(colToDrop.as(bytesToStrFactory)).delete());
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Added delete for column " + colToDrop.as(bytesToStrFactory));
                }
            }
            for (Entry addition : additions) {
                byte[] val = addition.getValue().as(StaticBuffer.ARRAY_FACTORY);
                if (val == null || val.length == 0) val = NULL_BYTE_ARRAY_VALUE;
                updates.add(new AttributeUpdate(addition.getColumn().as(bytesToStrFactory)).put(val));
                if (LOG.isTraceEnabled()) {
                    logBytes("Added insert for column " + addition.getColumn().as(bytesToStrFactory) + " with value ",
                        addition.getValue().as(StaticBuffer.ARRAY_FACTORY));
                }
            }
            UpdateItemSpec update = new UpdateItemSpec()
                .withPrimaryKey(PARTITION_KEY, storeName, SORT_KEY, key.as(bytesToStrFactory))
                .withAttributeUpdate(updates);
            if (LOG.isDebugEnabled()) LOG.debug("Doing update on key " + key.as(bytesToStrFactory));
            table.get().updateItem(update);
        } catch (Exception e) {
            LOG.error("Failed to run update for key " + key.as(bytesToStrFactory), e);
            throw new TemporaryBackendException("Failed to run update", e);

        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) {
        throw new UnsupportedOperationException();

    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return doScan(query, query.getKeyStart(), query.getKeyEnd());
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        return doScan(query, null, null);
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() {
    }

    private KeyIterator doScan(SliceQuery sliceQuery, StaticBuffer keyStart, StaticBuffer keyEnd) throws BackendException {
        String end = keyEnd == null ? null : keyEnd.as(bytesToStrMinusOneFactory);
        try (DynamoTable table = getTable()) {
            QuerySpec querySpec = new QuerySpec()
                .withHashKey(PARTITION_KEY, storeName);
            LOG.debug("Starting query with partition key " + storeName);

            if (keyStart != null) {
                querySpec.withRangeKeyCondition(
                    new RangeKeyCondition(SORT_KEY)
                        .between(keyStart.as(bytesToStrFactory), end));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding range key condition between " + keyStart.as(bytesToStrFactory) + " and " + end);
                }
            }
            ItemCollection<QueryOutcome> results = table.get().query(querySpec);
            final Iterator<Item> resultsIter = results.iterator();

            return new KeyIterator() {
                boolean closed = false;
                SortedMap<String, Object> currSortedItem;
                Item currItem;


                @Override
                public RecordIterator<Entry> getEntries() {
                    if (closed) throw new IllegalStateException("Attempt to call hasNext on closed iterator");
                    return new RecordIterator<Entry>() {
                        boolean innerClosed = false;
                        Iterator<Map.Entry<String, Object>> itemIter = currSortedItem.entrySet().iterator();

                        @Override
                        public void close() {
                            innerClosed = true;

                        }

                        @Override
                        public boolean hasNext() {
                            if (innerClosed) throw new IllegalStateException("Attempt to call hasNext on closed iterator");
                            return itemIter.hasNext();
                        }

                        @Override
                        public Entry next() {
                            if (innerClosed) throw new IllegalStateException("Attempt to call hasNext on closed iterator");
                            return StaticArrayEntry.ofBytes(itemIter.next(), itemEntryGetter);
                        }
                    };
                }

                @Override
                public void close() {
                    closed = true;

                }

                @Override
                public boolean hasNext() {
                    if (closed) throw new IllegalStateException("Attempt to call hasNext on closed iterator");
                    while (resultsIter.hasNext()) {
                        currItem = resultsIter.next();
                        currSortedItem = sortFilterLimit(sliceQuery, currItem);
                        if (currSortedItem.size() > 0) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public StaticBuffer next() {
                    if (closed) throw new IllegalStateException("Attempt to call hasNext on closed iterator");
                    return StaticArrayBuffer.of(strToBytes(currItem.getString(SORT_KEY)));
                }
            };


        } catch (Exception e) {
            String msg = "Failed to run query on store " + storeName;
            if (keyStart != null) {
                msg += " and keyStart " + keyStart.as(bytesToStrFactory) + " and keyEnd " + end;
            }
            LOG.error(msg, e);
            throw new TemporaryBackendException(msg, e);
        }
    }

    private SortedMap<String, Object> sortFilterLimit(SliceQuery query, Item item) {
        Map<String, Object> unsortedMap = item.asMap();
        LOG.trace("sortFilterLimit passed item with " + unsortedMap.size() + " values");
        SortedMap<String, Object> sortedItem =
            new TreeMap<>(unsortedMap)
                .subMap(query.getSliceStart().as(bytesToStrFactory), query.getSliceEnd().as(bytesToStrFactory));
        LOG.trace("After filtering for slice query map has " + sortedItem.size() + " values");
        // If there's a limit, apply it
        if (query.hasLimit() && query.getLimit() < sortedItem.size()) {
            Iterator<Map.Entry<String, Object>> iter = sortedItem.entrySet().iterator();
            String lastKey = "";
            for (int i = 0; i <= query.getLimit(); i++) {
                assert iter.hasNext();
                lastKey = iter.next().getKey();
            }
            sortedItem = sortedItem.headMap(lastKey);
            LOG.trace("After applying limit map has " + sortedItem.size() + " values");
        }
        return sortedItem;
    }

    private DynamoTable getTable() throws BackendException {
        return new DynamoTable(manager.getPool(), manager.getStorageConfig());
    }

    private final StaticArrayEntry.GetColVal<Map.Entry<String, Object>, byte[]> itemEntryGetter = new StaticArrayEntry.GetColVal<Map.Entry<String, Object>, byte[]>() {
        @Override
        public byte[] getColumn(Map.Entry<String, Object> element) {
            return strToBytes(element.getKey());
        }

        @Override
        public byte[] getValue(Map.Entry<String, Object> element) {
            byte[] b = (byte[])element.getValue();
            if (Arrays.equals(NULL_BYTE_ARRAY_VALUE, b)) {
                b = EMPTY_BYTE_ARRAY;
            }
            return b;
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<String, Object> element) {
            return manager.getMetaDataSchema(storeName);
        }

        @Override
        public Object getMetaData(Map.Entry<String, Object> element, EntryMetaData meta) {
            throw new UnsupportedOperationException();
        }
    };
}

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
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.janusgraph.diskstorage.dynamo.DynamoStore.COL_NAME;
import static org.janusgraph.diskstorage.dynamo.DynamoStore.PARTITION_KEY;
import static org.janusgraph.diskstorage.dynamo.DynamoStore.SORT_KEY;
import static org.janusgraph.diskstorage.dynamo.Utils.unsignedBytesFactory;

/**
 * Collects mutations for {@link DynamoStoreManager#mutateMany(Map, StoreTransaction)} and produces a set of
 * {@link com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec}s.  Each of these has to have 25 or
 * less operations as that is the max size allowed by
 * {@link com.amazonaws.services.dynamodbv2.document.DynamoDB#batchWriteItem(BatchWriteItemSpec)}.
 *
 * <p> Since JanusGraph also expects deletes to be done before inserts, the returned BatchWriteItems are ordered
 * to have all deletes for a given table before any inserts for that table.
 * </p>
 * <p>
 *   Since batching all these requests up can take a bit and making a bunch of calls to Dynamo can take a bit, this
 *   class runs the batching in a separate thread so that it can start returning results to the caller to be passed off
 *   to Dynamo as quickly as possible.
 * </p>
 */
class MutationCollector implements Iterable<BatchWriteItemSpec> {

    private static final Logger LOG = LoggerFactory.getLogger(MutationCollector.class);
    private final static int MAX_BATCH_OPS = 25;

    private class CountedBatchWriteItemSpec {
        final BatchWriteItemSpec spec;
        final Map<String, TableWriteItems> tableWriteItems;
        int cnt;
        boolean containsDeletes;

        CountedBatchWriteItemSpec() {
            spec = new BatchWriteItemSpec();
            cnt = 0;
            tableWriteItems = new HashMap<>();
            containsDeletes = false;
        }

        void insertDelete(String tableName, StaticBuffer partitionKey, StaticBuffer sortKey) {
            assert cnt < MAX_BATCH_OPS;
            TableWriteItems twi = getTableWriteItems(tableName);
            twi.addHashAndRangePrimaryKeyToDelete(PARTITION_KEY, partitionKey.as(StaticBuffer.ARRAY_FACTORY),
                SORT_KEY, sortKey.as(unsignedBytesFactory));
            cnt++;
            containsDeletes = true;
        }

        void insertInsert(String tableName, StaticBuffer partitionKey, Entry entry) {
            assert cnt < MAX_BATCH_OPS && !containsDeletes;
            TableWriteItems twi = getTableWriteItems(tableName);
            twi.addItemToPut(
                new Item()
                    .withPrimaryKey(PARTITION_KEY, partitionKey.as(StaticBuffer.ARRAY_FACTORY), SORT_KEY, entry.getColumn().as(unsignedBytesFactory))
                    .withBinary(COL_NAME, entry.getValue().as(StaticBuffer.ARRAY_FACTORY))
            );
            cnt++;
        }

        private TableWriteItems getTableWriteItems(String tableName) {
            return tableWriteItems.computeIfAbsent(tableName, tn -> {
                TableWriteItems t = new TableWriteItems(tn);
                spec.withTableWriteItems(t);
                return t;
            });
        }
    }

    private Deque<CountedBatchWriteItemSpec> batches;


    MutationCollector() {
        batches = new ArrayDeque<>();
    }

    void addMutationsForOneTable(final String tableName, final Map<StaticBuffer, KCVMutation> mutations) {
        for (Map.Entry<StaticBuffer, KCVMutation> entry : mutations.entrySet()) {
            StaticBuffer hashKey = entry.getKey();
            KCVMutation kvcm = entry.getValue();
            if (kvcm.getDeletions() != null) {
                for (StaticBuffer colToDrop : kvcm.getDeletions()) {
                    CountedBatchWriteItemSpec currentDelete = getLastSpec(false);
                    currentDelete.insertDelete(tableName, hashKey, colToDrop);
                }
            }
            if (kvcm.getAdditions() != null) {
                for (Entry janusEntry : kvcm.getAdditions()) {
                    CountedBatchWriteItemSpec currentInsert = getLastSpec(true);
                    currentInsert.insertInsert(tableName, hashKey, janusEntry);
                }
            }
        }
    }

    @Override
    public Iterator<BatchWriteItemSpec> iterator() {
        return new Iterator<BatchWriteItemSpec>() {
            @Override
            public boolean hasNext() {
                return batches.size() != 0;
            }

            @Override
            public BatchWriteItemSpec next() {
                if (batches.size() == 0) throw new NoSuchElementException("No batches left!");
                return batches.pop().spec;
            }
        };
    }

    private CountedBatchWriteItemSpec getLastSpec(boolean isInsert) {
        CountedBatchWriteItemSpec latest = batches.isEmpty() ? null : batches.getLast();
        if (latest == null || latest.cnt >= MAX_BATCH_OPS || (latest.containsDeletes && isInsert)) {
            LOG.trace("Adding a new batch as this is the first one or the last one was full or the last one had" +
                " deletes and this is an insert.  Last one had " + (latest == null ? 0 : latest.cnt) + " entries");
            latest = new CountedBatchWriteItemSpec();
            batches.addLast(latest);
        }
        return latest;
    }


}

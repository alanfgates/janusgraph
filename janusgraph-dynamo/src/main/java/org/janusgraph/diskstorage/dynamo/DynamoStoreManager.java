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

import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.NULL_BYTE_ARRAY_VALUE;
import static org.janusgraph.diskstorage.dynamo.BytesStrTranslations.bytesToStrFactory;
import static org.janusgraph.diskstorage.dynamo.ConfigConstants.DYNAMO_TABLE_NAME;
import static org.janusgraph.diskstorage.dynamo.ConfigConstants.PARTITION_KEY;
import static org.janusgraph.diskstorage.dynamo.ConfigConstants.SORT_KEY;

/**
 * StoreManager that stores data in Amazon's DynamoDB.  All of the data is stored in a single table.  The JanusGraph
 * store is used as the partition key for the table and the JanusGraph key as the sort key.  This enables efficient
 * querying of all the entries in a store, since a Dynamo query can't access more than one partition key.
 * <p>
 * The JanusGraph column name is then used as the attribute name in the Dynamo item for the (storename, januskey) primary key,
 * and the value stored in JanusGraph column is stored as the value for that attribute in the Dynamo item.  This allows
 * somewhat efficient slice queries since filters can be applied on the attribute names.
 * <p>
 * All of this comes with a drawback, which is that the januskey and the janus column name have to be converted to
 * Strings.  The janus key has to be converted to something sortable since Dynamo sorts byte[] using Java and thus
 * screws up any value greater than 0x7f.  The janus column name has to be converted to a string because Dynamo item
 * attribute names have to be strings.  Needing to parse the strings coming out of Dynamo isn't the most efficient.
 * <p>
 * For details on JanusGraph's data model and how Amazon DynamoDB works with see the following.
 * @see <a href="https://docs.janusgraph.org/0.1.1/data-model.html">JanusGraph Data Model</a>
 * @see <a href="https://d1.awsstatic.com/whitepapers/AWS_Comparing_the_Use_of_DynamoDB_and_HBase_for_NoSQL.pdf>Comparing the Use of Amazon DynamoDB and Apache HBase for NoSQL</a>
 *
 * <p>
 * Nothing special happens here regarding credentials.  We use the standard Amazon method of gathering
 * crecentials, environment variables, system properties or the ~/.aws/credentials file.
 *
 * @see <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">Working with AWS Credentials</a>
 *
 */
public class DynamoStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    static final String NAME = "janusgraph";

    private static final Logger LOG = LoggerFactory.getLogger(DynamoStoreManager.class);

    private final Map<String, DynamoStore> stores; // Don't access directly, use getStore()
    private final ConnectionPool pool;
    private final String tableName;

    public DynamoStoreManager(Configuration conf) {
        super(conf);
        pool = new ConnectionPool(conf);
        stores = new HashMap<>();
        tableName = storageConfig.get(DYNAMO_TABLE_NAME);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) {
        return getStore(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        LOG.debug("received mutateMany with " + mutations.size() + " different stores");
        Deque<OneMutation> byKey = new ArrayDeque<>();
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeEntry : mutations.entrySet()) {
            LOG.debug("mutate many for store " + storeEntry.getKey() + " with " + storeEntry.getValue().size() + " different keys");
            for (Map.Entry<StaticBuffer, KCVMutation> keyEntry : storeEntry.getValue().entrySet()) {
                byKey.add(new OneMutation(storeEntry.getKey(), keyEntry.getKey(), keyEntry.getValue()));
            }
        }
        // If there is just one, call store.mutate() as we have to make two calls for every set of 25
        // in here anyway
        if (byKey.size() == 1) {
            OneMutation oneMutation = byKey.pop();
            DynamoStore store = getStore(oneMutation.storeName);
            store.mutate(oneMutation.keyAsBuffer, oneMutation.mutations.getAdditions(),
                oneMutation.mutations.getDeletions(), txh);
        } else {
            try (DynamoConnection conn = pool.getConnection()) {
                // DynamoDB can only handle 25 mods in one call
                while (byKey.size() > 1) {
                    Map<Map.Entry<String, String>, OneMutation> batch = new HashMap<>(25);
                    for (int i = 0; i < 25 && byKey.size() > 0; i++) {
                        OneMutation one = byKey.pop();
                        batch.put(new AbstractMap.SimpleEntry<>(one.storeName, one.key), one);
                    }
                    TableKeysAndAttributes keysToFetch = new TableKeysAndAttributes(DynamoTable.getTableName(storageConfig));
                    for (OneMutation one : batch.values()) {
                        keysToFetch.addHashAndRangePrimaryKey(PARTITION_KEY, one.storeName, SORT_KEY, one.key);
                    }
                    BatchGetItemOutcome fetchOutcome = conn.get().batchGetItem(keysToFetch);

                    List<Item> itemsToUpdate = new ArrayList<>();
                    // There may or may not be values for the fetched keys as this key could be new.  So first walk through
                    // the items we got back and update them based on the mutations.  Then add the rest of the mutations
                    // There should be at most one entry in the map because we only fetched from one table
                    assert fetchOutcome.getTableItems().size() == 0 || fetchOutcome.getTableItems().size() == 1;
                    for (Item fetchedItem : fetchOutcome.getTableItems().get(DynamoTable.getTableName(storageConfig))) {
                        Map.Entry<String, String> key =
                            new AbstractMap.SimpleEntry<>(fetchedItem.getString(PARTITION_KEY), fetchedItem.getString(SORT_KEY));
                        OneMutation existing = batch.remove(key);
                        assert existing != null;
                        for (StaticBuffer colToDrop : existing.mutations.getDeletions()) {
                            fetchedItem.removeAttribute(colToDrop.as(bytesToStrFactory));
                        }
                        for (Entry addition : existing.mutations.getAdditions()) {
                            byte[] val = addition.getValue().as(StaticBuffer.ARRAY_FACTORY);
                            if (val == null || val.length == 0) val = NULL_BYTE_ARRAY_VALUE;
                            fetchedItem.withBinary(addition.getColumn().as(bytesToStrFactory), val);
                        }
                        itemsToUpdate.add(fetchedItem);
                    }
                    // There may be new entries left over that just need inserted
                    for (Map.Entry<Map.Entry<String, String>, OneMutation> one : batch.entrySet()) {
                        Item itemToInsert = new Item();
                        itemToInsert.withString(PARTITION_KEY, one.getKey().getKey());
                        itemToInsert.withString(SORT_KEY, one.getKey().getValue());
                        for (Entry addition : one.getValue().mutations.getAdditions()) {
                            byte[] val = addition.getValue().as(StaticBuffer.ARRAY_FACTORY);
                            if (val == null || val.length == 0) val = NULL_BYTE_ARRAY_VALUE;
                            itemToInsert.withBinary(addition.getColumn().as(bytesToStrFactory), val);
                        }
                        itemsToUpdate.add(itemToInsert);
                    }
                    TableWriteItems keysToUpdate =
                        new TableWriteItems(DynamoTable.getTableName(storageConfig)).withItemsToPut(itemsToUpdate);
                    conn.get().batchWriteItem(keysToUpdate);
                }
            } catch (Exception e) {
                LOG.error("Failed to run batch update", e);
                throw new TemporaryBackendException("Failed to run batch update", e);
            }
        }
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) {
        return new DynamoTransaction(config);
    }

    @Override
    public void close() {
        clearStores();
        pool.closeAll();
    }

    @Override
    public void clearStorage() throws BackendException {
        DynamoTable.drop(pool, storageConfig);
    }

    @Override
    public boolean exists() throws BackendException {
        return DynamoTable.exists(pool, storageConfig);
    }

    @Override
    public StoreFeatures getFeatures() {
        return new StandardStoreFeatures.Builder()
            .unorderedScan(true)
            .orderedScan(true)
            .batchMutation(true)
            .distributed(true)
            .persists(true)
            .supportsInterruption(true)
            .build();
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() {
        throw new UnsupportedOperationException();
        // There's no notion of local in Dynamo, so we don't support this
    }

    ConnectionPool getPool() {
        return pool;
    }

    private DynamoStore getStore(final String storeName) {
        return stores.computeIfAbsent(storeName, s -> new DynamoStore(DynamoStoreManager.this, storeName));
    }

    // This just removes our record of the stores.  It doesn't actually drop the underlying tables
    private void clearStores() {
        for (DynamoStore store : stores.values()) store.close();
        stores.clear();
    }

    private class OneMutation {
        final String storeName;
        final String key;
        final StaticBuffer keyAsBuffer;
        final KCVMutation mutations;

        OneMutation(String storeName, StaticBuffer key, KCVMutation mutations) {
            this.storeName = storeName;
            this.keyAsBuffer = key;
            this.key = key.as(bytesToStrFactory);
            this.mutations = mutations;
        }
    }

}

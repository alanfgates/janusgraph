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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigOption;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.diskstorage.dynamo.DynamoStore.COL_NAME;
import static org.janusgraph.diskstorage.dynamo.DynamoStore.PARTITION_KEY;
import static org.janusgraph.diskstorage.dynamo.DynamoStore.SORT_KEY;

/**
 * StoreManager that stores data in Amazon's DynamoDB.  All of the data is stored in a table per store.  The name
 * of that table will be <i>configval</i>.<i>storename</i> where the configval is the base name taken from the config
 * file that will be used for all tables.  Inside the table the JanusGraph key is used as the hash key for DynamoDB
 * and the JanusGraph column name is used as the sort key.  This allows us to do efficient SliceQueries where slices
 * of JanusGraph columns are looked up.  It means that every entry in DynamoDB has the two part key (Janus key,
 * Janus column) and then a single attribute, the Janus column value.  As DynamoDB does not support range queries
 * on the hash key this implementation does not support ordered key operations on Janus keys.
 *
 * @see <a href="https://docs.janusgraph.org/0.1.1/data-model.html">JanusGraph Data Model</a>
 * @see <a href="https://d1.awsstatic.com/whitepapers/AWS_Comparing_the_Use_of_DynamoDB_and_HBase_for_NoSQL.pdf>Comparing the Use of Amazon DynamoDB and Apache HBase for NoSQL</a>
 */
public class DynamoStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    static final String NAME = "janusgraph";
    public static final ConfigOption<String> DYNAMO_REGION = new ConfigOption<>(Utils.DYNAMO_NS,
        "dynamo-region", "AWS region to access", ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> DYNAMO_URL = new ConfigOption<>(Utils.DYNAMO_NS,
        "dynamo-url", "URL to connect to DynamoDB", ConfigOption.Type.LOCAL, String.class);

    private static final Logger LOG = LoggerFactory.getLogger(DynamoStoreManager.class);

    private final Map<String, DynamoStore> stores; // Don't access directly, use getStore()
    private DynamoDB dynamo; // connection to dynamo, don't access directly, call getDynamo()

    public DynamoStoreManager(Configuration conf) {
        super(conf);

        stores = new HashMap<>();
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        return getStore(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        BatchWriteItemSpec batchSpec = new BatchWriteItemSpec();
        // Per the DynamoDB docs, you can only do 25 batched items per call.  The BatchedWriteItemOutcome
        // includes a list of unprocessed items, and the interface has a call to pass back unprocessed items
        // to Dynamo.  I'm guessing I can just stack everything in one request, it will process as many
        // as it does, and then I can keep passing it back the unprocessed items until they reach zero.
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> topEntry : mutations.entrySet()) {
            DynamoStore store = getStore(topEntry.getKey());
            // Table creation in the stores is lazy, as is store creation.  If the preceding line created a store,
            // then we need to make sure that the table gets created.  Otherwise the subsequent bulk operations will
            // fail.
            store.getTable();
            String tableName = store.getTableName();
            for (Map.Entry<StaticBuffer, KCVMutation> e : topEntry.getValue().entrySet()) {
                StaticBuffer hashKey = e.getKey();
                TableWriteItems tableOps = new TableWriteItems(tableName);
                // Handle the deletions
                for (StaticBuffer colToDrop : e.getValue().getDeletions()) {
                    tableOps.addHashAndRangePrimaryKeyToDelete(PARTITION_KEY, hashKey.asByteBuffer(), SORT_KEY, colToDrop.asByteBuffer());
                }
                // Handle the insert
                for (Entry entry : e.getValue().getAdditions()) {
                    tableOps.addItemToPut(
                        new Item()
                            .withPrimaryKey(PARTITION_KEY, hashKey.asByteBuffer(), SORT_KEY, entry.getColumn().asByteBuffer())
                            .withBinary(COL_NAME, entry.getValue().asByteBuffer())
                    );
                }
                batchSpec.withTableWriteItems(tableOps);
            }
        }
        try {
            BatchWriteItemOutcome outcome = getDynamo().batchWriteItem(batchSpec);
            // Make sure we're converging to zero
            int itemsToGo = outcome.getUnprocessedItems().size();
            while (itemsToGo > 0) {
                outcome = getDynamo().batchWriteItemUnprocessed(outcome.getUnprocessedItems());
                if (outcome.getUnprocessedItems().size() >= itemsToGo) {
                    LOG.error("Failing to make progress writing batch items, last time we had " +
                        itemsToGo + ", this time we have " + outcome.getUnprocessedItems().size());
                    throw new TemporaryBackendException("Failed to make progress writing batch items");
                }
                itemsToGo = outcome.getUnprocessedItems().size();
            }
        } catch (Exception e) {
            if (e instanceof TemporaryBackendException) throw e;
            LOG.error("Failed to write batch items", e);
            throw new TemporaryBackendException("Failed to write batch items", e);
        }
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new DynamoTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        clearStores();

    }

    @Override
    public void clearStorage() throws BackendException {
        for (DynamoStore store : stores.values()) store.drop();
        clearStores();
    }

    @Override
    public boolean exists() throws BackendException {
        return true; // TODO - don't know if this is right, but I'm not sure what existence means here
    }

    @Override
    public StoreFeatures getFeatures() {
        return new StandardStoreFeatures.Builder()
            .unorderedScan(true)
            .batchMutation(true)
            .distributed(true)
            .persists(true)
            .supportsInterruption(true)
            .build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
        // There's no notion of local in Dynamo, so we don't support this
    }

    private DynamoStore getStore(String storeName) throws BackendException {
        DynamoStore store = stores.get(storeName);
        if (store == null) {
            stores.put(storeName, new DynamoStore(this, storageConfig, storeName));
        }
        return store;
    }

    private void clearStores() throws BackendException {
        for (DynamoStore store : stores.values()) store.close();
        stores.clear();
    }

    DynamoDB getDynamo() {
        if (dynamo == null) {
            Preconditions.checkArgument(storageConfig.has(DYNAMO_URL) && storageConfig.has(DYNAMO_REGION));

            // TODO - following taken from example DynamoDB code, probably way more that needs
            // done here, like user credentials, security tokens, etc.
            LOG.info("Connecting to DynamoDB at endpoint " + storageConfig.get(DYNAMO_URL) + " and region " +
                storageConfig.get(DYNAMO_REGION));
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder
                .standard()
                .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(storageConfig.get(DYNAMO_URL), storageConfig.get(DYNAMO_REGION))
                )
                .build();
            dynamo = new DynamoDB(client);
        }
        return dynamo;
    }

}

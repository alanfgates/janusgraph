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
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.diskstorage.dynamo.DynamoStore.DYNAMO_READ_THROUGHPUT;
import static org.janusgraph.diskstorage.dynamo.DynamoStore.DYNAMO_TABLE_BASE;
import static org.janusgraph.diskstorage.dynamo.DynamoStore.DYNAMO_WRITE_THROUGHPUT;

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
 *
 * <p>Nothing special happens here regarding credentials.  We use the standard Amazon method of gathering
 * crecentials, environment variables, system properties or the ~/.aws/credentials file.</p>
 *
 * @see <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">Working with AWS Credentials</a>
 *
 * <p>All of the various stores are tracked in a table called the storesTable.  This allows us to properly track whether
 * any stores exist and all the stores we need to clear if someone calls clearStorage.</p>
 */
public class DynamoStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    static final String NAME = "janusgraph";
    public static final ConfigOption<String> DYNAMO_REGION = new ConfigOption<>(Utils.DYNAMO_NS,
        "dynamo-region", "AWS region to access", ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> DYNAMO_URL = new ConfigOption<>(Utils.DYNAMO_NS,
        "dynamo-url", "URL to connect to DynamoDB", ConfigOption.Type.LOCAL, String.class);

    private static final String STORES_TABLE_NAME = "storestable";
    private static final String STORES_TABLE_PART_KEY = "name";
    private static final Logger LOG = LoggerFactory.getLogger(DynamoStoreManager.class);

    private final Map<String, DynamoStore> stores; // Don't access directly, use getStore()
    private DynamoDB dynamo; // connection to dynamo, don't access directly, call getDynamo()
    private Table storesTable; // handle for the table that store names of tables we are using in Dynamo.  Don't access direclty, call getStoresTable()

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
        MutationCollector collector = new MutationCollector();
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> topEntry : mutations.entrySet()) {
            DynamoStore store = getStore(topEntry.getKey());
            // Table creation in the stores is lazy, as is store creation.  If the preceding line created a store,
            // then we need to make sure that the table gets created.  Otherwise the subsequent bulk operations will
            // fail.
            store.getTable();
            String tableName = store.getTableName();
            collector.addMutationsForOneTable(tableName, topEntry.getValue());
        }
        try {
            for (BatchWriteItemSpec batchSpec : collector) {
                getDynamo().batchWriteItem(batchSpec);
            }
        } catch (Exception e) {
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
        if (dynamo != null) dynamo.shutdown();
        dynamo = null;
        storesTable = null;

    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            for (Item storeInfo : getAllStores()) {
                // Go around getStore() here, because it does a bunch of stuff we don't need
                String storeName = storeInfo.getString(STORES_TABLE_PART_KEY);
                DynamoStore store = stores.get(storeName);
                if (store == null) store = new DynamoStore(this, storageConfig, storeName);
                store.drop();
                getStoresTable().deleteItem(STORES_TABLE_PART_KEY, storeName);
            }
            clearStores();
        } catch (Exception e) {
            LOG.error("Failed to clear storage", e);
            throw new TemporaryBackendException("Failed to clear storage", e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        // If there's at least one store, than we exist
        return getAllStores().iterator().hasNext();
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

    private DynamoStore getStore(String storeName) throws BackendException {
        DynamoStore store = stores.get(storeName);
        if (store == null) {
            store = addStore(storeName);
            stores.put(storeName, store);
        }
        return store;
    }

    // This just removes our record of the stores.  It doesn't actually drop the underlying tables
    private void clearStores() throws BackendException {
        for (DynamoStore store : stores.values()) store.close();
        stores.clear();
    }

    private DynamoStore addStore(String storeName) throws BackendException {
        // See if the store exists, if not, add it
        try {
            Item item = getStoresTable().getItem(STORES_TABLE_PART_KEY, storeName);
            if (item == null) {
                // This store doesn't exist yet, so we need to record it
                getStoresTable().putItem(new Item().withPrimaryKey(STORES_TABLE_PART_KEY, storeName));
            }
            return new DynamoStore(this, storageConfig, storeName);
        } catch (Exception e) {
            LOG.error("Failed to fetch information about the store in the table table", e);
            throw new TemporaryBackendException("Failed to fetch information about the store", e);
        }
    }

    private Table getStoresTable() throws BackendException {
        if (storesTable == null) {
            String tableName = storageConfig.get(DYNAMO_TABLE_BASE) + STORES_TABLE_NAME;
            TableCollection<ListTablesResult> existingTables = getDynamo().listTables(storageConfig.get(DYNAMO_TABLE_BASE));
            for (Table maybe : existingTables) {
                if (maybe.getTableName().equals(tableName)) {
                    storesTable = maybe;
                    break;
                }
            }
            // We didn't find it in the list, so we need to create it.
            if (storesTable == null) {
                LOG.info("Unable to find table " + tableName + ", will create it");
                storesTable = getDynamo().createTable(tableName,
                    Collections.singletonList(new KeySchemaElement(STORES_TABLE_PART_KEY, KeyType.HASH)),
                    Collections.singletonList(new AttributeDefinition(STORES_TABLE_PART_KEY, ScalarAttributeType.S)),
                    new ProvisionedThroughput(storageConfig.get(DYNAMO_READ_THROUGHPUT), storageConfig.get(DYNAMO_WRITE_THROUGHPUT)));
                try {
                    storesTable.waitForActive();
                } catch (InterruptedException e) {
                    throw new TemporaryBackendException("Interupted waiting for table to be active", e);
                }
            }
        }
        return storesTable;
    }

    // This returns a list of all of the stores that have been created, not just the ones cached in this instance.
    private Iterable<Item> getAllStores() throws BackendException {
        try {
            return getStoresTable().scan(new ScanSpec());
        } catch (Exception e) {
            LOG.error("Failed to scan storesTable to find all existing stores");
            throw new TemporaryBackendException("Failed to scan storesTable to find all existing stores", e);
        }

    }

}

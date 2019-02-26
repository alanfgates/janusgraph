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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.diskstorage.dynamo.ConfigConstants.DYNAMO_TABLE_NAME;

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
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) {
        throw new UnsupportedOperationException();
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

}

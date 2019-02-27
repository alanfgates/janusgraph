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

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

import static org.janusgraph.diskstorage.dynamo.ConfigConstants.DYNAMO_TABLE_NAME;
import static org.janusgraph.diskstorage.dynamo.ConfigConstants.PARTITION_KEY;
import static org.janusgraph.diskstorage.dynamo.ConfigConstants.SORT_KEY;

/**
 * A reference to a DynamoTable.  Wrapped to handle closing the dynamo connection in the table and returning
 * it to the connection pool.
 */
class DynamoTable implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoTable.class);

    private static boolean knownToExist = false;

    private final DynamoConnection dynamo;
    private final Table table;

    DynamoTable(ConnectionPool pool, Configuration conf) throws BackendException {
        dynamo = pool.getConnection(); // Don't auto close this, we'll close it when this table connection is closed
        if (exists(pool, conf)) {
            table = dynamo.get().getTable(getTableName(conf));
        } else {
            synchronized (DynamoTable.class) {
                // Somebody else might have jumped in and created it while we waited
                if (!knownToExist) {
                    CreateTableRequest tableDef = new CreateTableRequest()
                        .withTableName(getTableName(conf))
                        .withKeySchema(
                            new KeySchemaElement(PARTITION_KEY, KeyType.HASH),
                            new KeySchemaElement(SORT_KEY, KeyType.RANGE))
                        .withAttributeDefinitions(
                            new AttributeDefinition(PARTITION_KEY, ScalarAttributeType.S),
                            new AttributeDefinition(SORT_KEY, ScalarAttributeType.S))
                        .withProvisionedThroughput(new ProvisionedThroughput(conf.get(ConfigConstants.DYNAMO_READ_THROUGHPUT), conf.get(ConfigConstants.DYNAMO_WRITE_THROUGHPUT)));
                    LOG.info("Unable to find table " + tableDef.getTableName() + ", will create it");
                    try {
                        table = dynamo.get().createTable(tableDef);
                        table.waitForActive();
                    } catch (Exception e) {
                        throw new TemporaryBackendException("Unable to create table", e);
                    }
                    knownToExist = true;
                } else {
                    table = dynamo.get().getTable(getTableName(conf));
                }
            }
        }
    }

    Table get() {
        return table;
    }


    @Override
    public void close() {
        dynamo.close();
    }

    /**
     * Check to see if the table exists, without creating it if it does not.
     * @param pool connection pool
     * @param conf configuration object
     * @return true if the table already exists
     * @throws BackendException if we don't know if the table exists and we can't connect to Dynamo
     * to figure it out
     */
    static boolean exists(ConnectionPool pool, Configuration conf) throws BackendException {
        if (!knownToExist) {
            try (DynamoConnection dynamo = pool.getConnection()) {
                TableCollection<ListTablesResult> existingTables = dynamo.get().listTables();
                for (Table maybe : existingTables) {
                    if (maybe.getTableName().equals(getTableName(conf))) {
                        knownToExist = true;
                    }
                }
            }
        }
        return knownToExist;
    }

    /**
     * Drop this table if it exists.
     * @param pool connection pool
     * @param conf configuration object
     * @throws BackendException if we cannot connect to Dynamo or the drop fails.
     */
    static void drop(ConnectionPool pool, Configuration conf) throws BackendException {
        if (exists(pool, conf)) {
            try (DynamoTable table = new DynamoTable(pool, conf)) {
                table.get().delete();
                knownToExist = false;
            } catch (Exception e) {
                LOG.error("Failed to drop table: " + e.getMessage(), e);
                throw new TemporaryBackendException("Failed to drop table", e);
            }
        }
    }

    static String getTableName(Configuration conf) {
        return conf.get(DYNAMO_TABLE_NAME);
    }
}

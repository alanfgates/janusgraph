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

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Manage Dynamo tables.  Abstracted because
 * {@link com.amazonaws.services.dynamodbv2.document.DynamoDB#getTable(String)} just gets a handle to a table without
 * regard to whether that table really exists.  This class tracks which tables have been assured to exist so that
 * we don't spend time repeatedly checking.
 */
class DynamoTableManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoTableManager.class);

    private final Configuration conf;
    private final ConnectionPool pool;
    private final Set<String> knownTables;


    DynamoTableManager(Configuration conf, ConnectionPool pool) {
        this.conf = conf;
        this.pool = pool;
        knownTables = new HashSet<>();
    }

    synchronized DynamoTable getTable(String tableName, CreateTableRequest tableDef) throws BackendException {
        if (knownTables.contains(tableName)) {
            return new DynamoTable(pool, tableName);
        } else {
            DynamoTable dynamoTable = new DynamoTable(pool, conf, tableDef);
            knownTables.add(tableDef.getTableName());
            return dynamoTable;
        }
    }



}

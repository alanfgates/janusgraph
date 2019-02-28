// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage.jdbc;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class JdbcKeyValueStoreTest extends KeyValueStoreTest {
    private static final Logger log = LoggerFactory.getLogger(JdbcKeyValueStoreTest.class);

    @Rule public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();
    private JdbcStoreManager mgr;


    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        if (mgr == null) {
            log.debug("Creating new storage manager");
            DataSource conn = pg.getEmbeddedPostgres().getPostgresDatabase();
            mgr = new JdbcStoreManager(GraphDatabaseConfiguration.buildGraphConfiguration(), conn);
        }
        return mgr;
    }
}

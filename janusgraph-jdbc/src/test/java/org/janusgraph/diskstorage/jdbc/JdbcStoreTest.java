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
package org.janusgraph.diskstorage.jdbc;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class JdbcStoreTest extends KeyColumnValueStoreTest {
    private static final Logger log = LoggerFactory.getLogger(JdbcStoreTest.class);

    private static String containerName;
    private KeyColumnValueStoreManager mgr;

    @BeforeClass
    public static void startPostgres() throws InterruptedException, SQLException, IOException {
        containerName = DockerUtils.startDocker();
    }

    @AfterClass
    public static void stopPostgres() throws IOException, InterruptedException {
        DockerUtils.shutdownDocker(containerName);
    }

    @Rule
    public TestWatcher testStartAndFinishLogNotification = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            log.debug("TEST Starting test " + description.getMethodName());
        }

        @Override
        protected void finished(Description description) {
            super.finished(description);
            log.debug("TEST Finished test " + description.getMethodName());
        }
    };

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        log.debug("Creating new storage manager");
        mgr = new PostgresStoreManager(DockerUtils.getConfig());
        return mgr;
    }
}

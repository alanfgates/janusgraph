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

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.util.ReadArrayBuffer;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DynamoStoreTest extends KeyColumnValueStoreTest {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoStoreTest.class);

    private static String containerName;

    @BeforeClass
    public static void startDynamo() throws IOException, InterruptedException {
        TestUtils.setFakeCredentials();
        containerName = TestUtils.startDockerDynamo();
    }

    @AfterClass
    public static void stopDynamo() throws IOException, InterruptedException {
        TestUtils.shutdownDockerDynamo(containerName);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() {
        return new DynamoStoreManager(TestUtils.getConfig());
    }

    @Test
    public void myTestGetKeysWithKeyRange() throws Exception {
        populateDBWith100Keys();

        tx.commit();
        tx = startTx();

        KeyIterator keyIterator = store.getKeys(new KeyRangeQuery(
            KeyColumnValueStoreUtil.longToByteBuffer(10), // key start
            KeyColumnValueStoreUtil.longToByteBuffer(40), // key end
            new ReadArrayBuffer("b".getBytes()), // column start
            new ReadArrayBuffer("c".getBytes())), tx);

        myExamineGetKeysResults(keyIterator, 10, 40);
    }

    protected void myExamineGetKeysResults(KeyIterator keyIterator,
                                         long startKey, long endKey) {
        Assert.assertNotNull(keyIterator);

        int count = 0;
        int expectedNumKeys = (int) (endKey - startKey);
        final List<StaticBuffer> existingKeys = new ArrayList<>(expectedNumKeys);

        for (int i = (int) (startKey == 0 ? 1 : startKey); i <= endKey; i++) {
            existingKeys.add(KeyColumnValueStoreUtil.longToByteBuffer(i));
            LOG.debug("Adding key " + i + " to existing keys");
        }

        while (keyIterator.hasNext()) {
            StaticBuffer key = keyIterator.next();

            Assert.assertNotNull(key);
            LOG.debug("Looking for key " + key.toString());
            Assert.assertTrue(existingKeys.contains(key));

            RecordIterator<Entry> entries = keyIterator.getEntries();

            Assert.assertNotNull(entries);

            int entryCount = 0;
            while (entries.hasNext()) {
                Assert.assertNotNull(entries.next());
                entryCount++;
            }

            Assert.assertEquals(1, entryCount);

            count++;
        }

        Assert.assertEquals(expectedNumKeys, count);
    }

}

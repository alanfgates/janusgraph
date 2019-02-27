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

import org.apache.commons.collections.map.HashedMap;
import org.janusgraph.diskstorage.AbstractKCVSTest;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.KeyColumnValueStoreUtil;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The provided unit tests don't seem to hammer all the cases of mutateMany, so add some tests for it here
 */
public class DynamoStoreManagerMutateManyTest extends AbstractKCVSTest {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoStoreManagerMutateManyTest.class);
    private static final int NUM_COLS = 10;

    private static String containerName;

    private KeyColumnValueStoreManager manager;

    @BeforeClass
    public static void startDynamo() throws IOException, InterruptedException {
        TestUtils.setFakeCredentials();
        containerName = TestUtils.startDockerDynamo();
    }

    @AfterClass
    public static void stopDynamo() throws IOException, InterruptedException {
        TestUtils.shutdownDockerDynamo(containerName);
    }

    @Before
    public void openStorageManager() {
        manager = new DynamoStoreManager(TestUtils.getConfig());
    }

    // Test that one mutateMany call can do inserts, updates, and deletes on multiple stores.  Make it big enough
    // that at least two batches are required
    @Test
    public void manyStoresManyKeys() throws BackendException {
        Map<String, Map<StaticBuffer, KCVMutation>> mutations = new HashMap<>();
        Map<String, Map<StaticBuffer, List<Entry>>> expected = new HashMap<>();
        for (int i = 0; i < 11; i++) {
            String storeName = "msmk" + i;
            Map<StaticBuffer, KCVMutation> keyEntry = mutations.computeIfAbsent(storeName, sn -> new HashMap<>());
            Map<StaticBuffer, List<Entry>> expectedKeyEntry = expected.computeIfAbsent(storeName, sn -> new HashMap<>());
            for (int j = 0; j < 13; j++) {
                StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(j);
                List<Entry> inserts = insertColumnsForOneKey(storeName, key);
                List<Entry> upserts = new ArrayList<>();
                // change the value of the 2nd element
                upserts.add(StaticArrayEntry.of(KeyColumnValueStoreUtil.longToByteBuffer(1), KeyColumnValueStoreUtil.longToByteBuffer(100)));
                // add a 10th element
                StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(NUM_COLS);
                upserts.add(StaticArrayEntry.of(col, col));
                keyEntry.put(key, new KCVMutation(upserts, Collections.singletonList(inserts.get(0).getColumn())));

                // Pass back a modified copy of the list.
                List<Entry> expectedVals = new LinkedList<>(inserts);
                expectedVals.set(1, upserts.get(0));
                expectedVals.add(upserts.get(1));
                expectedVals.remove(0);
                expectedKeyEntry.put(key, expectedVals);
            }
        }
        manager.mutateMany(mutations, manager.beginTransaction(getTxConfig()));

        for (Map.Entry<String, Map<StaticBuffer, List<Entry>>> expectedEntry : expected.entrySet()) {
            for (Map.Entry<StaticBuffer, List<Entry>> expectedKeyEntry : expectedEntry.getValue().entrySet()) {
                KeyColumnValueStore store = manager.openDatabase(expectedEntry.getKey());
                checkColumnsForOneKey(store, expectedKeyEntry.getKey(), expectedKeyEntry.getValue());
            }
        }
    }

    @Test
    public void manyStoresManyKeysInsertOnly() throws BackendException {
        Map<String, Map<StaticBuffer, KCVMutation>> mutations = new HashMap<>();
        Map<String, Map<StaticBuffer, List<Entry>>> expected = new HashMap<>();
        for (int i = 0; i < 7; i++) {
            String storeName = "msmkio" + i;
            Map<StaticBuffer, KCVMutation> keyEntry = mutations.computeIfAbsent(storeName, sn -> new HashMap<>());
            Map<StaticBuffer, List<Entry>> expectedEntry = expected.computeIfAbsent(storeName, sn -> new HashMap<>());
            for (int j = 0; j < 13; j++) {
                StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(j);
                List<Entry> inserts = new ArrayList<>();
                for (int k = 0; k < NUM_COLS; k++) {
                    StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(k);
                    inserts.add(StaticArrayEntry.of(col, col));
                }
                keyEntry.put(key, new KCVMutation(inserts, Collections.emptyList()));
                expectedEntry.put(key, inserts);
            }
        }
        manager.mutateMany(mutations, manager.beginTransaction(getTxConfig()));

        for (Map.Entry<String, Map<StaticBuffer, List<Entry>>> expectedEntry : expected.entrySet()) {
            for (Map.Entry<StaticBuffer, List<Entry>> expectedKeyEntry : expectedEntry.getValue().entrySet()) {
                KeyColumnValueStore store = manager.openDatabase(expectedEntry.getKey());
                checkColumnsForOneKey(store, expectedKeyEntry.getKey(), expectedKeyEntry.getValue());
            }
        }

    }

    // Make sure that the single store single key case (which gets redirected to DynamoStore.mutate) works.
    @Test
    public void singleStoreSingleKey() throws BackendException {
        String storeName = "sssk";
        StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);
        List<Entry> inserted = insertColumnsForOneKey(storeName, key);

        // Make sure it's all there
        KeyColumnValueStore store = manager.openDatabase(storeName);
        checkColumnsForOneKey(store, key, inserted);
    }

    @Test
    public void singleStoreSingleKeyWithDelete() throws BackendException {
        String storeName = "ssskwd";
        StaticBuffer key = KeyColumnValueStoreUtil.longToByteBuffer(0);
        List<Entry> inserted = insertColumnsForOneKey(storeName, key);

        List<Entry> updated = modColumnsForOneKey(storeName, key, inserted);

        KeyColumnValueStore store = manager.openDatabase(storeName);
        checkColumnsForOneKey(store, key, updated);
    }

    private List<Entry> insertColumnsForOneKey(String storeName, StaticBuffer key) throws BackendException {
        List<Entry> entries = new LinkedList<>();
        for (int i = 0; i < NUM_COLS; i++) {
            StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(i);
            entries.add(StaticArrayEntry.of(col, col));
        }
        Map<String, Map<StaticBuffer, KCVMutation>> mutations = Collections.singletonMap(storeName,
            Collections.singletonMap(key, new KCVMutation(entries, Collections.emptyList())));
        manager.mutateMany(mutations, manager.beginTransaction(getTxConfig()));
        return entries;
    }

    private void checkColumnsForOneKey(KeyColumnValueStore store, StaticBuffer key, List<Entry> expected) throws BackendException {
        List<Entry> readBack = store.getSlice(new KeySliceQuery(key, KeyColumnValueStoreUtil.longToByteBuffer(0),
            KeyColumnValueStoreUtil.longToByteBuffer(100)), manager.beginTransaction(getTxConfig()));
        Assert.assertEquals(NUM_COLS, readBack.size());
        for (int i = 0; i < NUM_COLS; i++) {
            Assert.assertEquals(expected.get(i), readBack.get(i));
        }
    }

    private List<Entry> modColumnsForOneKey(String storeName, StaticBuffer key, List<Entry> cols) throws BackendException {
        List<Entry> upserts = new ArrayList<>();
        // change the value of the 2nd element
        upserts.add(StaticArrayEntry.of(KeyColumnValueStoreUtil.longToByteBuffer(1), KeyColumnValueStoreUtil.longToByteBuffer(100)));
        // add a 10th element
        StaticBuffer col = KeyColumnValueStoreUtil.longToByteBuffer(NUM_COLS);
        upserts.add(StaticArrayEntry.of(col, col));
        Map<String, Map<StaticBuffer, KCVMutation>> mutations = Collections.singletonMap(storeName,
            Collections.singletonMap(key,
                new KCVMutation(upserts, Collections.singletonList(cols.get(0).getColumn()))));
        manager.mutateMany(mutations, manager.beginTransaction(getTxConfig()));

        // Pass back a modified copy of the list.
        List<Entry> toReturn = new LinkedList<>(cols);
        toReturn.set(1, upserts.get(0));
        toReturn.add(upserts.get(1));
        toReturn.remove(0);
        return toReturn;
    }

}

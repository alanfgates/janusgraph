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
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.janusgraph.diskstorage.dynamo.ConfigConstants.DYNAMO_TABLE_NAME;

public class DynamoTableTest {

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

    @Test
    public void existenceCreationAndDrop() throws BackendException {
        ModifiableConfiguration conf = TestUtils.getConfig();
        conf.set(DYNAMO_TABLE_NAME, "dtt");
        DynamoStoreManager mgr = new DynamoStoreManager(conf);
        Assert.assertFalse(mgr.tableExists());
        DynamoStoreManager.DynamoTable table = mgr.getTable();
        Assert.assertTrue(mgr.tableExists());
        mgr.clearStorage();
        Assert.assertFalse(mgr.tableExists());
    }
}

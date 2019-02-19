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

import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class DynamoStoreTest extends KeyColumnValueStoreTest {

    @BeforeClass
    public static void startDynamo() throws IOException, InterruptedException {
        TestUtils.startDockerDynamo();
    }

    @AfterClass
    public static void stopDynamo() throws IOException, InterruptedException {
        TestUtils.shutdownDockerDynamo();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() {
        return new DynamoStoreManager(TestUtils.getConfig());
    }
}

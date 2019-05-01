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

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

public class ConfigConstants {
    // TODO - no idea if these are reasonable values or not
    private static final int DEFAULT_MAX_CONNECTIONS = 10;
    private static final long DEFAULT_READ_THROUGHPUT = 10L;
    private static final long DEFAULT_WRITE_THROUGHPUT = 10L;
    private static final String DEFAULT_DYNAMO_TABLE = DynamoStoreManager.NAME;

    public static final ConfigNamespace DYNAMO_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "dynamo",
        "DynamoDB backend storage configuration options");
    public static final ConfigOption<Integer> DYNAMO_MAX_CONNECTIONS = new ConfigOption<>(DYNAMO_NS,
        "maxconnections", "Maximum concurrent connections to DynamoDB", ConfigOption.Type.LOCAL,
        DEFAULT_MAX_CONNECTIONS);
    public static final ConfigOption<String> DYNAMO_REGION = new ConfigOption<>(DYNAMO_NS,
        "region", "AWS region to access", ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> DYNAMO_URL = new ConfigOption<>(DYNAMO_NS,
        "url", "URL to connect to DynamoDB", ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> DYNAMO_TABLE_NAME = new ConfigOption<>(DYNAMO_NS,
        "tablename", "Name of table to be created in dynamoDB", ConfigOption.Type.FIXED, // TODO not sure if this should be FIXED or LOCAL
        DEFAULT_DYNAMO_TABLE);
    public static final ConfigOption<Long> DYNAMO_READ_THROUGHPUT = new ConfigOption<>(DYNAMO_NS,
        "readthroughput", "Dynamo read capacity throughput", ConfigOption.Type.LOCAL, DEFAULT_READ_THROUGHPUT);
    public static final ConfigOption<Long> DYNAMO_WRITE_THROUGHPUT = new ConfigOption<>(DYNAMO_NS,
        "writethroughput", "Dynamo write capacity throughput", ConfigOption.Type.LOCAL, DEFAULT_WRITE_THROUGHPUT);

    static final String PARTITION_KEY = "storename"; // Column name of partition key in Dynamo, uses the JG key
    static final String SORT_KEY = "januskey"; // Column name of sort key in Dynamo, uses the JG column name
}

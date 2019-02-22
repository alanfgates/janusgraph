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

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

class Utils {
    private static final String DEFAULT_DYNAMO_TABLE_BASE = DynamoStoreManager.NAME + "-";
    // TODO - no idea if these are reasonable values or not
    private static final long DEFAULT_READ_THROUGHPUT = 10L;
    private static final long DEFAULT_WRITE_THROUGHPUT = 10L;
    private static final int DEFAULT_MAX_CONNECTIONS = 10;

    public static final ConfigNamespace DYNAMO_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "dynamo",
        "DynamoDB backend storage configuration options");
    public static final ConfigOption<Long> DYNAMO_WRITE_THROUGHPUT = new ConfigOption<>(DYNAMO_NS,
        "dynamo-write-throughput", "Dynamo write capacity throughput", ConfigOption.Type.LOCAL, DEFAULT_WRITE_THROUGHPUT);
    public static final ConfigOption<Long> DYNAMO_READ_THROUGHPUT = new ConfigOption<>(DYNAMO_NS,
        "dynamo-read-throughput", "Dynamo read capacity throughput", ConfigOption.Type.LOCAL, DEFAULT_READ_THROUGHPUT);
    public static final ConfigOption<String> DYNAMO_TABLE_BASE = new ConfigOption<>(DYNAMO_NS,
        "dynamo-table-base", "Basename for tables to be created in dynamoDB", ConfigOption.Type.FIXED, // TODO not sure if this should be FIXED or LOCAL
        DEFAULT_DYNAMO_TABLE_BASE);
    public static final ConfigOption<String> DYNAMO_URL = new ConfigOption<>(DYNAMO_NS,
        "dynamo-url", "URL to connect to DynamoDB", ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> DYNAMO_REGION = new ConfigOption<>(DYNAMO_NS,
        "dynamo-region", "AWS region to access", ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<Integer> DYNAMO_MAX_CONNECTIONS = new ConfigOption<>(DYNAMO_NS,
        "dynamo-max-connections", "Maximum concurrent connections to DynamoDB", ConfigOption.Type.LOCAL,
        DEFAULT_MAX_CONNECTIONS);


    /**
     * Produces a byte array double the length of the array from the StaticBuffer.  Each byte from the
     * static buffer is stored in 2 bytes, the high in the left and the low in the right.
     */
    static StaticBuffer.Factory<byte[]> unsignedBytesFactory = Utils::bytesToUnsignedBytes;

    static byte[] bytesToUnsignedBytes(byte[] signed, int offset, int limit) {
        assert limit > offset;
        byte[] unsigned = new byte[(limit - offset) * 2];
        for (int i = offset; i < limit; i++) {
            unsigned[(i - offset) * 2] = (signed[i] & 0x80) > 0 ? (byte)1 : (byte)0;
            unsigned[(i - offset) * 2 + 1] = (byte)(signed[i] & (byte)0x7f);
        }
        /*
        byte[] unsigned = new byte[limit - offset];
        for (int i = offset; i < limit; i++) {
            unsigned[i - offset] = (byte)((signed[i] & 0x80) == 0x80 ? signed[i] & 0x7f : signed[i] - (byte)128);
        }
        */
        return unsigned;
    }

    /**
     * Produces a "normal" byte array.  This pushes the unsigned byte array back into half the length.
     * @param unsigned unsigned byte array as stored in Dynamo.
     * @return regular byte array used by JG
     */
    static byte[] unsignedBytesToBytes(byte[] unsigned) {
        assert unsigned.length % 2 == 0;

        byte[] signed = new byte[unsigned.length / 2];
        for (int i = 0; i < signed.length; i++) {
            signed[i] = (byte)((unsigned[i * 2] == 1 ? 0x80 : 0) | unsigned[i * 2 + 1]);
        }
        /*
        byte[] signed = new byte[unsigned.length];
        for (int i = 0; i < unsigned.length; i++) {
            signed[i] = (byte)((unsigned[i] & 0x80) == 0 ? unsigned[i] | 0x80 : unsigned[i] + (byte)128);
        }
        */
        return signed;
    }

}

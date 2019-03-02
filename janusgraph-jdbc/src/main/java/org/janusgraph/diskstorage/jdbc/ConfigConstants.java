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

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.time.Duration;

public class ConfigConstants {
    public static final ConfigNamespace JDBC_NS =
        new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "jdbc", "SQL JDBC backend " +
            "storage configuration options");
    public static final ConfigOption<Duration> JDBC_TIMEOUT = new ConfigOption<>(JDBC_NS,
        "jdbc-timeout", "JDBC connection timeout in seconds for backends that use JDBC",
        ConfigOption.Type.LOCAL, Duration.ofSeconds(60L));
    public static final ConfigOption<Integer> JDBC_POOL_SIZE = new ConfigOption<>(JDBC_NS,
        "jdbc-pool-size", "JDBC connection pool size for backends that use JDBC",
        ConfigOption.Type.LOCAL, 10, ConfigOption.positiveInt());
    // TODO - this should not be stored in the open, is there some way to encrypt this or fetch
    // it securely?
    public static final ConfigOption<String> JDBC_PASSWORD = new ConfigOption<>(JDBC_NS,
        "jdbc-password", "Password for JDBC connection for backends that use JDBC",
        ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> JDBC_USER = new ConfigOption<>(JDBC_NS,
        "jdbc-user", "User for JDBC connection for backends that use JDBC",
        ConfigOption.Type.LOCAL, String.class);
    public static final ConfigOption<String> JDBC_URL = new ConfigOption<>(JDBC_NS,
        "jdbc-url", "URL for JDBC connection for backends that use JDBC",
        ConfigOption.Type.LOCAL, String.class);
}

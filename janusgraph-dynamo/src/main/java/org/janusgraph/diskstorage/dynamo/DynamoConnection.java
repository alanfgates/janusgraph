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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

import static org.janusgraph.diskstorage.dynamo.Utils.DYNAMO_REGION;
import static org.janusgraph.diskstorage.dynamo.Utils.DYNAMO_URL;

class DynamoConnection implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoConnection.class);

    private final ConnectionPool pool;
    private final DynamoDB dynamo;

    DynamoConnection(ConnectionPool pool, Configuration conf) {
        this.pool = pool;
        dynamo = connect(conf);
    }

    /**
     * Shutdown this connection.  Once this is called the connection is unusable.
     */
    void destroy() {
        dynamo.shutdown();
    }

    DynamoDB get() {
        return dynamo;
    }

    @Override
    public void close() {
        pool.putBack(this);

    }

    private DynamoDB connect(Configuration conf) {
        Preconditions.checkArgument(conf.has(DYNAMO_URL) && conf.has(DYNAMO_REGION));

        // TODO - following taken from example DynamoDB code, probably way more that needs
        // done here, like user credentials, security tokens, etc.
        LOG.info("Connecting to DynamoDB at endpoint " + conf.get(DYNAMO_URL) + " and region " +
            conf.get(DYNAMO_REGION));
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder
            .standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(conf.get(DYNAMO_URL), conf.get(DYNAMO_REGION))
            )
            .build();
        return new DynamoDB(client);

    }
}

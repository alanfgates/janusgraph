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

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    private static final String IMAGE_NAME = "amazon/dynamodb-local";
    private static final String BASE_CONTAINER_NAME = "janus-dynamo-test-";
    private static final String PORT_MAPPING = "8000:8000";

    private static String containerName = genContainerName();

    static ModifiableConfiguration getConfig() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "dynamo");
        config.set(DynamoStoreManager.DYNAMO_REGION, "us-west-2");
        // This sets it to use a local docker instance of Dynamo rather than Amazon's actual service
        config.set(DynamoStoreManager.DYNAMO_URL, "http://localhost:8000");
        return config;
    }

    static void startDockerDynamo() throws IOException, InterruptedException {
        // Not clear if I need these or not
        System.setProperty("aws.accessKeyId", "abc");
        System.setProperty("aws.secretKey", "123");
        LOG.info("Starting DynamoDB in a container");
        if (runCmdAndPrintStreams(new String[] {"docker", "run", "--name", containerName, "-p", PORT_MAPPING, "-d",
            IMAGE_NAME}, 300) != 0) {
            throw new IOException("Failed to run docker image");
        }
    }

    static void shutdownDockerDynamo() throws IOException, InterruptedException {
        LOG.info("Shutting down DynamoDB in docker container");
        if (runCmdAndPrintStreams(new String[]{"docker", "stop", containerName}, 30) != 0) {
            throw new IOException("Failed to stop docker container");
        }
    }

    private static class ProcessResults {
        final String stdout;
        final String stderr;
        final int rc;

        ProcessResults(String stdout, String stderr, int rc) {
            this.stdout = stdout;
            this.stderr = stderr;
            this.rc = rc;
        }
    }

    private static int runCmdAndPrintStreams(String[] cmd, long secondsToWait) throws InterruptedException, IOException {
        ProcessResults results = runCmd(cmd, secondsToWait);
        if (results.rc != 0) {
            LOG.error("Failed to run command " + StringUtils.join(cmd));
            LOG.error("stdout <" + results.stdout + ">");
            LOG.error("stderr <" + results.stderr + ">");
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Stdout from proc: " + results.stdout);
            LOG.debug("Stderr from proc: " + results.stderr);
        }
        return results.rc;
    }

    private static ProcessResults runCmd(String[] cmd, long secondsToWait) throws IOException, InterruptedException {
        LOG.debug("Going to run: " + StringUtils.join(cmd, " "));
        Process proc = Runtime.getRuntime().exec(cmd);
        if (!proc.waitFor(secondsToWait, TimeUnit.SECONDS)) {
            throw new RuntimeException("Process " + cmd[0] + " failed to run in " + secondsToWait +
                " seconds");
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        final StringBuilder lines = new StringBuilder();
        reader.lines()
            .forEach(s -> lines.append(s).append('\n'));

        reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        final StringBuilder errLines = new StringBuilder();
        reader.lines()
            .forEach(s -> errLines.append(s).append('\n'));
        return new ProcessResults(lines.toString(), errLines.toString(), proc.exitValue());
    }

    private static String genContainerName() {
        return BASE_CONTAINER_NAME + new Random().nextInt(1000000);
    }

}

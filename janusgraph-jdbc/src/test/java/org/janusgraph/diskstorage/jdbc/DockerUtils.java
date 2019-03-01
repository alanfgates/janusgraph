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

import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class DockerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DockerUtils.class);
    private static final String IMAGE_NAME = "postgres:9.3";
    private static final String BASE_CONTAINER_NAME = "janus-postgres-test-";
    private static final String PORT_MAPPING = "5432:5432";
    private static final String PSQL_USER = "dynamotest";
    private static final String PSQL_USER_PWD = "5Up3r53cr3t";

    static ModifiableConfiguration getConfig() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "dynamo");
        config.set(ConfigConstants.JDBC_URL, "jdbc:postgresql://localhost:5432/" + PSQL_USER);
        // This sets it to use a local docker instance of Dynamo rather than Amazon's actual service
        config.set(ConfigConstants.JDBC_USER, PSQL_USER);
        // Set the number of concurrent connections high because the concurrency tests run 64 simultaneous threads
        config.set(ConfigConstants.JDBC_PASSWORD, PSQL_USER_PWD);
        return config;
    }

    static String startDocker() throws IOException, InterruptedException {
        String name = genContainerName();
        LOG.info("Starting Postgres in a container with name " + name);
        if (runCmdAndPrintStreams(new String[] {"docker", "run", "--name", name, "-p", PORT_MAPPING, "-d",
            IMAGE_NAME}, 300) != 0) {
            throw new IOException("Failed to run docker image");
        }
        // Connect to the postgres instance and create a test user
        //PGSimpleDataSource ds = new PGSimpleDataSource

        return name;
    }

    static void shutdownDocker(String name) throws IOException, InterruptedException {
        if (name == null) return;
        LOG.info("Shutting down Postgres in docker container");
        if (runCmdAndPrintStreams(new String[]{"docker", "stop", name}, 30) != 0) {
            throw new IOException("Failed to stop docker container");
        }
        runCmdAndPrintStreams(new String[] {"docker", "rm", name}, 30);
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
        return BASE_CONTAINER_NAME + Math.abs(new Random().nextInt());
    }
}

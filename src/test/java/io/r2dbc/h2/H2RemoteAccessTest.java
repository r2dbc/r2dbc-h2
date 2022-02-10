/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2;

import org.h2.tools.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.FileSystemUtils;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

final class H2RemoteAccessTest {

    Server server;
    Path basePath;

    @BeforeEach
    void startupRemoteDatabase() throws SQLException, IOException {
        server = Server.createTcpServer("-tcpPort", "9123", "-ifNotExists").start();
        basePath = Files.createTempDirectory("h2-test-");
    }

    @AfterEach
    void shutdownRemoteDatabase() {
        server.stop();
    }

    @Test
    void tcpByUrlWorks() throws IOException {
        FileSystemUtils.deleteRecursively(basePath);

        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .url("tcp://localhost:9123/" + basePath)
            .username("sa")
            .password("")
            .build();

        new H2ConnectionFactory(configuration).create()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        FileSystemUtils.deleteRecursively(basePath);
    }

    @Test
    void tcpWithHostnameAndPortWorks() throws IOException {
        FileSystemUtils.deleteRecursively(basePath);

        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .tcp("localhost", 9123, basePath.toString())
            .username("sa")
            .password("")
            .build();

        new H2ConnectionFactory(configuration).create()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        FileSystemUtils.deleteRecursively(basePath);
    }

    @Test
    void tcpWithHostnameButNoPortWorks() throws IOException, SQLException {

        // Stopping the default one...
        server.stop();

        // ...to launch another one with the default port
        server = Server.createTcpServer("-ifNotExists").start();

        FileSystemUtils.deleteRecursively(basePath);

        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .tcp("localhost", basePath.toString())
            .username("sa")
            .password("")
            .build();

        new H2ConnectionFactory(configuration).create()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        FileSystemUtils.deleteRecursively(basePath);
    }
}

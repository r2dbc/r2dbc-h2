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

import io.r2dbc.h2.codecs.DefaultCodecs;
import io.r2dbc.h2.util.IntegrationTestSupport;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link DefaultCodecs} testing all known codecs with pre-defined values and {@code null} values.
 */
class H2LobIntegrationTest extends IntegrationTestSupport {

    static byte[] ALL_BYTES = new byte[-(-128) + 127];

    static {
        Hooks.onOperatorDebug();

        for (int i = -128; i < 127; i++) {
            ALL_BYTES[-(-128) + i] = (byte) i;
        }
    }

    @Test
    void testNullBlob() {
        createTable(connection, "IMAGE");

        Flux.from(connection.createStatement("INSERT INTO lob_test values($1)")
            .bindNull("$1", Blob.class)
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get("my_col", Blob.class))))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo(Optional.empty()))
            .verifyComplete();
    }

    @Test
    void testSmallBlob() {
        createTable(connection, "IMAGE");

        Flux.from(connection.createStatement("INSERT INTO lob_test values($1)")
            .bind("$1", Blob.from(Mono.just("foo".getBytes()).map(ByteBuffer::wrap)))
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
            .flatMap(Blob::stream)
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo(ByteBuffer.wrap("foo".getBytes())))
            .verifyComplete();
    }

    @Test
    void testBigBlob() {
        createTable(connection, "LONGBLOB");

        int i = 50 + new Random().nextInt(1000);

        Flux.from(connection.createStatement("INSERT INTO lob_test values($1)")
            .bind("$1", Blob.from(Flux.range(0, i).map(it -> ByteBuffer.wrap(ALL_BYTES))))
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Blob.class)))
            .flatMap(Blob::stream)
            .map(Buffer::remaining)
            .collect(Collectors.summingInt(value -> value))
            .as(StepVerifier::create)
            .expectNext(i * ALL_BYTES.length)
            .verifyComplete();
    }

    @Test
    void testNullClob() {
        createTable(connection, "NTEXT");

        Flux.from(connection.createStatement("INSERT INTO lob_test values($1)")
            .bindNull("$1", Clob.class)
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get("my_col", Clob.class))))
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo(Optional.empty()))
            .verifyComplete();
    }

    @Test
    void testSmallClob() {
        createTable(connection, "NTEXT");

        Flux.from(connection.createStatement("INSERT INTO lob_test values($1)")
            .bind("$1", Clob.from(Mono.just("foo你好")))
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
            .flatMap(Clob::stream)
            .as(StepVerifier::create)
            .consumeNextWith(actual -> assertThat(actual).isEqualTo("foo你好"))
            .verifyComplete();
    }

    @Test
    void testBigClob() {
        createTable(connection, "LONGTEXT");

        int i = 50 + new Random().nextInt(1000);

        String TEST_STRING = "foo你好bar";

        Flux.from(connection.createStatement("INSERT INTO lob_test values($1)")
            .bind("$1", Clob.from(Flux.range(0, i).map(it -> TEST_STRING)))
            .execute())
            .flatMap(Result::getRowsUpdated)
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();

        connection.createStatement("SELECT my_col FROM lob_test")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get("my_col", Clob.class)))
            .flatMap(Clob::stream)
            .map(CharSequence::length)
            .collect(Collectors.summingInt(value -> value))
            .as(StepVerifier::create)
            .expectNext(i * TEST_STRING.length())
            .verifyComplete();
    }

    private void createTable(H2Connection connection, String columnType) {
        connection.createStatement("DROP TABLE lob_test").execute()
            .flatMap(H2Result::getRowsUpdated)
            .onErrorResume(e -> Mono.empty())
            .thenMany(connection
                .createStatement("CREATE TABLE lob_test (my_col " + columnType + ")")
                .execute()
                .flatMap(H2Result::getRowsUpdated))
            .as(StepVerifier::create)
            .expectNext(0)
            .verifyComplete();
    }
}

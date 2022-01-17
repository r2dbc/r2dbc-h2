/*
 * Copyright 2018 the original author or authors.
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

import io.r2dbc.h2.client.Binding;
import io.r2dbc.h2.client.Client;
import io.r2dbc.h2.codecs.MockCodecs;
import io.r2dbc.h2.util.H2ServerExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcBadGrammarException;
import org.h2.command.CommandInterface;
import org.h2.result.LocalResult;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

import java.util.Arrays;
import java.util.Collections;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.H2_DRIVER;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.*;
import static reactor.function.TupleUtils.predicate;

final class H2StatementTest {

    private final Client client = mock(Client.class, RETURNS_SMART_NULLS);

    private final Value parameter = ValueInteger.get(100);

    private final MockCodecs codecs = MockCodecs.builder().encoding(100, this.parameter).build();

    private final H2Statement statement = new H2Statement(this.client, this.codecs, "test-query-$1");

    @Test
    void shouldNotAcceptQuestionMarkAlone() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind("?", 100).getCurrentBinding())
            .withMessage("Identifier '?' is not a valid identifier. Should be of the pattern '.*([$?])([\\d]+).*'.");
    }

    @Test
    void shouldNotAcceptDollarAlone() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind("$", 100).getCurrentBinding())
            .withMessage("Identifier '$' is not a valid identifier. Should be of the pattern '.*([$?])([\\d]+).*'.");
    }

    @Test
    void bind() {
        assertThat(this.statement.bind("$1", 100).getCurrentBinding()).isEqualTo(new Binding().add(0, ValueInteger.get(100)));
    }

    @Test
    void bindWithPositionNumberAsObject() {
        assertThat(this.statement.bind(0, 100).getCurrentBinding())
            .isEqualTo(new Binding().add(0, ValueInteger.get(100)));
    }

    @Test
    void bindWithQuestionMark() {
        H2Statement questionMarkStatement = new H2Statement(this.client, this.codecs, "test-query-?1");

        assertThat(questionMarkStatement.bind("?1", 100).getCurrentBinding())
            .isEqualTo(new Binding().add(0, ValueInteger.get(100)));
    }

    @Test
    void bindIndex() {
        assertThat(this.statement.bind(0, 100).getCurrentBinding()).isEqualTo(new Binding().add(0, ValueInteger.get(100)));
    }

    @Test
    void bindIndexNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind(1, null))
            .withMessage("value must not be null");
    }

    @Test
    void bindNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind(null, ""))
            .withMessage("name must not be null");
    }

    @Test
    void bindNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind("$1", null))
            .withMessage("value must not be null");
    }

    @Test
    void bindNull() {
        MockCodecs codecs = MockCodecs.builder()
            .encoding(Integer.class, ValueNull.INSTANCE)
            .build();

        H2Statement statement = new H2Statement(this.client, codecs, "test-query-$1");

        assertThat(statement.bindNull("$1", Integer.class).getCurrentBinding())
            .isEqualTo(new Binding().add(0, ValueNull.INSTANCE));
    }

    @Test
    void bindNullNoIdentifier() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull(null, Integer.class))
            .withMessage("name must not be null");
    }

    @Test
    void bindNullWrongIdentifierFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull("foo", Integer.class))
            .withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '.*([$?])([\\d]+).*'.");
    }

    @Test
    void bindWrongIdentifierFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind("foo", ""))
            .withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '.*([$?])([\\d]+).*'.");
    }

    @Test
    void constructorNoClient() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2Statement(null, this.codecs, "test-query"))
            .withMessage("client must not be null");
    }

    @Test
    void constructorNoSql() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2Statement(this.client, this.codecs, null))
            .withMessage("sql must not be null");
    }

    @Test
    void execute() {
        MockCodecs codecs = getMockCodecs();

        new H2Statement(this.client, codecs, "select test-query-$1 from my_table")
            .add()
            .bind("$1", 100)
            .add()
            .bind("$1", 200)
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();
    }
    @Test
    void executeIndexed() {
        MockCodecs codecs = getMockCodecs();

        new H2Statement(this.client, codecs, "select test-query-$1 from my_table")
                .add()
                .bind(0, 100)
                .add()
                .bind(0, 200)
                .execute()
                .as(StepVerifier::create)
                .expectNextCount(2)
                .verifyComplete();
    }

    private MockCodecs getMockCodecs() {
        CommandInterface command1 = mock(CommandInterface.class);
        CommandInterface command2 = mock(CommandInterface.class);
        when(this.client.prepareCommand("select test-query-$1 from my_table", Arrays.asList(
            new Binding().add(0, ValueInteger.get(100)),
            new Binding().add(0, ValueInteger.get(200))
        ))).thenReturn(Arrays.asList(command1, command2).iterator());
        when(command1.isQuery()).thenReturn(true);
        when(command2.isQuery()).thenReturn(true);
        when(this.client.query(command1)).thenReturn(new LocalResult());
        when(this.client.query(command2)).thenReturn(new LocalResult());

        return MockCodecs.builder()
            .encoding(100, ValueInteger.get(100))
            .encoding(200, ValueInteger.get(200))
            .build();
    }

    @Test
    void executeWithoutAdd() {
        CommandInterface command = mock(CommandInterface.class);
        when(this.client.prepareCommand("insert test-query-$1", Arrays.asList(
            new Binding().add(0, ValueInteger.get(100))
        ))).thenReturn(Collections.singleton(command).iterator());
        when(this.client.update(command, false)).thenReturn(new ResultWithGeneratedKeys.WithKeys(0, new LocalResult()));

        new H2Statement(this.client, this.codecs, "insert test-query-$1")
            .bind("$1", 100)
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void returnGeneratedValues() {
        CommandInterface command = mock(CommandInterface.class);
        when(this.client.prepareCommand("INSERT test-query", Collections.emptyList())).thenReturn(Collections.singleton(command).iterator());
        when(this.client.update(command, new String[]{"foo", "bar"})).thenReturn(new ResultWithGeneratedKeys.WithKeys(0, new LocalResult()));

        new H2Statement(this.client, MockCodecs.empty(), "INSERT test-query")
            .returnGeneratedValues("foo", "bar")
            .execute()
            .flatMap(result -> result.map((row, rowMetadata) -> row))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    void returnGenerateValuesNoArguments() throws Exception {
        H2ServerExtension SERVER = new H2ServerExtension();

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, H2_DRIVER)
            .option(PASSWORD, SERVER.getPassword())
            .option(URL, SERVER.getUrl())
            .option(USER, SERVER.getUsername())
            .build());

        SERVER.beforeAll(null);

        SERVER.getJdbcOperations().execute("CREATE TABLE test ( id INTEGER AUTO_INCREMENT, id2 INTEGER AUTO_INCREMENT, test_value INTEGER);");

        Mono.from(connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection

                    .createStatement(String.format("INSERT INTO test (test_value) VALUES (200)"))
                    .returnGeneratedValues()
                    .execute())

                .concatWith(Mono.from(connection.close()).then(Mono.empty())))
            .flatMap(result -> ((H2Result) result).map(Tuples::of))
            .as(StepVerifier::create)
            .expectNextMatches(predicate((row, rowMetadata) -> {
                assertThat(row.get("ID")).isEqualTo(1);
                assertThat(row.get("ID2")).isEqualTo(1);
                assertThat(rowMetadata.getColumnMetadatas()).hasSize(2);
                return true;
            }))
            .verifyComplete();

        SERVER.getJdbcOperations().execute("DROP TABLE test");

        SERVER.afterAll(null);
    }

    @Test
    void returnGeneratedValuesNotUsed() throws Exception {
        H2ServerExtension SERVER = new H2ServerExtension();

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, H2_DRIVER)
            .option(PASSWORD, SERVER.getPassword())
            .option(URL, SERVER.getUrl())
            .option(USER, SERVER.getUsername())
            .build());

        SERVER.beforeAll(null);

        SERVER.getJdbcOperations().execute("CREATE TABLE test ( id INTEGER AUTO_INCREMENT, id2 INTEGER AUTO_INCREMENT, test_value INTEGER);");

        Mono.from(connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection

                    .createStatement(String.format("INSERT INTO test (test_value) VALUES (200)"))
                    .execute())

                .concatWith(Mono.from(connection.close()).then(Mono.empty())))
            .flatMap(result -> ((H2Result) result).map(Tuples::of))
            .as(StepVerifier::create)
            .expectNextCount(0)
            .verifyComplete();

        SERVER.getJdbcOperations().execute("DROP TABLE test");

        SERVER.afterAll(null);
    }

    @Test
    void returnGeneratedValuesSpecificColumn() throws Exception {
        H2ServerExtension SERVER = new H2ServerExtension();

        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, H2_DRIVER)
            .option(PASSWORD, SERVER.getPassword())
            .option(URL, SERVER.getUrl())
            .option(USER, SERVER.getUsername())
            .build());

        SERVER.beforeAll(null);

        SERVER.getJdbcOperations().execute("CREATE TABLE test ( id INTEGER AUTO_INCREMENT, id2 INTEGER AUTO_INCREMENT, test_value INTEGER);");

        Mono.from(connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection

                    .createStatement(String.format("INSERT INTO test (test_value) VALUES (200)"))
                    .returnGeneratedValues("id2")
                    .execute())

                .concatWith(Mono.from(connection.close()).then(Mono.empty())))
            .flatMap(result -> ((H2Result) result).map(Tuples::of))
            .as(StepVerifier::create)
            .expectNextMatches(predicate((row, rowMetadata) -> {
                assertThat(row.get("ID2")).isEqualTo(1);
                assertThat(rowMetadata.getColumnMetadatas()).hasSize(1);
                return true;
            }))
            .verifyComplete();

        SERVER.getJdbcOperations().execute("DROP TABLE test");

        SERVER.afterAll(null);
    }

    @Test
    void executeErrorResponse() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(H2ConnectionConfiguration
            .builder() //
            .inMemory("r2dbc") //
            .username("sa") //
            .password("") //
            .option("DB_CLOSE_DELAY=-1").build());

        Flux.from(connectionFactory.create())
            .flatMap(conn -> conn.createStatement("SELECT foobar FROM doesnt_exist").execute())
            .as(StepVerifier::create)
            .expectErrorSatisfies(throwable -> {
                assertThat(throwable).isInstanceOf(R2dbcBadGrammarException.class);
            })
            .verify();
    }
}

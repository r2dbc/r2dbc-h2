package io.r2dbc.h2;

import io.r2dbc.h2.util.H2ServerExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.test.TestKit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

@ExtendWith(H2ServerExtension.class)
final class H2PostgresqlTestKit implements TestKit<String> {

    private final ConnectionFactory connectionFactory;

    private final JdbcOperations jdbcOperations;

    public H2PostgresqlTestKit(ConnectionFactory connectionFactory, JdbcOperations jdbcOperations) {

        this.connectionFactory = connectionFactory;
        this.jdbcOperations = jdbcOperations;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public String getIdentifier(int index) {
        return getPlaceholder(index);
    }

    @Override
    public JdbcOperations getJdbcOperations() {
        return this.jdbcOperations;
    }

    @Override
    public String getPlaceholder(int index) {
        return String.format("$%d", index + 1);
    }

    @Test
    @Override
    public void columnMetadata() {
        getJdbcOperations().execute("INSERT INTO test_two_column VALUES (100, 'hello')");

        Mono.from(getConnectionFactory().create())
            .flatMapMany(connection -> Flux.from(connection

                    .createStatement("SELECT col1 AS test_value, col2 AS test_value FROM test_two_column")
                    .execute())
                .flatMap(result -> {
                    return result.map((row, rowMetadata) -> {
                        Collection<String> columnNames = ((H2RowMetadata) rowMetadata).getColumnNames();
                        return Arrays.asList(rowMetadata.getColumnMetadata("test_value").getName(), rowMetadata.getColumnMetadata("TEST_VALUE").getName(), columnNames.contains("test_value"), columnNames.contains(
                            "TEST_VALUE"));
                    });
                })
                .flatMapIterable(Function.identity())
                .concatWith(close(connection)))
            .as(StepVerifier::create)
            .expectNext("TEST_VALUE").as("Column label col1")
            .expectNext("TEST_VALUE").as("Column label col1 (get by uppercase)")
            .expectNext(true).as("getColumnNames.contains(test_value)")
            .expectNext(true).as("getColumnNames.contains(TEST_VALUE)")
            .verifyComplete();
    }

    @Override
    public void bindFails() {
        // TODO: Figure out how to perform bind validations that are normally done during execution phase.
    }

    @Override
    public void prepareStatementWithIncompleteBatchFails() {
        // TODO: Figure out how to perform bind validations that are normally done during execution phase.
    }

    @Override
    public void prepareStatementWithIncompleteBindingFails() {
        // TODO: Figure out how to perform bind validations that are normally done during execution phase.
    }

    @Override
    public void returnGeneratedValues() {
        // TODO: Figure out how to insert a column and get the row back instead of rows updated.
    }

    @Test
    @Override
    public void rowMetadata() {
        getJdbcOperations().execute(expand(TestStatement.INSERT_TWO_COLUMNS));

        Mono.from(getConnectionFactory().create())
            .flatMapMany(connection -> Flux.from(connection

                    .createStatement(expand(TestStatement.SELECT_VALUE_ALIASED_COLUMNS))
                    .execute())
                .flatMap(result -> result.map((row, rowMetadata) -> new ArrayList<>(((H2RowMetadata) rowMetadata).getColumnNames())))
                .flatMapIterable(Function.identity())
                .concatWith(close(connection)))
            .as(StepVerifier::create)
            .expectNext("B").as("First column label: B")
            .expectNext("C").as("First column label: C")
            .expectNext("A").as("First column label: A")
            .verifyComplete();
    }

    <T> Mono<T> close(Connection connection) {
        return Mono.from(connection
                .close())
            .then(Mono.empty());
    }
}

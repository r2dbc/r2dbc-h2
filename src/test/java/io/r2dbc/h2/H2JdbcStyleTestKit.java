package io.r2dbc.h2;

import io.r2dbc.h2.util.H2ServerExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.test.TestKit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.function.Function;

@Disabled("TODO: Fix H2Statement so it properly handles plain JDBC placeholders.")
@ExtendWith(H2ServerExtension.class)
final class H2JdbcStyleTestKit implements TestKit<Integer> {

    private final ConnectionFactory connectionFactory;

    private final JdbcOperations jdbcOperations;

    public H2JdbcStyleTestKit(ConnectionFactory connectionFactory, JdbcOperations jdbcOperations) {

        this.connectionFactory = connectionFactory;
        this.jdbcOperations = jdbcOperations;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Override
    public Integer getIdentifier(int index) {
        return index;
    }

    @Override
    public JdbcOperations getJdbcOperations() {
        return jdbcOperations;
    }

    @Override
    public String getPlaceholder(int index) {
        return "?";
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

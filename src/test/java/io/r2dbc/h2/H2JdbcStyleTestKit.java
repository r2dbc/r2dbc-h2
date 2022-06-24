package io.r2dbc.h2;

import io.r2dbc.h2.util.H2ServerExtension;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.test.TestKit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.function.Function;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.H2_DRIVER;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Disabled("TODO: Fix H2Statement so it properly handles plain JDBC placeholders.")
final class H2JdbcStyleTestKit implements TestKit<Integer> {

    @RegisterExtension
    static final H2ServerExtension SERVER = new H2ServerExtension();

    private final ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, H2_DRIVER)
        .option(PASSWORD, SERVER.getPassword())
        .option(URL, SERVER.getUrl())
        .option(USER, SERVER.getUsername())
        .build());

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
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        if (jdbcOperations == null) {
            throw new IllegalStateException("JdbcOperations not yet initialized");
        }

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

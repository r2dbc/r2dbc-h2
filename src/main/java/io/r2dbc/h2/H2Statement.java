/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import io.r2dbc.h2.codecs.Codecs;
import io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Statement;
import org.h2.engine.GeneratedKeysMode;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.r2dbc.h2.client.Client.SELECT;

/**
 * An implementation of {@link Statement} for an H2 database.
 */
public final class H2Statement implements Statement {

    // search for $ or ? in the statement.
    private static final Pattern PARAMETER_SYMBOLS = Pattern.compile(".*([$?])([\\d]+).*");

    // the value of the binding will be on the second group
    private static final int BIND_POSITION_NUMBER_GROUP = 2;

    private final Bindings bindings = new Bindings();

    private final Client client;

    private final Codecs codecs;

    private final String sql;

    private String[] generatedColumns;

    private boolean allGeneratedColumns = false;


    H2Statement(Client client, Codecs codecs, String sql) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        this.codecs = Assert.requireNonNull(codecs, "codecs must not be null");
        this.sql = Assert.requireNonNull(sql, "sql must not be null");
    }

    @Override
    public H2Statement add() {
        this.bindings.finish();
        return this;
    }

    @Override
    public H2Statement bind(Object identifier, Object value) {
        Assert.requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof String) {
            int indexPosition = getIndex((String) identifier);
            return addIndex(indexPosition, value);
        }

        if (identifier instanceof Integer) {
            return addIndex((int) identifier, value);
        }

        throw new IllegalArgumentException("identifier must be: String or Integer");
    }

    @Override
    public H2Statement bind(int index, Object value) {
        return addIndex(index, value);
    }

    @Override
    public H2Statement bindNull(Object identifier, @Nullable Class<?> type) {
        Assert.requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof String) {
            int index = getIndex((String) identifier);
            bindNull(index, type);
            return this;
        }

        if (identifier instanceof Integer) {
            bindNull((int) identifier, type);
            return this;
        }

        throw new IllegalArgumentException("identifier must be: String or Integer");
    }

    @Override
    public H2Statement bindNull(int index, @Nullable Class<?> type) {
        this.bindings.getCurrent().add(index, this.codecs.encodeNull(type));

        return this;
    }

    @Override
    public Flux<H2Result> execute() {
        return Flux.fromArray(this.sql.split(";"))
            .map(String::trim)
            .flatMap(sql -> {
                if (this.generatedColumns == null) {
                    return execute(this.client, sql, this.bindings, this.codecs, this.allGeneratedColumns);
                }
                return execute(this.client, sql, this.bindings, this.codecs, this.generatedColumns);
            });
    }

    @Override
    public H2Statement returnGeneratedValues(String... columns) {
        Assert.requireNonNull(columns, "columns must not be null");

        if (columns.length == 0) {
            this.allGeneratedColumns = true;
        } else {
            this.generatedColumns = columns;
        }
        
        return this;
    }

    Binding getCurrentBinding() {
        return this.bindings.getCurrent();
    }

    private H2Statement addIndex(int index, Object value) {
        Assert.requireNonNull(value, "value must not be null");

        this.bindings.getCurrent().add(index, this.codecs.encode(value));

        return this;
    }

    private static Flux<H2Result> execute(Client client, String sql, Bindings bindings, Codecs codecs, Object generatedColumns) {
        if (!SELECT.matcher(sql).matches()) {
            return client.update(sql, bindings.bindings, generatedColumns)
                .map(result -> {
                    if (GeneratedKeysMode.valueOf(generatedColumns) == GeneratedKeysMode.NONE) {
                        return H2Result.toResult(codecs, result.getUpdateCount());
                    } else {
                        return H2Result.toResult(codecs, result.getGeneratedKeys(), result.getUpdateCount());
                    }
                });
        } else {
            return client.query(sql, bindings.bindings)
                .map(result -> H2Result.toResult(codecs, result, null));
        }
    }

    private int getIndex(String identifier) {
        Matcher matcher = PARAMETER_SYMBOLS.matcher(identifier);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format("Identifier '%s' is not a valid identifier. Should be of the pattern '%s'.", identifier, PARAMETER_SYMBOLS.pattern()));
        }

        return Integer.parseInt(matcher.group(BIND_POSITION_NUMBER_GROUP)) - 1;
    }

    private static final class Bindings {

        private final List<Binding> bindings = new ArrayList<>();

        private Binding current;

        @Override
        public String toString() {
            return "Bindings{" +
                "bindings=" + bindings +
                ", current=" + current +
                '}';
        }

        private void finish() {
            this.current = null;
        }

        private Binding getCurrent() {
            if (this.current == null) {
                this.current = new Binding();
                this.bindings.add(this.current);
            }

            return this.current;
        }
    }
}

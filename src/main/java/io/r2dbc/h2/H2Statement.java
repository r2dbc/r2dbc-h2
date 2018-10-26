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
import io.r2dbc.h2.util.ObjectUtils;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.r2dbc.h2.client.Client.SELECT;

/**
 * An implementation of {@link Statement} for an H2 database.
 */
public final class H2Statement implements Statement<H2Statement> {

    private static final Pattern PARAMETER_SYMBOL = Pattern.compile(".*\\$([\\d]+).*");

    private final Bindings bindings = new Bindings();

    private final Client client;

    private final Codecs codecs;

    private final String sql;


    H2Statement(Client client, Codecs codecs, String sql) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.codecs = Objects.requireNonNull(codecs, "codecs must not be null");
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
    }

    @Override
    public H2Statement add() {
        this.bindings.finish();
        return this;
    }

    @Override
    public H2Statement bind(Object identifier, Object value) {
        Objects.requireNonNull(identifier, "identifier must not be null");
        ObjectUtils.requireType(identifier, String.class, "identifier must be a String");

        return bind(getIndex((String) identifier), value);
    }

    @Override
    public H2Statement bind(int index, Object value) {
        Objects.requireNonNull(value, "value must not be null");

        this.bindings.getCurrent().add(index, this.codecs.encode(value));

        return this;
    }

    @Override
    public H2Statement bindNull(Object identifier, @Nullable Class<?> type) {
        Objects.requireNonNull(identifier, "identifier must not be null");
        ObjectUtils.requireType(identifier, String.class, "identifier must be a String");

        bindNull(getIndex((String) identifier), type);

        return this;
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
            .flatMap(sql -> execute(this.client, sql, this.bindings, this.codecs));
    }

    Binding getCurrentBinding() {
        return this.bindings.getCurrent();
    }

    private static Flux<H2Result> execute(Client client, String sql, Bindings bindings, Codecs codecs) {
        if (!SELECT.matcher(sql).matches()) {
            return client.update(sql, bindings.bindings)
                .map(result -> H2Result.toResult(result.getGeneratedKeys(), result.getUpdateCount(), codecs));
        } else {
            return client.query(sql, bindings.bindings)
                .map(result -> H2Result.toResult(result, null, codecs));
        }
    }

    private int getIndex(String identifier) {
        Matcher matcher = PARAMETER_SYMBOL.matcher(identifier);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format("Identifier '%s' is not a valid identifier. Should be of the pattern '%s'.", identifier, PARAMETER_SYMBOL.pattern()));
        }

        return Integer.parseInt(matcher.group(1)) - 1;
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

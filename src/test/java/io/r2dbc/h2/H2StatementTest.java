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
import io.r2dbc.h2.codecs.MockCodecs;
import org.h2.result.LocalResult;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class H2StatementTest {

    private final Client client = mock(Client.class, RETURNS_SMART_NULLS);

    private final Value parameter = ValueInt.get(100);

    private final MockCodecs codecs = MockCodecs.builder().encoding(100, this.parameter).build();

    private final H2Statement statement = new H2Statement(this.client, this.codecs, "test-query-$1");

    @Test
    void bind() {
        assertThat(this.statement.bind("$1", 100).getCurrentBinding()).isEqualTo(new Binding().add(0, ValueInt.get(100)));
    }

    @Test
    void bindIndex() {
        assertThat(this.statement.bind(0, 100).getCurrentBinding()).isEqualTo(new Binding().add(0, ValueInt.get(100)));
    }

    @Test
    void bindIndexNoValue() {
        assertThatNullPointerException().isThrownBy(() -> this.statement.bind(1, null))
            .withMessage("value must not be null");
    }

    @Test
    void bindNoIdentifier() {
        assertThatNullPointerException().isThrownBy(() -> this.statement.bind(null, ""))
            .withMessage("identifier must not be null");
    }

    @Test
    void bindNoValue() {
        assertThatNullPointerException().isThrownBy(() -> this.statement.bind("$1", null))
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
        assertThatNullPointerException().isThrownBy(() -> this.statement.bindNull(null, Integer.class))
            .withMessage("identifier must not be null");
    }

    @Test
    void bindNullWrongIdentifierFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull("foo", Integer.class))
            .withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '.*\\$([\\d]+).*'.");
    }

    @Test
    void bindNullWrongIdentifierType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bindNull(new Object(), Integer.class))
            .withMessage("identifier must be a String");
    }

    @Test
    void bindWrongIdentifierFormat() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind("foo", ""))
            .withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '.*\\$([\\d]+).*'.");
    }

    @Test
    void bindWrongIdentifierType() {
        assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind(new Object(), ""))
            .withMessage("identifier must be a String");
    }

    @Test
    void constructorNoClient() {
        assertThatNullPointerException().isThrownBy(() -> new H2Statement(null, this.codecs, "test-query"))
            .withMessage("client must not be null");
    }

    @Test
    void constructorNoSql() {
        assertThatNullPointerException().isThrownBy(() -> new H2Statement(this.client, this.codecs, null))
            .withMessage("sql must not be null");
    }

    @Test
    void execute() {
        when(this.client.query("select test-query-$1", Arrays.asList(
            new Binding().add(0, ValueInt.get(100)),
            new Binding().add(0, ValueInt.get(200))
        ))).thenReturn(Flux.just(
            new LocalResult(),
            new LocalResult()
        ));

        MockCodecs codecs = MockCodecs.builder()
            .encoding(100, ValueInt.get(100))
            .encoding(200, ValueInt.get(200))
            .build();

        new H2Statement(this.client, codecs, "select test-query-$1")
            .bind("$1", 100)
            .add()
            .bind("$1", 200)
            .add()
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(2)
            .verifyComplete();
    }

    @Test
    void executeWithoutAdd() {
        when(this.client.update("insert test-query-$1", Arrays.asList(
            new Binding().add(0, ValueInt.get(100))
        ))).thenReturn(Flux.just(
            new ResultWithGeneratedKeys.WithKeys(0, new LocalResult())
        ));

        new H2Statement(this.client, this.codecs, "insert test-query-$1")
            .bind("$1", 100)
            .execute()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

}
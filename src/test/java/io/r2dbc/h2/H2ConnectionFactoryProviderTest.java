/*
 * Copyright 2017-2019 the original author or authors.
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

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.H2_DRIVER;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.PROTOCOL_MEM;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static org.assertj.core.api.Assertions.assertThat;

final class H2ConnectionFactoryProviderTest {

    private final H2ConnectionFactoryProvider provider = new H2ConnectionFactoryProvider();

    @Test
    void doesNotSupport() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, H2_DRIVER)
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithWrongDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, "test-driver")
            .option(URL, "test-url")
            .build())).isFalse();
    }

    @Test
    void doesNotSupportWithoutDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(URL, "test-url")
            .build())).isFalse();
    }

    @Test
    void supportsWithProtocolAndDatabase() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, H2_DRIVER)
            .option(PROTOCOL, PROTOCOL_MEM)
            .option(DATABASE, "test-database")
            .build())).isTrue();
    }

    @Test
    void supportsWithUrl() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, H2_DRIVER)
            .option(URL, "test-url")
            .build())).isTrue();
    }

    @Test
    void returnsDriverIdentifier() {
        assertThat(this.provider.getDriver()).isEqualTo(H2_DRIVER);
    }
}

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

import io.r2dbc.spi.Closeable;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

/**
 * Union-interface combining {@link ConnectionFactory} and {@link Closeable} for {@link H2Connection}.
 * <p>Closing this {@link ConnectionFactory} invalidates all open {@link H2Connection}s and {@link #create() connection creation} is no longer possible.
 */
public interface CloseableConnectionFactory extends ConnectionFactory, Closeable {

    @Override
    Mono<H2Connection> create();

    @Override
    Mono<Void> close();
}

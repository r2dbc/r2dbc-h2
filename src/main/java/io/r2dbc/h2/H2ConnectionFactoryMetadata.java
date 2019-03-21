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

import io.r2dbc.spi.ConnectionFactoryMetadata;

/**
 * An implementation of {@link ConnectionFactoryMetadata} for an H2 database.
 */
public final class H2ConnectionFactoryMetadata implements ConnectionFactoryMetadata {

    /**
     * The name of the H2 database product.
     */
    public static final String NAME = "H2";

    static final H2ConnectionFactoryMetadata INSTANCE = new H2ConnectionFactoryMetadata();

    private H2ConnectionFactoryMetadata() {
    }

    @Override
    public String getName() {
        return NAME;
    }
}

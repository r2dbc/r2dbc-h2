/*
 * Copyright 2017-2018 the original author or authors.
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

import io.r2dbc.spi.R2dbcException;
import org.h2.message.DbException;

/**
 * An exception that represents a database error.  This exception is a wrapper around {@link DbException}.
 */
public final class H2DatabaseException extends R2dbcException {

    /**
     * Creates a new exception.
     *
     * @param delegate the exception being wrapped.
     */
    public H2DatabaseException(DbException delegate) {
        super(delegate.getSQLException().getMessage(), delegate.getSQLException().getSQLState(), delegate.getErrorCode(), delegate);
    }

    @Override
    public String toString() {
        return "H2DatabaseException{} " + super.toString();
    }
}

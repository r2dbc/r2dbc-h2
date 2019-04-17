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

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientResourceException;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;

/**
 * Factory to translate JDBC exceptions to R2DBC exceptions.
 */
public final class H2DatabaseExceptionFactory {

    public static R2dbcException create(SQLException e) {
        if (e.getClass() == SQLDataException.class) {
            return new H2R2dbcDataException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLException.class) {
            return new H2R2dbcException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLFeatureNotSupportedException.class) {
            return new H2R2dbcNonTransientException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLIntegrityConstraintViolationException.class) {
            return new R2dbcDataIntegrityViolationException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLInvalidAuthorizationSpecException.class) {
            return new R2dbcPermissionDeniedException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLNonTransientConnectionException.class) {
            return new R2dbcNonTransientResourceException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLNonTransientException.class) {
            return new H2R2dbcNonTransientException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLRecoverableException.class) {
            return new H2R2dbcNonTransientException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLSyntaxErrorException.class) {
            return new R2dbcBadGrammarException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLTimeoutException.class) {
            return new R2dbcTimeoutException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLTransactionRollbackException.class) {
            return new R2dbcRollbackException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLTransientConnectionException.class) {
            return new R2dbcTransientResourceException(e.getMessage(), e.getSQLState(), e);
        }

        if (e.getClass() == SQLTransientException.class) {
            return new H2R2dbcTransientException(e.getMessage(), e.getSQLState(), e);
        }

        return new H2R2dbcException(e.getMessage(), e.getSQLState(), e);
    }

    static class H2R2dbcDataException extends R2dbcException {

        H2R2dbcDataException(String message, String sqlState, SQLException e) {
            super(message, sqlState, e);
        }
    }

    static class H2R2dbcException extends R2dbcException {

        H2R2dbcException(String message, String sqlState, SQLException e) {
            super(message, sqlState, e);
        }
    }

    static class H2R2dbcNonTransientException extends R2dbcNonTransientException {

        H2R2dbcNonTransientException(String message, String sqlState, SQLException e) {
            super(message, sqlState, e);
        }
    }

    static class H2R2dbcTransientException extends R2dbcNonTransientException {

        H2R2dbcTransientException(String message, String sqlState, SQLException e) {
            super(message, sqlState, e);
        }
    }

}

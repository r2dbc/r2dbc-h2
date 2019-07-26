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
import io.r2dbc.spi.R2dbcTransientException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.Test;

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

import static org.assertj.core.api.Assertions.assertThat;

final class H2DatabaseExceptionFactoryTest {

    @Test
    void sqlDataException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLDataException("SQLDataException", "SQLState", 999)))
            .hasMessage("SQLDataException")
            .isInstanceOf(R2dbcException.class)
            .extracting("sqlState", "errorCode").contains("SQLState", 999);
    }

    @Test
    void sqlException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void featureNotSupportedException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLFeatureNotSupportedException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcNonTransientException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void integrityConstraintViolationException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLIntegrityConstraintViolationException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcDataIntegrityViolationException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void invalidAuthorizationSpecException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLInvalidAuthorizationSpecException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcPermissionDeniedException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void nonTransientConnectionException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLNonTransientConnectionException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcNonTransientResourceException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void nonTransientException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLNonTransientException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcNonTransientException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void recoverableException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLRecoverableException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcNonTransientException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void syntaxErrorException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLSyntaxErrorException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcBadGrammarException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void timeoutException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLTimeoutException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcTimeoutException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void transactionRollbackException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLTransactionRollbackException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcRollbackException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void transientConnectionException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLTransientConnectionException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcTransientResourceException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void transientException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLTransientException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcTransientException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    @Test
    void unknownException() {
        assertThat(H2DatabaseExceptionFactory.convert(new SQLException("SQLException", "SQLState", 999)))
            .hasMessage("SQLException")
            .isInstanceOf(R2dbcException.class)
            .extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }
}

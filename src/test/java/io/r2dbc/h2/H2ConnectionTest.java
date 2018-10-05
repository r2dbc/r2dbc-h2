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

import io.r2dbc.spi.IsolationLevel;
import org.h2.engine.ConnectionInfo;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * @author Greg Turnquist
 */
final class H2ConnectionTest {

	@Test
	void constructorConnectionUrl() {

		new H2Connection("mem:r2dbc-test-mem")
			.close()
			.as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void constructorConnectionInfo() {

		ConnectionInfo connectionInfo = new ConnectionInfo("mem:r2dbc-test-mem2");
		connectionInfo.setUserName("foo");
		connectionInfo.setUserPasswordHash("bar".getBytes());

		new H2Connection(connectionInfo, true)
			.close()
			.as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void beginTransaction() {

		new H2Connection("mem:r2dbc-test-mem3")
			.beginTransaction()
			.as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void setTransactionIsolationReadCommitted() {

		new H2Connection("mem:r2dbc-test-mem4")
			.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED)
			.as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void setTransactionIsolationReadUncommitted() {

		new H2Connection("mem:r2dbc-test-mem5")
			.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED)
			.as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void setTransactionIsolationRepeatableRead() {

		new H2Connection("mem:r2dbc-test-mem6")
			.setTransactionIsolationLevel(IsolationLevel.REPEATABLE_READ)
			.as(StepVerifier::create)
			.verifyComplete();
	}

	@Test
	void setTransactionIsolationSerializable() {

		new H2Connection("mem:r2dbc-test-mem7")
			.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE)
			.as(StepVerifier::create)
			.verifyComplete();
	}
}

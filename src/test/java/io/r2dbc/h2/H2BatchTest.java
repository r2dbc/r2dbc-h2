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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import org.h2.command.CommandInterface;
import org.h2.engine.SessionInterface;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * @author Greg Turnquist
 */
final class H2BatchTest {

	private SessionInterface mockSession;
	private CommandInterface mockCommand;
	private ResultWithGeneratedKeys mockResultWithGeneratedKeys;
	private ResultInterface mockResult;

	@BeforeEach
	void setUp() {

		this.mockSession = mock(SessionInterface.class);
		this.mockCommand = mock(CommandInterface.class);
		this.mockResultWithGeneratedKeys = mock(ResultWithGeneratedKeys.class);
		this.mockResult = mock(ResultInterface.class);
	}

	@Test
	void constructorWithNullSessionShouldFail() {

		assertThatNullPointerException().isThrownBy(() -> {
			new H2Batch(null);
		}).withMessage("session must not be null");
	}

	@Test
	void constructor() {

		H2Batch batch = new H2Batch(mockSession);

		assertThat(batch.getSession()).isEqualTo(mockSession);
	}


	@Test
	void add() {

		H2Batch batch = new H2Batch(mockSession)
			.add("test-query")
			.add("another-test-query");

		assertThat(batch.getStatements()).containsExactly("test-query", "another-test-query");
	}

	@Test
	void addWithNullSqlShouldFail() {

		assertThatNullPointerException().isThrownBy(() -> {
			new H2Batch(mockSession).add(null);
		}).withMessage("sql must not be null");
	}

	@Test
	void executeWithOneStatement() {

		when(this.mockSession.prepareCommand(any(), anyInt())).thenReturn(mockCommand);
		when(this.mockCommand.executeUpdate(any())).thenReturn(this.mockResultWithGeneratedKeys);
		when(this.mockResultWithGeneratedKeys.getGeneratedKeys()).thenReturn(this.mockResult);
		when(this.mockResult.getRowCount()).thenReturn(42);

		new H2Batch(mockSession)
			.add("test-query")
			.execute()
			.as(StepVerifier::create)
			.expectNextCount(1)
			.verifyComplete();
	}

	@Test
	void executeWithMultipleStatements() {

		when(this.mockSession.prepareCommand(any(), anyInt())).thenReturn(mockCommand);
		when(this.mockCommand.executeUpdate(any())).thenReturn(this.mockResultWithGeneratedKeys);
		when(this.mockResultWithGeneratedKeys.getGeneratedKeys()).thenReturn(this.mockResult);
		when(this.mockResult.getRowCount()).thenReturn(42);

		new H2Batch(mockSession)
			.add("test-query")
			.add("another-test-query")
			.execute()
			.as(StepVerifier::create)
 			.expectNextCount(2)
			.verifyComplete();
	}

}

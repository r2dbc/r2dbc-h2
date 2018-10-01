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
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.h2.command.CommandInterface;
import org.h2.engine.SessionInterface;
import org.h2.expression.Parameter;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * @author Greg Turnquist
 */
final class H2StatementTest {

	private SessionInterface mockSession;
	private CommandInterface mockCommand;
	private ResultInterface mockResult;
	private H2Statement statement;

	@BeforeEach
	void setUp() {
		
		this.mockSession = mock(SessionInterface.class);
		this.mockCommand = mock(CommandInterface.class);
		this.mockResult = mock(ResultInterface.class);

		this.statement = new H2Statement(mockSession, "select test-query");
	}

	@Test
	void constructorWithNullSessionShouldFail() {

		assertThatNullPointerException().isThrownBy(() -> {
			new H2Statement(null, "test-query");
		}).withMessage("session must not be null");
	}

	@Test
	void constructorWithNullSqlShouldFail() {

		assertThatNullPointerException().isThrownBy(() -> {
			new H2Statement(this.mockSession, null);
		}).withMessage("sql must not be null");
	}

	@Test
	void bindNoIndex() {

		assertThatNullPointerException().isThrownBy(() -> {
			this.statement.bind((Integer) null, "some value");
		}).withMessage("index must not be null");
	}

	@Test
	void bindIndexNoValue() {

		assertThatNullPointerException().isThrownBy(() -> {
			this.statement.bind(1, null);
		}).withMessage("value must not be null");
	}

	@Test
	void bindNoIdentifier() {

		assertThatNullPointerException().isThrownBy(() -> {
			this.statement.bind((String) null, "some value");
		}).withMessage("identifier must not be null");
	}

	@Test
	void bindIdentifierNoValue() {

		assertThatNullPointerException().isThrownBy(() -> {
			this.statement.bind("$1", null);
		}).withMessage("value must not be null");
	}

	@Test
	void bindWrongIdentifierFormat() {

		assertThatIllegalArgumentException().isThrownBy(() -> {
			this.statement.bind("foo", Integer.class);
		}).withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '.*\\$([\\d]+).*'.");
	}

	@Test
	void bindWrongIdentifierType() {
		assertThatIllegalArgumentException().isThrownBy(() -> this.statement.bind(new Object(), ""))
			.withMessage("identifier must be a String");
	}

	@Test
	void bind() {

		assertThat(this.statement.bind("$1", 100).getCurrentBinding())
			.isEqualTo(new Binding().add(0, 100));
	}

	@Test
	void bindNullNoIdentifier() {

		assertThatNullPointerException().isThrownBy(() -> {
			this.statement.bindNull(null, Integer.class);
		}).withMessage("identifier must not be null");
	}

	@Test
	void bindNullNoType() {

		assertThatNullPointerException().isThrownBy(() -> {
			this.statement.bindNull("$1", null);
		}).withMessage("type must not be null");
	}

	@Test
	void bindNullWrongIdentifierFormat() {

		assertThatIllegalArgumentException().isThrownBy(() -> {
			this.statement.bindNull("foo", Integer.class);
		}).withMessage("Identifier 'foo' is not a valid identifier. Should be of the pattern '.*\\$([\\d]+).*'.");
	}

	@Test
	void bindNull() {

		assertThat(this.statement.bindNull("$1", Integer.class).getCurrentBinding())
			.isEqualTo(new Binding().add(0, ValueNull.INSTANCE));
	}

	@Test
	void execute() {

		when(this.mockSession.prepareCommand(any(), anyInt())).thenReturn(mockCommand);
		when(this.mockCommand.executeQuery(anyInt(), anyBoolean())).thenReturn(mockResult);
		when(this.mockResult.getRowCount()).thenReturn(42);

		this.statement
			.execute()
			.flatMap(H2Result::getRowsUpdated)
			.as(StepVerifier::create)
			.expectNextCount(1)
			.verifyComplete();
	}

	@Test
	void executeReturningGeneratedKeys() {

		List<Parameter> parameters = new ArrayList<>();
		parameters.add(new Parameter(0));

		ResultWithGeneratedKeys resultWithGeneratedKeys = mock(ResultWithGeneratedKeys.class);

		when(this.mockSession.prepareCommand(any(), anyInt())).thenReturn(mockCommand);
		when(this.mockCommand.executeUpdate(any())).thenReturn(resultWithGeneratedKeys);
		doReturn(parameters).when(this.mockCommand).getParameters();
		when(resultWithGeneratedKeys.getGeneratedKeys()).thenReturn(this.mockResult);
		when(this.mockResult.getRowCount()).thenReturn(42);

		new H2Statement(this.mockSession, "INSERT INTO test VALUES ($1)")
			.bind("$1", 100)
			.executeReturningGeneratedKeys()
			.as(StepVerifier::create)
			.expectNextCount(1)
			.verifyComplete();
	}
}
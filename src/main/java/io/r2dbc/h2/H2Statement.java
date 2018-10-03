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

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.r2dbc.h2.helper.ObjectUtils;
import io.r2dbc.spi.Statement;
import org.h2.engine.SessionInterface;
import org.h2.value.ValueNull;
import reactor.core.publisher.Flux;

/**
 * A strongly typed {@link Statement} for H2.
 *
 * @author Ben Hale
 * @author Greg Turnquist
 */
@Slf4j
@ToString
@EqualsAndHashCode
public final class H2Statement implements Statement {

	public static final Pattern PARAMETER_SYMBOL = Pattern.compile(".*\\$([\\d]+).*");

	private final SessionInterface session;
	private final String sql;
	private final Bindings bindings = new Bindings();


	H2Statement(SessionInterface session, String sql) {

		Objects.requireNonNull(session, "session must not be null");
		Objects.requireNonNull(sql, "sql must not be null");
		
		this.session = session;
		this.sql = sql;
	}

	@Override
	public H2Statement add() {

		this.bindings.finish();
		return this;
	}

	@Override
	public H2Statement bind(Object identifier, Object value) {

		Objects.requireNonNull(identifier, "identifier must not be null");
		ObjectUtils.requireType(identifier, String.class, "identifier must be a String");

		return bind(getIndex((String) identifier), value);
	}

	@Override
	public H2Statement bind(Integer index, Object value) {
		
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(value, "value must not be null");

		this.bindings.getCurrent().add(index, value);

		return this;
	}

	@Override
	public H2Statement bindNull(Object identifier, Class<?> type) {
		
		Objects.requireNonNull(identifier, "identifier must not be null");
		ObjectUtils.requireType(identifier, String.class, "identifier must be a String");
		Objects.requireNonNull(type, "type must not be null");

		bindNull(getIndex((String) identifier), type);
		
		return this;
	}

	private void bindNull(Integer index, Class<?> type) {
		this.bindings.getCurrent().add(index, ValueNull.INSTANCE);
	}

	@Override
	public Flux<H2Result> execute() {

		return Flux.fromArray(this.sql.split(";"))
			.map(String::trim)
			.flatMap(this::execute);
	}

	@Override
	public Flux<H2Result> executeReturningGeneratedKeys() {
		return this.execute(this.sql);
	}

	private Flux<H2Result> execute(String sql) {

		if (sql.toLowerCase().startsWith("select")) {

			return H2Utils.query(this.session, sql, this.bindings.bindings, 3)
				.map(H2Result::toResult);

		} else {

			return H2Utils.update(this.session, sql, this.bindings.bindings)
				.map(H2Result::toResult);
		}
	}

	Binding getCurrentBinding() {
		return this.bindings.getCurrent();
	}

	private Integer getIndex(String identifier) {

		Matcher matcher = PARAMETER_SYMBOL.matcher(identifier);

		if (!matcher.find()) {
			throw new IllegalArgumentException(String.format("Identifier '%s' is not a valid identifier. Should be of the pattern '%s'.", identifier, PARAMETER_SYMBOL.pattern()));
		}

		return  Integer.parseInt(matcher.group(1))- 1;
	}
	
	@ToString
	@EqualsAndHashCode
	private static final class Bindings {

		private final List<Binding> bindings = new ArrayList<>();

		private Binding current;

		private void finish() {
			this.current = null;
		}

		private Binding getCurrent() {

			if (this.current == null) {
				this.current = new Binding();
				this.bindings.add(this.current);
			}

			return this.current;
		}
	}
}

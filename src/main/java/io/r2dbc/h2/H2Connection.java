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

import static org.h2.engine.Constants.*;
import static org.h2.util.StringUtils.*;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.function.Function;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Mutability;
import org.h2.api.ErrorCode;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.CloseWatcher;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Greg Turnquist
 */
@Slf4j
public final class H2Connection implements Connection {

	private final SessionInterface session;

	private final String url;
	private final String user;
	private final boolean scopeGeneratedKeys;
	private final CloseWatcher watcher;

	public H2Connection(String connectionUrl) {
		this(createConnectionInfo(connectionUrl, "sa", ""), true);
	}

	public H2Connection(ConnectionInfo ci, boolean useBaseDirectory) {

		if (useBaseDirectory) {
			String baseDir = SysProperties.getBaseDir();
			if (baseDir != null) {
				ci.setBaseDir(baseDir);
			}
		}

		// this will return an embedded or server connection
		session = new SessionRemote(ci).connectEmbeddedOrServer(false);
		session.setAutoCommit(false);
		this.user = ci.getUserName();
		if (log.isTraceEnabled()) {
			log.trace("Connection = DriverManager.getConnection("
				+ quoteJavaString(ci.getURL()) + ", " + quoteJavaString(user)
				+ ", \"\");");
		}
		this.url = ci.getURL();
		scopeGeneratedKeys = ci.getProperty("SCOPE_GENERATED_KEYS", false);
		closeOld();
		watcher = CloseWatcher.register(this, session, true);
	}

	private static ConnectionInfo createConnectionInfo(String connectionUrl, String user, String password) {

		ConnectionInfo connectionInfo = new ConnectionInfo(connectionUrl);
		connectionInfo.setUserName(user);
		connectionInfo.setUserPasswordHash(password.getBytes());

		return connectionInfo;
	}

	private void closeOld() {
		
		while (true) {
			CloseWatcher w = CloseWatcher.pollUnclosed();
			if (w == null) {
				break;
			}
			try {
				w.getCloseable().close();
			} catch (Exception e) {
				log.error(e.getMessage());
			}
			Exception ex = DbException.get(ErrorCode.TRACE_CONNECTION_NOT_CLOSED);
			log.error(w.getOpenStackTrace());
			log.error(ex.getMessage());
		}
	}


	@Override
	public Mono<Void> beginTransaction() {

		return useTransactionStatus(inTransaction -> {
			if (!inTransaction) {
				this.session.setAutoCommit(false);
				return Mono.empty();
			} else {
				log.debug("Skipping begin transaction because already in one");
				return Mono.empty();
			}
		});
	}

	@Override
	public Mono<Void> close() {

		if (this.session == null) {
			return Mono.empty();
		}

		return Mono.defer(() -> {
			CloseWatcher.unregister(this.watcher);
			this.session.cancel();
			this.session.close();
			return Mono.empty();
		})
		.then();
	}

	@Override
	public Mono<Void> commitTransaction() {

		return withTransactionStatus(inTransaction -> {
				if (inTransaction) {
					return H2Utils.update(this.session, "COMMIT").then();
				} else {
					log.debug("Skipping commit transaction because no transaction in progress.");
					return Mono.empty();
				}
		}).then();
	}

	@Override
	public H2Batch createBatch() {
		return new H2Batch(this.session);
	}

	@Override
	public Mono<Void> createSavepoint(String name) {

		Objects.requireNonNull(name, "name must not be null");

		return useTransactionStatus(inTransaction -> {
			if (inTransaction) {
				return H2Utils.update(this.session, String.format("SAVEPOINT %s", name));
			} else {
				log.debug("Skipping savepoint because no transaction in progress.");
				return Mono.empty();
			}
		});
	}

	@Override
	public H2Statement createStatement(String sql) {

		Objects.requireNonNull(sql, "sql must not be null");

		return new H2Statement(this.session, sql);
	}

	@Override
	public Mono<Void> releaseSavepoint(String name) {

		Objects.requireNonNull(name, "name must not be null");

		return useTransactionStatus(inTransaction -> {
			if (inTransaction) {
				return H2Utils.update(this.session, String.format("RELEASE SAVEPOINT %s", name));
			} else {
				log.debug("Skipping release savepoint because no transaction in progress.");
				return Mono.empty();
			}
		});
	}

	@Override
	public Mono<Void> rollbackTransaction() {

		return useTransactionStatus(inTransaction -> {
			if (inTransaction) {
				return H2Utils.update(this.session, "ROLLBACK");
			} else {
				log.debug("Skipping rollback because no transaction in progress.");
				return Mono.empty();
			}
		});
	}

	@Override
	public Mono<Void> rollbackTransactionToSavepoint(String name) {

		Objects.requireNonNull(name, "name must not be null");

		return useTransactionStatus(inTransaction -> {
			if (inTransaction) {
				return H2Utils.update(this.session, String.format("ROLLBACK TO SAVEPOINT %s", name));
			} else {
				log.debug("Skipping rollback to savepoint because no transaction in progress.");
				return Mono.empty();
			}
		});
	}

	@Override
	public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {

		Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");

		return H2Utils.update(this.session, getTransactionIsolationLevelQuery(isolationLevel))
			.then();
	}

	@Override
	public Mono<Void> setTransactionMutability(Mutability mutability) {

		log.info("setTransactionMutability: Transaction mutability not yet supported.");
		return Mono.empty();
	}

	private Mono<Void> useTransactionStatus(Function<Boolean, Publisher<?>> f) {
		return Flux.defer(() -> f.apply(inTransaction(this.session))).then();
	}

	private <T> Mono<T> withTransactionStatus(Function<Boolean, T> f) {
		return Mono.defer(() -> Mono.just(f.apply(inTransaction(session))));
	}

	/**
	 * Is this {@link SessionInterface} in the middle of a transaction?
	 * 
	 * @param session
	 * @return
	 */
	private static boolean inTransaction(SessionInterface session) {
		return !session.getAutoCommit();
	}

	private static String getTransactionIsolationLevelQuery(IsolationLevel isolationLevel) {

		switch (isolationLevel) {
			case READ_COMMITTED: 	return "SET LOCK_MODE " + LOCK_MODE_READ_COMMITTED;
			case READ_UNCOMMITTED: 	return "SET LOCK_MODE " + LOCK_MODE_OFF;
			case REPEATABLE_READ:
			case SERIALIZABLE: 		return "SET LOCK_MODE " + LOCK_MODE_TABLE;
			default: 				throw DbException.getInvalidValueException("level", isolationLevel);
		}
	}

//	private Mono<IsolationLevel> getTransactionIsolationLevel() {
//
//		return H2Utils.query(this.session, "CALL LOCK_MODE()", this.bindings, 0)
//			.map(resultInterface -> {
//				resultInterface.next();
//				int lockMode = resultInterface.currentRow()[0].getInt();
//				resultInterface.close();;
//				return lockMode;
//			})
//			.map(lockMode -> {
//				switch(lockMode) {
//					case LOCK_MODE_READ_COMMITTED: 	return READ_COMMITTED;
//					case LOCK_MODE_OFF: 			return READ_UNCOMMITTED;
//					case LOCK_MODE_TABLE:
//					case LOCK_MODE_TABLE_GC:		return SERIALIZABLE;
//					default:    					throw DbException.getInvalidValueException("level", lockMode);
//				}
//			});
//	}
}

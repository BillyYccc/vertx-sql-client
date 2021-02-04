/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.vertx.sqlclient.impl;

import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.impl.command.CommandBase;
import io.vertx.sqlclient.Transaction;

import io.vertx.core.*;
import io.vertx.sqlclient.impl.tracing.QueryTracer;
import io.vertx.sqlclient.spi.DatabaseMetadata;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class SqlConnectionImpl<C extends SqlConnection> extends SqlConnectionBase<C> implements SqlConnection, Connection.Holder {

  private volatile Handler<Throwable> exceptionHandler;
  private volatile Handler<Void> closeHandler;
  private TransactionImpl tx;

  public SqlConnectionImpl(ContextInternal context, Connection conn, QueryTracer tracer, ClientMetrics metrics) {
    super(context, conn, tracer, metrics);
  }

  @Override
  protected <T> PromiseInternal<T> promise() {
    return context.promise();
  }

  @Override
  protected <T> PromiseInternal<T> promise(Handler<AsyncResult<T>> handler) {
    return context.promise(handler);
  }

  @Override
  public void handleClosed() {
    Handler<Void> handler = closeHandler;
    if (handler != null) {
      context.runOnContext(handler);
    }
  }

  @Override
  public <R> void schedule(CommandBase<R> cmd, Promise<R> promise) {
    if (tx != null) {
      tx.schedule(cmd, promise);
    } else {
      conn.schedule(cmd, promise);
    }
  }

  @Override
  public void handleException(Throwable err) {
    Handler<Throwable> handler = exceptionHandler;
    if (handler != null) {
      context.runOnContext(v -> {
        handler.handle(err);
      });
    } else {
      err.printStackTrace();
    }
  }

  @Override
  public boolean isSSL() {
    return conn.isSsl();
  }

  @Override
  public DatabaseMetadata databaseMetadata() {
    return conn.getDatabaseMetaData();
  }

  @Override
  public C closeHandler(Handler<Void> handler) {
    closeHandler = handler;
    return (C) this;
  }

  @Override
  public C exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return (C) this;
  }

  @Override
  public Future<Transaction> begin() {
    if (tx != null) {
      throw new IllegalStateException();
    }
    tx = new TransactionImpl(context, conn);
    tx.completion().onComplete(ar -> {
      tx = null;
    });
    return tx.begin();
  }

  @Override
  boolean autoCommit() {
    return tx == null;
  }

  @Override
  public void begin(Handler<AsyncResult<Transaction>> handler) {
    Future<Transaction> fut = begin();
    fut.onComplete(handler);
  }

  public void handleEvent(Object event) {
  }

  @Override
  public Future<Void> close() {
    Promise<Void> promise = promise();
    close(promise);
    return promise.future();
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    close(promise(handler));
  }

  private void close(Promise<Void> promise) {
    context.execute(promise, p -> {
      if (tx != null) {
        tx.rollback(ar -> conn.close(this, p));
        tx = null;
      } else {
        conn.close(this, p);
      }
    });
  }

  public <T extends SqlConnectionImpl<T>> Future<SqlConnectionImpl<C>> initiateClientState() {
    // NO-OP
    return Future.succeededFuture(this);
  }
}

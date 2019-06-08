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

package io.vertx.mysqlclient;

import io.vertx.mysqlclient.junit.MySQLRule;
import io.vertx.sqlclient.*;
import io.vertx.core.Vertx;

import java.sql.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class RawBenchmark {

  private static final Tuple args = Tuple.of(1);

  public static void main(String[] args) throws Exception {
    MySQLRule rule = new MySQLRule();
    MySQLConnectOptions options = rule.startServer(MySQLRule.DatabaseType.MySQL, "8.0", false);
    /* remote usage
    MySQLConnectOptions options = new MySQLConnectOptions()
      .setHost("localhost")
      .setPort(3306)
      .setDatabase("mysql")
      .setUser("mysql")
      .setPassword("mysql");
    */
    largeSelectJDBC(options, 5_000);
    largeSelect(options, 5_000);
    singleSelectJDBC(options, 200_000);
    singleSelect(options, 200_000);

    rule.stopServer();
  }

  interface Benchmark {

    void run(Connection conn) throws Exception;

  }

  private static void singleSelectJDBC(MySQLConnectOptions options, int reps) throws Exception {
    benchmark("Single select jdbc", options, conn -> {
      PreparedStatement ps = conn.prepareStatement("select id, randomnumber from world where id=(?)");
      for (int i = 0;i < reps;i++) {
        ps.setInt(1, 1);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
          resultSet.getInt(1);
        }
        resultSet.close();
      }
    });
  }

  private static void largeSelectJDBC(MySQLConnectOptions options, int reps) throws Exception {
    benchmark("Large select jdbc", options, conn -> {
      PreparedStatement ps = conn.prepareStatement("SELECT id, randomnumber from world");
      for (int i = 0;i < reps;i++) {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
          resultSet.getInt(1);
        }
        resultSet.close();
      }
    });
  }

  private static void benchmark(String name, MySQLConnectOptions options, Benchmark benchmark) throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:mysql://"
      + options.getHost() + ":"
      + options.getPort() + "/testschema", options.getUser(), options.getPassword());
    long now = System.currentTimeMillis();
    benchmark.run(conn);
    System.out.println(name + ": " + (System.currentTimeMillis() - now));
  }

  private static void singleSelect(MySQLConnectOptions options, int reps) throws Exception {
    benchmark("Single select", options, (conn, latch) -> doSingleQuery(conn, reps, latch));
  }

  private static void largeSelect(MySQLConnectOptions options, int reps) throws Exception {
    benchmark("Large select", options, (conn, latch) -> doLargeQuery(conn, reps, latch));
  }

  private static void doSingleQuery(SqlConnection conn, int remaining, CompletableFuture<Void> latch) {
    if (remaining > 0) {
      conn.preparedQuery("SELECT id, randomnumber from world where id=?", args, ar -> {
        if (ar.succeeded()) {
          doSingleQuery(conn, remaining -1, latch);
        } else {
          latch.completeExceptionally(ar.cause());
        }
      });
    } else {
      latch.complete(null);
    }
  }

  private static void doLargeQuery(SqlConnection conn, int remaining, CompletableFuture<Void> latch) {
    if (remaining > 0) {
      conn.preparedQuery("SELECT id, randomnumber from world", ar -> {
        if (ar.succeeded()) {
          doLargeQuery(conn, remaining -1, latch);
          RowSet<Row> result = ar.result();
          for (Tuple tuple : result) {
            int val = tuple.getInteger(0);
          }
        } else {
          latch.completeExceptionally(ar.cause());
        }
      });
    } else {
      latch.complete(null);
    }
  }

  private static void benchmark(String name, MySQLConnectOptions options, BiConsumer<SqlConnection, CompletableFuture<Void>> benchmark) throws Exception {
    Vertx vertx = Vertx.vertx();
    MySQLPool client = MySQLPool.pool(vertx, new MySQLConnectOptions()
      .setHost(options.getHost())
      .setPort(options.getPort())
      .setDatabase(options.getDatabase())
      .setUser(options.getUser())
      .setPassword(options.getPassword()), new PoolOptions()
    );
    CompletableFuture<Void> latch = new CompletableFuture<>();
    long now = System.currentTimeMillis();
    client.getConnection(ar -> {
      if (ar.succeeded()) {
        benchmark.accept(ar.result(), latch);
      } else {
        latch.completeExceptionally(ar.cause());
      }
    });
    latch.get(2, TimeUnit.MINUTES);
    System.out.println(name + ": " + (System.currentTimeMillis() - now));
  }
}

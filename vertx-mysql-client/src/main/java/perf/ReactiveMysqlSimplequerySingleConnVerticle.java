package perf;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class ReactiveMysqlSimplequerySingleConnVerticle extends AbstractVerticle {
    Query<RowSet<Row>> query;

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle("perf.ReactiveMysqlSimplequerySingleConnVerticle", new DeploymentOptions()
                .setInstances(16)
                .setWorker(false))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        System.out.println("ReactiveMysqlSimplequerySingleConnVerticle started");
                    } else {
                        ar.cause().printStackTrace();
                    }
                });
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        HttpServer server = vertx.createHttpServer();
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setHost("localhost")
                .setPort(3306)
                .setDatabase("my_test")
                .setCachePreparedStatements(true)
                .setPreparedStatementCacheMaxSize(4096)
                .setUser("mysql")
                .setPassword("password");

        MySQLConnection.connect(vertx, connectOptions)
                .onFailure(err->err.printStackTrace())
                .onSuccess(conn -> {
                    Map<Integer, Query<RowSet<Row>>> mapping = new HashMap<>();
                    for (int i = 1; i <= 10; i++) {
                        mapping.put(i, conn.query(String.format("SELECT * FROM immutable WHERE id = %d;", i)));
                    }

                    Router router = Router.router(vertx);

                    router.route("/test").handler(routingContext -> {

                        int reqId = ThreadLocalRandom.current().nextInt(1, 11);
                        Query<RowSet<Row>> query = mapping.get(reqId);

                        // This handler will be called for every request
                        HttpServerResponse response = routingContext.response();
                        query.execute()
                                .onComplete(res -> {
                                    if (res.succeeded()) {
                                        RowSet<Row> rowSet = res.result();
                                        RowIterator<Row> iterator = rowSet.iterator();
                                        JsonArray jsonArray = new JsonArray();
                                        while (iterator.hasNext()) {
                                            Row row = iterator.next();
                                            JsonObject jsonObject = new JsonObject();
                                            jsonObject.put("id", row.getInteger("id"));
                                            jsonObject.put("message", row.getString("message"));
                                            jsonArray.add(jsonObject);
                                        }
                                        response.setStatusCode(200)
                                                .putHeader("content-type", "application/json; charset=utf-8");

                                        // Write to the response and end it

                                        response.end(jsonArray.encode());
//                            response.end("Hello, World!");
                                    } else {
                                        res.cause().printStackTrace();
                                        response.setStatusCode(500)
                                                .putHeader("content-type", "application/json; charset=utf-8");
                                    }
                                });


                    });
                    server.requestHandler(router).listen(1234)
                            .onFailure(err-> startPromise.fail(err))
                            .onSuccess(res -> startPromise.complete());

                });
    }
}

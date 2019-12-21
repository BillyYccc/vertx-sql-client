package io.vertx.pgclient;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.sqlclient.PoolOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ContextTest extends PgTestBase {

  private Vertx vertx;

  @Before
  public void setup() throws Exception {
    super.setup();
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void testConnectionNotInContext(TestContext testContext) {
    Async async = testContext.async();

    PgConnection.connect(vertx, options, testContext.asyncAssertSuccess(conn -> {
      Context connContext = Vertx.currentContext();
      testContext.assertNotNull(connContext);
      conn.query("SELECT 1", testContext.asyncAssertSuccess(result -> {
        testContext.assertEquals(connContext, Vertx.currentContext());
        conn.close();
        async.complete();
      }));
    }));
  }

  @Test
  public void testConnectionInSameContext(TestContext testContext) {
    Async async = testContext.async();
    Context appCtx = vertx.getOrCreateContext();
    appCtx.runOnContext(v1 -> {
      PgConnection.connect(vertx, options, testContext.asyncAssertSuccess(conn -> {
        testContext.assertEquals(appCtx, Vertx.currentContext());
        conn.query("SELECT 1", testContext.asyncAssertSuccess(result -> {
          testContext.assertEquals(appCtx, Vertx.currentContext());
          conn.close();
          async.complete();
        }));
      }));
    });
  }

  @Test
  public void testConnectionInDifferentContext(TestContext testContext) {
    Async async = testContext.async();
    Context appCtx1 = vertx.getOrCreateContext();
    Context appCtx2 = vertx.getOrCreateContext();
    testContext.assertNotEquals(appCtx1, appCtx2);
    appCtx1.runOnContext(v1 -> {
      PgConnection.connect(vertx, options, testContext.asyncAssertSuccess(conn -> {
        testContext.assertEquals(appCtx1, Vertx.currentContext());
        appCtx2.runOnContext(v2 -> {
          testContext.assertEquals(appCtx2, Vertx.currentContext());
          conn.query("SELECT 1", testContext.asyncAssertSuccess(result -> {
            testContext.assertEquals(appCtx1, Vertx.currentContext()); //FIXME appCtx1?
            conn.close();
            async.complete();
          }));
        });
      }));
    });
  }

  @Test
  public void testPooledConnectionNotInContext(TestContext testContext) {
    Context appCtx = vertx.getOrCreateContext();
    Async async = testContext.async();
    PgPool pool = PgPool.pool(vertx, options, new PoolOptions());
    pool.getConnection(testContext.asyncAssertSuccess(conn -> {
      conn.query("SELECT 1", testContext.asyncAssertSuccess(result -> {
        testContext.assertNotEquals(appCtx, Vertx.currentContext());
        conn.close();
        async.complete();
      }));
    }));
  }

  @Test
  public void testPooledConnectionInDifferentContext1(TestContext testContext) {
    // this tests a pool is created in a non-vertx context but get connection occurs in a vertx context
    Context appCtx = vertx.getOrCreateContext();
    Async async = testContext.async();
    testContext.assertNull(Vertx.currentContext());
    PgPool pool = PgPool.pool(vertx, options, new PoolOptions());
    appCtx.runOnContext(v1 -> {
      pool.getConnection(testContext.asyncAssertSuccess(conn -> {
        testContext.assertEquals(appCtx, Vertx.currentContext());
        conn.query("SELECT 1", testContext.asyncAssertSuccess(result -> {
          testContext.assertEquals(appCtx, Vertx.currentContext());
          conn.close();
          async.complete();
        }));
      }));
    });
  }

  @Test
  public void testPooledConnectionInDifferentContext2(TestContext testCtx) {
    // this tests a pool is created in appCtx but get connection occurs in connCtx
    Context appCtx = vertx.getOrCreateContext();
    Context connCtx = vertx.getOrCreateContext();
    testCtx.assertNotEquals(appCtx, connCtx);
    Async async = testCtx.async();
    appCtx.runOnContext(v1 -> {
      PgPool pool = PgPool.pool(vertx, options, new PoolOptions());
      connCtx.runOnContext(v -> {
        pool.getConnection(testCtx.asyncAssertSuccess(conn -> {
          testCtx.assertEquals(connCtx, Vertx.currentContext());
          conn.query("SELECT 1", testCtx.asyncAssertSuccess(result -> {
            testCtx.assertEquals(connCtx, Vertx.currentContext());
            async.complete();
          }));
        }));
      });
    });
  }

  @Test
  public void testPooledConnectionInDifferentContext3(TestContext testCtx) {
    // this tests a pool is created in a vertx context but get connection occurs in a non-vertx context
    Context appCtx = vertx.getOrCreateContext();
    Async async = testCtx.async();
    appCtx.runOnContext(v1 -> {
      PgPool pool = PgPool.pool(vertx, options, new PoolOptions());
      new Thread(() -> {
        testCtx.assertNull(Vertx.currentContext()); // non-vertx context
        pool.getConnection(testCtx.asyncAssertSuccess(conn -> {
          Context connCtx = Vertx.currentContext();
          testCtx.assertNotEquals(appCtx, connCtx);
          conn.query("SELECT 1", testCtx.asyncAssertSuccess(result -> {
            testCtx.assertEquals(connCtx, Vertx.currentContext());
            async.complete();
          }));
        }));
      }).start();
    });
  }
}

package io.vertx.pgclient.data;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class BinaryDataTypesExtendedCodecTest extends ExtendedQueryDataTypeCodecTestBase {
  @Test
  public void testBytea(TestContext ctx) {
    Random r = new Random();
    int len = 2048;
    byte[] bytes = new byte[len];
    r.nextBytes(bytes);
    Async async = ctx.async();
    PgConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.prepare("SELECT $1::BYTEA \"Bytea\"",
        ctx.asyncAssertSuccess(p -> {
          p.execute(Tuple.of(bytes), ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(1, result.size());
            Row row = result.iterator().next();
            ctx.assertTrue(Arrays.equals(bytes, row.getBuffer(0)));
            ctx.assertTrue(Arrays.equals(bytes, row.getBuffer("Bytea")));
            conn.close();
            async.complete();
          }));
        }));
    }));
  }

  @Test
  public void testBufferArray(TestContext ctx) {
    Random r = new Random();
    int len = 2048;
    byte[] bytes = new byte[len];
    r.nextBytes(bytes);
    Async async = ctx.async();
    PgConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.prepare("SELECT ARRAY[$1::BYTEA] \"Bytea\"",
        ctx.asyncAssertSuccess(p -> {
          p.execute(Tuple.tuple().addBuffer(bytes), ctx.asyncAssertSuccess(result -> {
            ctx.assertEquals(1, result.size());
            Row row = result.iterator().next();
            ctx.assertTrue(Arrays.equals(bytes, row.getBufferArray(0)[0]));
            ctx.assertTrue(Arrays.equals(bytes, row.getBufferArray("Bytea")[0]));
            conn.close();
            async.complete();
          }));
        }));
    }));
  }
}

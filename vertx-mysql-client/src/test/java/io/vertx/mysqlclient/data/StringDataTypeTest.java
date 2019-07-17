package io.vertx.mysqlclient.data;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(VertxUnitRunner.class)
public class StringDataTypeTest extends MySQLDataTypeTestBase {
  @Test
  public void testBinaryDecodeAll(TestContext ctx) {
    MySQLConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.preparedQuery("SELECT * FROM datatype WHERE id = 1", ctx.asyncAssertSuccess(result -> {
        ctx.assertEquals(1, result.size());
        Row row = result.iterator().next();
        ctx.assertEquals(1, row.getValue(0));
        ctx.assertTrue(Arrays.equals("HELLO".getBytes(), row.getBuffer(1)));
        ctx.assertTrue(Arrays.equals("HELLO, WORLD".getBytes(), row.getBuffer(2)));
        ctx.assertTrue(Arrays.equals("TINYBLOB".getBytes(), row.getBuffer(3)));
        ctx.assertTrue(Arrays.equals("BLOB".getBytes(), row.getBuffer(4)));
        ctx.assertTrue(Arrays.equals("MEDIUMBLOB".getBytes(), row.getBuffer(5)));
        ctx.assertTrue(Arrays.equals("LONGBLOB".getBytes(), row.getBuffer(6)));
        ctx.assertEquals("TINYTEXT", row.getValue(7));
        ctx.assertEquals("TEXT", row.getValue(8));
        ctx.assertEquals("MEDIUMTEXT", row.getValue(9));
        ctx.assertEquals("LONGTEXT", row.getValue(10));
        conn.close();
      }));
    }));
  }

  @Test
  public void testTextDecodeBinary(TestContext ctx) {
    testTextDecodeBinaryWithTable(ctx, "Binary", ("HELLO").getBytes());
  }

  @Test
  public void testBinaryDecodeBinary(TestContext ctx) {
    testBinaryDecodeBinaryWithTable(ctx, "Binary", ("HELLO").getBytes());
  }

  @Test
  public void testBinaryEncodeBinary(TestContext ctx) {
    testBinaryEncodeBinary(ctx, "Binary", ("HELLO").getBytes());
  }

  @Test
  public void testBinaryEncodeVarBinary(TestContext ctx) {
    testBinaryEncodeBinary(ctx, "VarBinary", ("HELLO, WORLD").getBytes());
  }

  @Test
  public void testTextDecodeVarBinary(TestContext ctx) {
    testTextDecodeBinaryWithTable(ctx, "VarBinary", ("HELLO, WORLD").getBytes());
  }

  @Test
  public void testBinaryDecodeVarBinary(TestContext ctx) {
    testBinaryDecodeBinaryWithTable(ctx, "VarBinary", ("HELLO, WORLD").getBytes());
  }

  @Test
  public void testBinaryEncodeTinyBlob(TestContext ctx) {
    testBinaryEncodeBinary(ctx, "TinyBlob", "TINYBLOB".getBytes());
  }

  @Test
  public void testTextDecodeTinyBlob(TestContext ctx) {
    testTextDecodeBinaryWithTable(ctx, "TinyBlob", "TINYBLOB".getBytes());
  }

  @Test
  public void testBinaryDecodeTinyBlob(TestContext ctx) {
    testBinaryDecodeBinaryWithTable(ctx, "TinyBlob", "TINYBLOB".getBytes());
  }

  @Test
  public void testBinaryEncodeBlob(TestContext ctx) {
    testBinaryEncodeBinary(ctx, "Blob", "BLOB".getBytes());
  }

  @Test
  public void testTextDecodeBlob(TestContext ctx) {
    testTextDecodeBinaryWithTable(ctx, "Blob", "BLOB".getBytes());
  }

  @Test
  public void testBinaryDecodeBlob(TestContext ctx) {
    testBinaryDecodeBinaryWithTable(ctx, "Blob", "BLOB".getBytes());
  }

  @Test
  public void testBinaryEncodeMediumBlob(TestContext ctx) {
    testBinaryEncodeBinary(ctx, "MediumBlob", "MEDIUMBLOB".getBytes());
  }

  @Test
  public void testTextDecodeMediumBlob(TestContext ctx) {
    testTextDecodeBinaryWithTable(ctx, "MediumBlob", "MEDIUMBLOB".getBytes());
  }

  @Test
  public void testBinaryDecodeMediumBlob(TestContext ctx) {
    testBinaryDecodeBinaryWithTable(ctx, "MediumBlob", "MEDIUMBLOB".getBytes());
  }

  @Test
  public void testBinaryEncodeLongBlob(TestContext ctx) {
    testBinaryEncodeBinary(ctx, "LongBlob", "LONGBLOB".getBytes());
  }

  @Test
  public void testTextDecodeLongBlob(TestContext ctx) {
    testTextDecodeBinaryWithTable(ctx, "LongBlob", "LONGBLOB".getBytes());
  }

  @Test
  public void testBinaryDecodeLongBlob(TestContext ctx) {
    testBinaryDecodeBinaryWithTable(ctx, "LongBlob", "LONGBLOB".getBytes());
  }

  @Test
  public void testBinaryEncodeTinyText(TestContext ctx) {
    testBinaryEncodeGeneric(ctx, "TinyText", "TINYTEXT");
  }

  @Test
  public void testTextDecodeTinyText(TestContext ctx) {
    testTextDecodeGenericWithTable(ctx, "TinyText", "TINYTEXT");
  }

  @Test
  public void testBinaryDecodeTinyText(TestContext ctx) {
    testBinaryDecodeGenericWithTable(ctx, "TinyText", "TINYTEXT");
  }

  @Test
  public void testBinaryEncodeText(TestContext ctx) {
    testBinaryEncodeGeneric(ctx, "Text", "TEXT");
  }

  @Test
  public void testTextDecodeText(TestContext ctx) {
    testTextDecodeGenericWithTable(ctx, "Text", "TEXT");
  }

  @Test
  public void testBinaryDecodeText(TestContext ctx) {
    testBinaryDecodeGenericWithTable(ctx, "Text", "TEXT");
  }

  @Test
  public void testBinaryEncodeMediumText(TestContext ctx) {
    testBinaryDecodeGenericWithTable(ctx, "MediumText", "MEDIUMTEXT");
  }

  @Test
  public void testTextDecodeMediumText(TestContext ctx) {
    testTextDecodeGenericWithTable(ctx, "MediumText", "MEDIUMTEXT");
  }

  @Test
  public void testBinaryDecodeMediumText(TestContext ctx) {
    testBinaryDecodeGenericWithTable(ctx, "MediumText", "MEDIUMTEXT");
  }

  @Test
  public void testBinaryEncodeLongText(TestContext ctx) {
    testBinaryEncodeGeneric(ctx, "LongText", "LONGTEXT");
  }

  @Test
  public void testTextDecodeLongText(TestContext ctx) {
    testTextDecodeGenericWithTable(ctx, "LongText", "LONGTEXT");
  }

  @Test
  public void testBinaryDecodeLongText(TestContext ctx) {
    testBinaryDecodeGenericWithTable(ctx, "LongText", "LONGTEXT");
  }

  private void testTextDecodeBinaryWithTable(TestContext ctx, String columnName, byte[] expected) {
    MySQLConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.query("SELECT `" + columnName + "` FROM datatype WHERE id = 1", ctx.asyncAssertSuccess(result -> {
        ctx.assertEquals(1, result.size());
        Row row = result.iterator().next();
        ctx.assertTrue(Arrays.equals(expected, row.getBuffer(0)));
        ctx.assertTrue(Arrays.equals(expected, row.getBuffer(columnName)));
        conn.close();
      }));
    }));
  }

  private void testBinaryDecodeBinaryWithTable(TestContext ctx, String columnName, byte[] expected) {
    MySQLConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.preparedQuery("SELECT `" + columnName + "` FROM datatype WHERE id = 1", ctx.asyncAssertSuccess(result -> {
        ctx.assertEquals(1, result.size());
        Row row = result.iterator().next();
        ctx.assertTrue(Arrays.equals(expected, row.getBuffer(0)));
        ctx.assertTrue(Arrays.equals(expected, row.getBuffer(columnName)));
        conn.close();
      }));
    }));
  }

  private void testBinaryEncodeBinary(TestContext ctx, String columnName, byte[] expected) {
    MySQLConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.preparedQuery("UPDATE datatype SET `" + columnName + "` = ?" + " WHERE id = 2", Tuple.tuple().addValue(expected), ctx.asyncAssertSuccess(updateResult -> {
        conn.preparedQuery("SELECT `" + columnName + "` FROM datatype WHERE id = 2", ctx.asyncAssertSuccess(result -> {
          ctx.assertEquals(1, result.size());
          Row row = result.iterator().next();
          ctx.assertTrue(Arrays.equals(expected, row.getBuffer(0)));
          ctx.assertTrue(Arrays.equals(expected, row.getBuffer(columnName)));
          conn.close();
        }));
      }));
    }));
  }
}

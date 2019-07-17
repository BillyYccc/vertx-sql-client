package io.vertx.pgclient.data;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import org.junit.Test;

import java.util.Arrays;

public class BinaryDataTypesSimpleCodecTest extends SimpleQueryDataTypeCodecTestBase {

  @Test
  public void testByteaHexFormat1(TestContext ctx) {
    testDecodeBytea(ctx, "12345678910", "Buffer1", "12345678910".getBytes());
  }

  @Test
  public void testByteaHexFormat2(TestContext ctx) {
    testDecodeBytea(ctx, "\u00DE\u00AD\u00BE\u00EF", "Buffer2", "\u00DE\u00AD\u00BE\u00EF".getBytes());
  }

  @Test
  public void testByteaEscapeBackslash(TestContext ctx) {
    testDecodeBytea(ctx, "\\\\\\134", "Buffer3", new byte[]{0x5C, 0x5C});
  }

  @Test
  public void testByteaEscapeNonPrintableOctets(TestContext ctx) {
    testDecodeBytea(ctx, "\\001\\007", "Buffer4", new byte[]{0x01, 0x07});
  }

  @Test
  public void testByteaEscapePrintableOctets(TestContext ctx) {
    testDecodeBytea(ctx, "123abc", "Buffer5", new byte[]{'1', '2', '3', 'a', 'b', 'c'});
  }

  @Test
  public void testByteaEscapeSingleQuote(TestContext ctx) {
    testDecodeBytea(ctx, "\'\'", "Buffer6", new byte[]{0x27});
  }

  @Test
  public void testByteaEscapeZeroOctet(TestContext ctx) {
    testDecodeBytea(ctx, "\\000", "Buffer7", new byte[]{0x00});
  }

  @Test
  public void testByteaEscapeFormat(TestContext ctx) {
    testDecodeBytea(ctx, "abc \\153\\154\\155 \\052\\251\\124", "Buffer8", new byte[]{'a', 'b', 'c', ' ', 'k', 'l', 'm', ' ', '*', (byte) 0xA9, 'T'});
  }

  @Test
  public void testByteaEmptyString(TestContext ctx) {
    testDecodeBytea(ctx, "", "Buffer9", "".getBytes());
  }

  @Test
  public void testDecodeHexByteaArray(TestContext ctx) {
    byte[][] expected = new byte[1][1];
    expected[0] = "HELLO".getBytes();
    testDecodeByteaArray(ctx, "ARRAY [decode('48454c4c4f', 'hex') :: BYTEA]", "BufferArray", expected);
  }

  @Test
  public void testDecodeEscapeByteaArray(TestContext ctx) {
    byte[][] expected = new byte[1][1];
    expected[0] = new byte[]{'a', 'b', 'c', ' ', 'k', 'l', 'm', ' ', '*', (byte) 0xA9, 'T'};
    testDecodeByteaArray(ctx, "ARRAY [decode('abc \\153\\154\\155 \\052\\251\\124', 'escape') :: BYTEA]", "BufferArray2", expected);
  }

  private void testDecodeBytea(TestContext ctx,
                               String data,
                               String columnName,
                               byte[] expected) {
    Async async = ctx.async();
    PgConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.query("SELECT '" + data + "' :: " + "BYTEA" + " \"" + columnName + "\"", ctx.asyncAssertSuccess(result -> {
        ctx.assertEquals(1, result.size());
        Row row = result.iterator().next();
        ctx.assertTrue(Arrays.equals(expected, row.getBuffer(0)));
        ctx.assertTrue(Arrays.equals(expected, row.getBuffer(columnName)));
        conn.close();
        async.complete();
      }));
    }));
  }

  private void testDecodeByteaArray(TestContext ctx,
                                    String data,
                                    String columnName,
                                    byte[][] expected) {
    Async async = ctx.async();
    PgConnection.connect(vertx, options, ctx.asyncAssertSuccess(conn -> {
      conn.query("SELECT " + data + " \"" + columnName + "\"", ctx.asyncAssertSuccess(result -> {
        ctx.assertEquals(1, result.size());
        Row row = result.iterator().next();
        ctx.assertTrue(Arrays.deepEquals(expected, row.getBufferArray(0)));
        ctx.assertTrue(Arrays.deepEquals(expected, row.getBufferArray(columnName)));
        conn.close();
        async.complete();
      }));
    }));
  }
}

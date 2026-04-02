package org.tron.core.services.jsonrpc;

import org.junit.Test;
import org.tron.common.utils.ByteArray;

public class HexParamValidationTest {

  @Test
  public void testNormalHex() {
    ByteArray.hexToBigInteger("0xffffffffffffffff");
  }

  @Test
  public void testMaxLengthHex() {
    StringBuilder sb = new StringBuilder("0x");
    for (int i = 0; i < 126; i++) {
      sb.append("a");
    }
    assert sb.toString().length() == 128;
    ByteArray.hexToBigInteger(sb.toString());
  }

  @Test
  public void testOversizedHex() {
    StringBuilder sb = new StringBuilder("0x");
    for (int i = 0; i < 198; i++) {
      sb.append("f");
    }
    assert sb.toString().length() == 200;
    ByteArray.hexToBigInteger(sb.toString());
  }
}

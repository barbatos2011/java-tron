package org.tron.core.services.jsonrpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Test;
import org.tron.core.services.filter.BufferedResponseWrapper;
import org.tron.core.services.filter.CachedBodyRequestWrapper;

public class JsonRpcLimitsTest {

  @Test
  public void testCachedBodyReplay() throws Exception {
    HttpServletRequest req = mock(HttpServletRequest.class);
    byte[] body = "{\"method\":\"eth_blockNumber\"}"
        .getBytes(StandardCharsets.UTF_8);
    CachedBodyRequestWrapper wrapper =
        new CachedBodyRequestWrapper(req, body);
    byte[] buf = new byte[body.length];
    wrapper.getInputStream().read(buf);
    assertEquals(new String(body), new String(buf));
  }

  @Test
  public void testBufferedResponseNormal() throws Exception {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    BufferedResponseWrapper wrapper =
        new BufferedResponseWrapper(resp, 1024);
    wrapper.getOutputStream().write("hello".getBytes());
    assertEquals("hello", new String(wrapper.toByteArray()));
  }

  @Test
  public void testBufferedResponseExceedsLimit() {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    BufferedResponseWrapper wrapper =
        new BufferedResponseWrapper(resp, 10);
    assertThrows(
        BufferedResponseWrapper.ResponseTooLargeException.class,
        () -> wrapper.getOutputStream()
            .write("this exceeds 10 bytes".getBytes()));
  }

  @Test
  public void testBufferedResponseNoLimit() throws Exception {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    BufferedResponseWrapper wrapper =
        new BufferedResponseWrapper(resp, 0);
    byte[] data = new byte[10_000];
    wrapper.getOutputStream().write(data);
    assertEquals(10_000, wrapper.toByteArray().length);
  }
}

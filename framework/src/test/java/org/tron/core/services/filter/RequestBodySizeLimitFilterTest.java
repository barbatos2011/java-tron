package org.tron.core.services.filter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tron.common.parameter.CommonParameter;

public class RequestBodySizeLimitFilterTest {

  private RequestBodySizeLimitFilter filter;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private FilterChain chain;
  private StringWriter responseBody;

  @Before
  public void setUp() throws Exception {
    filter = new RequestBodySizeLimitFilter();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    chain = mock(FilterChain.class);
    responseBody = new StringWriter();
    when(response.getWriter())
        .thenReturn(new PrintWriter(responseBody));
  }

  @Test
  public void testNormalRequest() throws Exception {
    when(request.getContentLength()).thenReturn(1024);
    filter.doFilter(request, response, chain);
    verify(chain).doFilter(request, response);
  }

  @Test
  public void testOversizedRequest() throws Exception {
    int limit = CommonParameter.getInstance()
        .getMaxHttpRequestBodySize();
    when(request.getContentLength()).thenReturn(limit + 1);
    filter.doFilter(request, response, chain);
    verify(response).setStatus(
        HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
    verify(chain, never()).doFilter(request, response);
    Assert.assertTrue(
        responseBody.toString().contains("request body too large"));
  }

  @Test
  public void testExactLimit() throws Exception {
    int limit = CommonParameter.getInstance()
        .getMaxHttpRequestBodySize();
    when(request.getContentLength()).thenReturn(limit);
    filter.doFilter(request, response, chain);
    verify(chain).doFilter(request, response);
  }

  @Test
  public void testMissingContentLength() throws Exception {
    when(request.getContentLength()).thenReturn(-1);
    filter.doFilter(request, response, chain);
    verify(chain).doFilter(request, response);
  }
}

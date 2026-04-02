package org.tron.core.services.filter;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.parameter.CommonParameter;

@Slf4j(topic = "API")
public class RequestBodySizeLimitFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) {
    try {
      if (request instanceof HttpServletRequest) {
        HttpServletRequest httpReq = (HttpServletRequest) request;
        int maxBodySize = CommonParameter.getInstance()
            .getMaxHttpRequestBodySize();
        if (maxBodySize > 0 && httpReq.getContentLength() > maxBodySize) {
          HttpServletResponse resp = (HttpServletResponse) response;
          resp.setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
          resp.setContentType("application/json; charset=utf-8");
          resp.getWriter().println(
              "{\"Error\":\"request body too large, limit is "
                  + maxBodySize + " bytes\"}");
          return;
        }
      }
      chain.doFilter(request, response);
    } catch (Exception e) {
      logger.error("RequestBodySizeLimitFilter exception: {}",
          e.getMessage());
    }
  }

  @Override
  public void destroy() {
  }
}

package org.tron.core.services.jsonrpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.jsonrpc4j.HttpStatusCodeProvider;
import com.googlecode.jsonrpc4j.JsonRpcInterceptor;
import com.googlecode.jsonrpc4j.JsonRpcServer;
import com.googlecode.jsonrpc4j.ProxyUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.services.filter.BufferedResponseWrapper;
import org.tron.core.services.filter.CachedBodyRequestWrapper;
import org.tron.core.services.http.RateLimiterServlet;

@Component
@Slf4j(topic = "API")
public class JsonRpcServlet extends RateLimiterServlet {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int ERR_BATCH_TOO_LARGE = -32600;
  private static final int ERR_RESPONSE_TOO_LARGE = -32003;
  private static final int ERR_TIMEOUT = -32002;

  private static final ExecutorService RPC_EXECUTOR =
      new ThreadPoolExecutor(4, 16, 60L, TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(256),
          r -> {
            Thread t = new Thread(r, "jsonrpc-worker");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  private JsonRpcServer rpcServer = null;

  @Autowired
  private TronJsonRpc tronJsonRpc;

  @Autowired
  private JsonRpcInterceptor interceptor;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    Object compositeService = ProxyUtil.createCompositeServiceProxy(
        cl,
        new Object[] {tronJsonRpc},
        new Class[] {TronJsonRpc.class},
        true);

    rpcServer = new JsonRpcServer(compositeService);
    rpcServer.setErrorResolver(JsonRpcErrorResolver.INSTANCE);

    HttpStatusCodeProvider httpStatusCodeProvider =
        new HttpStatusCodeProvider() {
          @Override
          public int getHttpStatusCode(int resultCode) {
            return 200;
          }

          @Override
          public Integer getJsonRpcCode(int httpStatusCode) {
            return null;
          }
        };
    rpcServer.setHttpStatusCodeProvider(httpStatusCodeProvider);

    rpcServer.setShouldLogInvocationErrors(false);
    if (CommonParameter.getInstance().isMetricsPrometheusEnable()) {
      rpcServer.setInterceptorList(
          Collections.singletonList(interceptor));
    }
  }

  @Override
  protected void doPost(HttpServletRequest req,
      HttpServletResponse resp) throws IOException {
    CommonParameter parameter = CommonParameter.getInstance();

    byte[] body = readBody(req.getInputStream());

    JsonNode rootNode = MAPPER.readTree(body);
    int maxBatchSize = parameter.getJsonRpcMaxBatchSize();
    if (rootNode.isArray() && maxBatchSize > 0
        && rootNode.size() > maxBatchSize) {
      writeJsonRpcError(resp, ERR_BATCH_TOO_LARGE,
          "Batch size " + rootNode.size()
              + " exceeds limit of " + maxBatchSize, null);
      return;
    }

    int maxResponseSize = parameter.getJsonRpcMaxResponseSize();
    CachedBodyRequestWrapper cachedReq =
        new CachedBodyRequestWrapper(req, body);
    BufferedResponseWrapper bufferedResp =
        new BufferedResponseWrapper(resp, maxResponseSize);

    int timeoutSec = parameter.getJsonRpcMaxRequestTimeout();
    try {
      RPC_EXECUTOR.submit(() -> {
        try {
          rpcServer.handle(cachedReq, bufferedResp);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).get(timeoutSec, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      JsonNode idNode =
          !rootNode.isArray() ? rootNode.get("id") : null;
      writeJsonRpcError(resp, ERR_TIMEOUT,
          "Request timeout after " + timeoutSec + "s", idNode);
      return;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException
          && cause.getCause()
          instanceof BufferedResponseWrapper
              .ResponseTooLargeException) {
        JsonNode idNode =
            !rootNode.isArray() ? rootNode.get("id") : null;
        writeJsonRpcError(resp, ERR_RESPONSE_TOO_LARGE,
            cause.getCause().getMessage(), idNode);
        return;
      }
      throw new IOException("RPC execution failed", cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("RPC interrupted", e);
    }

    byte[] responseBytes = bufferedResp.toByteArray();
    resp.setContentLength(responseBytes.length);
    resp.getOutputStream().write(responseBytes);
    resp.getOutputStream().flush();
  }

  private byte[] readBody(InputStream in) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] tmp = new byte[4096];
    int n;
    while ((n = in.read(tmp)) != -1) {
      buffer.write(tmp, 0, n);
    }
    return buffer.toByteArray();
  }

  private void writeJsonRpcError(HttpServletResponse resp, int code,
      String message, JsonNode id) throws IOException {
    String idStr =
        (id != null && !id.isNull() && !id.isMissingNode())
            ? id.toString() : "null";
    String body = "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":"
        + code + ",\"message\":\"" + message + "\"},\"id\":"
        + idStr + "}";
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    resp.setContentType("application/json");
    resp.setStatus(HttpServletResponse.SC_OK);
    resp.setContentLength(bytes.length);
    resp.getOutputStream().write(bytes);
    resp.getOutputStream().flush();
  }
}

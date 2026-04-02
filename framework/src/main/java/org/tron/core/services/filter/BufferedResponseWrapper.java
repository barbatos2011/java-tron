package org.tron.core.services.filter;

import java.io.ByteArrayOutputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * Buffers the response body so the caller can inspect the size
 * before committing. If maxBytes > 0, writes that push the buffer
 * past maxBytes throw ResponseTooLargeException immediately.
 */
public class BufferedResponseWrapper extends HttpServletResponseWrapper {

  private final ByteArrayOutputStream buffer =
      new ByteArrayOutputStream();
  private final int maxBytes;
  private final ServletOutputStream outputStream =
      new ServletOutputStream() {
        @Override
        public void write(int b) {
          checkLimit(1);
          buffer.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
          checkLimit(len);
          buffer.write(b, off, len);
        }

        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
        }
      };

  public BufferedResponseWrapper(HttpServletResponse response,
      int maxBytes) {
    super(response);
    this.maxBytes = maxBytes;
  }

  private void checkLimit(int incoming) {
    if (maxBytes > 0 && buffer.size() + incoming > maxBytes) {
      throw new ResponseTooLargeException(
          "Response size exceeds the limit of " + maxBytes
              + " bytes");
    }
  }

  @Override
  public ServletOutputStream getOutputStream() {
    return outputStream;
  }

  @Override
  public void setContentLength(int len) {
  }

  @Override
  public void setContentLengthLong(long len) {
  }

  public byte[] toByteArray() {
    return buffer.toByteArray();
  }

  public static class ResponseTooLargeException
      extends RuntimeException {

    public ResponseTooLargeException(String message) {
      super(message);
    }
  }
}

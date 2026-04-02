package org.tron.core.services.http;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Set;
import org.junit.Test;

public class RateLimiterWhitelistTest {

  @SuppressWarnings("unchecked")
  private Set<String> getAllowedAdapters() throws Exception {
    Field field = RateLimiterServlet.class
        .getDeclaredField("ALLOWED_ADAPTERS");
    field.setAccessible(true);
    return (Set<String>) field.get(null);
  }

  @Test
  public void testAllowedAdapters() throws Exception {
    Set<String> allowed = getAllowedAdapters();
    assertTrue(allowed.contains("GlobalPreemptibleAdapter"));
    assertTrue(allowed.contains("QpsRateLimiterAdapter"));
    assertTrue(allowed.contains("IPQPSRateLimiterAdapter"));
    assertTrue(allowed.contains("DefaultBaseQqsAdapter"));
  }

  @Test
  public void testUnknownAdapterBlocked() throws Exception {
    Set<String> allowed = getAllowedAdapters();
    assertFalse(allowed.contains("EvilAdapter"));
    assertFalse(allowed.contains("java.lang.Runtime"));
  }
}

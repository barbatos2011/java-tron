package org.tron.keystore;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.tron.common.crypto.SignInterface;
import org.tron.common.crypto.SignUtils;
import org.tron.common.crypto.sm2.SM2;
import org.tron.common.utils.ByteUtil;

@Slf4j
public class CredentialsTest {

  @Test
  public void testCreate() throws NoSuchAlgorithmException {
    Credentials credentials = Credentials.create(SignUtils.getGeneratedRandomSign(
        SecureRandom.getInstance("NativePRNG"), true));
    Assert.assertTrue("Credentials address create failed!",
        credentials.getAddress() != null && !credentials.getAddress().isEmpty());
    Assert.assertNotNull("Credentials cryptoEngine create failed",
        credentials.getSignInterface());
  }

  @Test
  public void testCreateFromSM2() {
    try {
      Credentials.create(SM2.fromNodeId(ByteUtil.hexToBytes("fffffffffff"
          + "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
          + "fffffffffffffffffffffffffffffffffffffff")));
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testEquals() throws NoSuchAlgorithmException {
    Credentials credentials1 = Credentials.create(SignUtils.getGeneratedRandomSign(
        SecureRandom.getInstance("NativePRNG"), true));
    Credentials credentials2 = Credentials.create(SignUtils.getGeneratedRandomSign(
        SecureRandom.getInstance("NativePRNG"), true));
    Assert.assertFalse("Credentials instance should be not equal!",
        credentials1.equals(credentials2));
  }

  @Test
  public void testEqualityWithMocks() {
    Object aObject = new Object();
    SignInterface si = Mockito.mock(SignInterface.class);
    SignInterface si2 = Mockito.mock(SignInterface.class);
    SignInterface si3 = Mockito.mock(SignInterface.class);
    byte[] address = "TQhZ7W1RudxFdzJMw6FvMnujPxrS6sFfmj".getBytes();
    byte[] address2 = "TNCmcTdyrYKMtmE1KU2itzeCX76jGm5Not".getBytes();
    Mockito.when(si.getAddress()).thenReturn(address);
    Mockito.when(si2.getAddress()).thenReturn(address);
    Mockito.when(si3.getAddress()).thenReturn(address2);
    Credentials aCredential = Credentials.create(si);
    Assert.assertFalse(aObject.equals(aCredential));
    Assert.assertFalse(aCredential.equals(aObject));
    Assert.assertFalse(aCredential.equals(null));
    Credentials anotherCredential = Credentials.create(si);
    Assert.assertTrue(aCredential.equals(anotherCredential));
    Credentials aCredential2 = Credentials.create(si2);
    // si and si2 are different mock objects, so credentials are not equal
    Assert.assertFalse(aCredential.equals(aCredential2));
    Credentials aCredential3 = Credentials.create(si3);
    Assert.assertFalse(aCredential.equals(aCredential3));
  }
}

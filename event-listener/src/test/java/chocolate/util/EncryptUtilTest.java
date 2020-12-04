package chocolate.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by jialili1 on 8/25/20
 */
public class EncryptUtilTest {

  private long rawID = 1234567890L;
  private long encryptedID = 44152383423L;

  @Test
  public void testDecryptUserId() {
    assertEquals(encryptedID, EncryptUtil.encryptUserId(rawID));
    assertEquals(rawID, EncryptUtil.decryptUserId(EncryptUtil.encryptUserId(rawID)));
  }

  @Test
  public void testEncryptUserId() {
    assertEquals(rawID, EncryptUtil.decryptUserId(encryptedID));
    assertEquals(encryptedID, EncryptUtil.encryptUserId(EncryptUtil.decryptUserId(encryptedID)));
  }
}

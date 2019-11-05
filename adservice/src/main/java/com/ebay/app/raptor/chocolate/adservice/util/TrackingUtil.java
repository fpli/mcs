package com.ebay.app.raptor.chocolate.adservice.util;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class TrackingUtil {

  private static final int MAX_KEY_LENGTH = 16;

  /**
   * Decrypts the given string
   * 
   * @param key
   *            - Key to be used for key generation.
   * @param encrypted
   *            - cleartext of encrypted text
   * @return - encrypted string
   * @throws Exception
   */
  public static String decrypt(String key, String encrypted) throws Exception {
    byte[] rawKey = getRawKey(key.getBytes());
    byte[] enc = toByte(encrypted);
    byte[] result = decrypt(rawKey, enc);
    return new String(result);
  }

  private static byte[] getRawKey(byte[] seed) {
    return padKeyToLength(seed, MAX_KEY_LENGTH);
  }

  private static byte[] padKeyToLength(byte[] key, int len) {
    byte[] newKey = new byte[len];
    System.arraycopy(key, 0, newKey, 0, Math.min(key.length, len));
    for (int i = key.length; i < len; i++)
      newKey[i] = key[key.length - 1];
    return newKey;
  }

  private static byte[] decrypt(byte[] raw, byte[] encrypted) throws Exception {
    SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
    Cipher cipher = Cipher.getInstance("AES");
    cipher.init(Cipher.DECRYPT_MODE, skeySpec);
    byte[] decrypted = cipher.doFinal(encrypted);
    return decrypted;
  }

  private static byte[] toByte(String hexString) {
    int len = hexString.length() / 2;
    byte[] result = new byte[len];
    for (int i = 0; i < len; i++)
      result[i] = Integer.valueOf(hexString.substring(2 * i, 2 * i + 2), 16).byteValue();
    return result;
  }

}

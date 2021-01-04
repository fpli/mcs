package com.ebay.app.raptor.chocolate.util;

/**
 * Created by jialili1 on 8/25/20
 */
public class EncryptUtil {
  private static final long LOW_COMPLEMENT = 238675309L;
  private static final long HIGH_COMPLEMENT = 10L;
  private static final long LOW_WORD = 0x00000000FFFFFFFFL;

  /**
   * Encrypts an ID.
   * The method is 'symmetrical' - decrypting encrypted ID returns original decrypted ID.
   */
  public static long encryptUserId(long decryptedUserId) {
    long high = ((decryptedUserId >> 32) ^ HIGH_COMPLEMENT) << 32;
    long low = (decryptedUserId & LOW_WORD) ^ LOW_COMPLEMENT;
    long result = low | high;
    return result;
  }

  /**
   * Decrypts an ID.
   * The method is 'symmetrical' - encrypting decrypted ID returns original encrypted ID.
   */
  public static long decryptUserId(long encryptedUserId) {
    long low = encryptedUserId & LOW_WORD;
    low = low ^ LOW_COMPLEMENT;
    long high = (encryptedUserId >> 32) & LOW_WORD;
    high = high ^ HIGH_COMPLEMENT;
    high <<= 32;
    return low | high;
  }

}

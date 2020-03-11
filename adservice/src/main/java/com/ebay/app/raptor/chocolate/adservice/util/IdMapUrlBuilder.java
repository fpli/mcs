package com.ebay.app.raptor.chocolate.adservice.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * @author rghosh
 *
 */
public class IdMapUrlBuilder {

  public static final String HASH_ALGO_SHA_256 = "SHA-256";

  public static String hashData(String data, String algorithm) {
    try {
      MessageDigest digest = MessageDigest.getInstance(algorithm);
      digest.reset();
      byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
      return UrlSafeBase64.encode(hash, false);
    } catch (Exception nse) {
    }
    return null;
  }

}

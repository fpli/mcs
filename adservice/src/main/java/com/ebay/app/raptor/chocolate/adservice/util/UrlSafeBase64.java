package com.ebay.app.raptor.chocolate.adservice.util;

/*
 * The difference from original Base64 -
 * 1. uses - and _ instead of + and / for encoding so that it is url safe
 * 2. uses * as the ending charactertead of =, to make it url safe
 * 3. for decoding, supports either standard Base64 encoded string or the UrlSafeBase64 encoded string
 */
public class UrlSafeBase64 {

  /**
   * returns a String of base64-encoded characters to represent the passed
   * data array.
   * 
   * @param data - the array of bytes to encode
   * @return base64-coded String.
   */
  static public String encode(byte[] data) {
    char[] out = new char[((data.length + 2) / 3) * 4];
    alphabet[64] = endingCharAsterisk;
    //
    // 3 bytes encode to 4 chars. Output is always an even
    // multiple of 4 characters.
    //

    for (int i = 0, index = 0; i < data.length; i += 3, index += 4) {
      boolean quad = false;
      boolean trip = false;
      int val = (0xFF & (int) data[i]);

      val <<= 8;
      if ((i + 1) < data.length) {
        val |= (0xFF & (int) data[i + 1]);
        trip = true;
      }
      val <<= 8;
      if ((i + 2) < data.length) {
        val |= (0xFF & (int) data[i + 2]);
        quad = true;
      }

      out[index + 3] = alphabet[(quad ? (val & 0x3F) : 64)];
      val >>= 6;
      out[index + 2] = alphabet[(trip ? (val & 0x3F) : 64)];
      val >>= 6;
      out[index + 1] = alphabet[val & 0x3F];
      val >>= 6;
      out[index + 0] = alphabet[val & 0x3F];
    }
    return new String(out);
  }

  static public String encode(byte[] data, boolean padding) {
    String encodedStr = encode(data);
    if (!padding) {
      if (encodedStr != null && encodedStr.length() == 44) {
        return encodedStr.substring(0, 43);
      }
    }
    return encodedStr;
  }

  // eBay uses non-statndard ending character '*' instead of '='
  private static char endingCharAsterisk = '*';

  private static char endingCharEqual = '=';

  //
  // code characters for values 0..63
  //

  private static char[] alphabet = new char[65];
  static {
    for (int i = 'A'; i <= 'Z'; i++) {
      alphabet[i - 'A'] = (char) i;
    }

    for (int i = 'a'; i <= 'z'; i++) {
      alphabet[(26 + i - 'a')] = (char) i;
    }

    for (int i = '0'; i <= '9'; i++) {
      alphabet[(52 + i - '0')] = (char) i;
    }
    alphabet[62] = '-';//'+';
    alphabet[63] = '_';//'/';
  }

  //
  // lookup table for converting base64 characters to value in range 0..63
  //

  static private byte[] codes = new byte[256];
  static {
    for (int i = 0; i < 256; i++) {
      codes[i] = -1;
    }

    for (int i = 'A'; i <= 'Z'; i++) {
      codes[i] = (byte) (i - 'A');
    }

    for (int i = 'a'; i <= 'z'; i++) {
      codes[i] = (byte) (26 + i - 'a');
    }

    for (int i = '0'; i <= '9'; i++) {
      codes[i] = (byte) (52 + i - '0');
    }
    codes['-'] = 62;
    codes['_'] = 63;
    //add duplicate mappings for the safe decoding
    codes['+'] = 62;
    codes['/'] = 63;
  }
}

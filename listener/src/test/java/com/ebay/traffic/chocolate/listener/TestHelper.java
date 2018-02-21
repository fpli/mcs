package com.ebay.traffic.chocolate.listener;

import java.nio.charset.Charset;

public class TestHelper {
    /** Generic message */
    public static byte[] entry = ("Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do " +
            "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, " +
            "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. " +
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu " +
            "fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa " +
            "qui officia deserunt mollit anim id est laborum.").getBytes(Charset.defaultCharset()); // 446 bytes;

    /** Return a positive long */
    public static long positiveLong() {
        return (long) (Math.random() * Long.MAX_VALUE - 10);
    }

}

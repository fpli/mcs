package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.lang3.Validate;

import java.util.Arrays;

/**
 * Wrapper class for the byte array, so we can actually use a byte array as map keys and such.
 * 
 * @author jepounds
 */
public class ByteArrayWrapper {
    // The backing byte array to use.
    private final byte [] bytes;

    /**
     * Ctor
     * 
     * @pre no null inputs
     * @param bytes the bytes to wrap
     */
    private ByteArrayWrapper(final byte [] bytes) {
        Validate.notNull(bytes, "Bytes cannot be null");
        this.bytes = bytes;
    }

    /**
     * Factory ctor
     * 
     * @pre bytes cannot be null
     * @return a wrapped array of bytes
     */
    public static ByteArrayWrapper wrap(final byte [] bytes) {
        return new ByteArrayWrapper(bytes);
    }

    /**
     * @return the wrapped bytes
     */
    public byte [] getBytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other instanceof ByteArrayWrapper) { return Arrays.equals(((ByteArrayWrapper) other).bytes, bytes); }

        /*
         * if (other instanceof byte []) { return Arrays.equals((byte[])other, bytes); }
         */

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}

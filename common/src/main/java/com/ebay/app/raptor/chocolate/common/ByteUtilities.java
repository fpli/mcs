package com.ebay.app.raptor.chocolate.common;

/**
 * Helper utilities for manipulating byte strings
 * 
 * @author jepounds
 */
public class ByteUtilities
{
    /**
     * @param bytes to generate a debug print for
     * @return the debug string representation
     */
    public static String toDebugString(final byte [] bytes)
    {
        if (bytes == null) return "null";
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < bytes.length; ++i)
        {
            if (i != 0 && (i % 8) == 0) sb.append("| ");
            sb.append((int) (bytes[i] & 0xff)).append(' ');
        }
        sb.append("len=" + bytes.length);
        return sb.toString();
    }

    /**
     * Compares one byte string to another
     * 
     * @pre does not support nulls
     * @param left to compare
     * @param right to compare against
     * @return the comparison value.
     */
    public static int compareBytes(byte [] left, byte [] right)
    {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++)
        {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) { return a - b; }
        }
        return left.length - right.length;
    }

    private ByteUtilities()
    {
        /** noop */
    }
}
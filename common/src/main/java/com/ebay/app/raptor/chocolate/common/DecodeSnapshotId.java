/**
 * 
 */
package com.ebay.app.raptor.chocolate.common;

/**
 * Utility for decoding snapshot IDs into human readable form. 
 * 
 * @author jepounds
 */
public class DecodeSnapshotId {

    /**
     * @param args to use in decoding. 
     */
    public static void main(String [] args) {
        for (String arg : args) {
            Long representation = Long.parseLong(arg);
            System.out.println("arg=" + arg + " translation=" + new SnapshotId(representation.longValue()).toDebugString());
        }
    }
}

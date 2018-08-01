package com.ebay.app.raptor.chocolate.common;

import java.io.Serializable;

/**
 * Simple boxed long implementation, useful for callbacks and such.
 * 
 * @author jepounds
 */
public class BoxedLong implements Serializable {
    // Hack for Spark hack.
    private static final long serialVersionUID = -3529326319429352191L;

    // The value we're boxing in.
    private long boxed;

    /**
     * Ctor, initializes to 0.
     */
    public BoxedLong() {
        boxed = 0l;
    }

    /**
     * Changes the value of long by this much.
     * 
     * @param increment to change the value by
     */
    public void increment(long increment) {
        boxed += increment;
    }

    /**
     * @return the value being boxed.
     */
    public long getValue() {
        return boxed;
    }

    @Override
    public String toString() {
        return Long.toString(boxed);
    }
}

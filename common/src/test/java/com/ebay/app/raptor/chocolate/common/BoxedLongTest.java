package com.ebay.app.raptor.chocolate.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("javadoc")
public class BoxedLongTest {

    @Test
    public void testSimple() {
        BoxedLong boxed = new BoxedLong();
        assertEquals(0l, boxed.getValue());
        boxed.increment(-5l);
        assertEquals(-5l, boxed.getValue());
        boxed.increment(-10l);
        assertEquals(-15l, boxed.getValue());
        boxed.increment(20l);
        assertEquals(5l, boxed.getValue());
    }
}

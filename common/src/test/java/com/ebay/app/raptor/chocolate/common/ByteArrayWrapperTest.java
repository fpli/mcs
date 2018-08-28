package com.ebay.app.raptor.chocolate.common;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

@SuppressWarnings("javadoc")
public class ByteArrayWrapperTest {

    @Test(expected = NullPointerException.class)
    public void testNull() {
        ByteArrayWrapper.wrap(null);
    }

    @Test
    public void testWrapper() {
        byte [] b1 = { 0x33, 0x34, 0x35 };
        byte [] b2 = { 0x33, 0x34, 0x35 };
        byte [] b3 = { 0x33, 0x34, 0x36 };

        ByteArrayWrapper same1 = ByteArrayWrapper.wrap(b1);
        ByteArrayWrapper same2 = ByteArrayWrapper.wrap(b2);
        ByteArrayWrapper diff = ByteArrayWrapper.wrap(b3);
        assertEquals(same1, same2);
        assertEquals(same2, same2);
        assertEquals(same1, same1);
        assertEquals(same1.hashCode(), same2.hashCode());
        assertNotEquals(same1, null);
        assertNotEquals(same1.hashCode(), diff.hashCode());

        Set<ByteArrayWrapper> set = new HashSet<ByteArrayWrapper>();
        set.add(same1);
        assertTrue(set.contains(same1));
        assertTrue(set.contains(same2));
        assertFalse(set.contains(diff));
        assertEquals(1, set.size());

        set.add(same2);
        assertTrue(set.contains(same1));
        assertTrue(set.contains(same2));
        assertFalse(set.contains(diff));
        assertEquals(1, set.size());

        set.add(diff);
        assertTrue(set.contains(same1));
        assertTrue(set.contains(same2));
        assertTrue(set.contains(diff));
        assertEquals(2, set.size());
    }
}

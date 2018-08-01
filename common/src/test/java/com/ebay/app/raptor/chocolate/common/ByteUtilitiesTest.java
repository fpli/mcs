package com.ebay.app.raptor.chocolate.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test for byte string utilities
 * 
 * @author jepounds
 */
@SuppressWarnings("javadoc")
public class ByteUtilitiesTest
{
    @Test
    public void testComparison() {
        // Equals check
        assertTrue(0 == ByteUtilities.compareBytes(new byte [] {0x0, 0x0, 0x0}, new byte [] {0x0, 0x0, 0x0}));
        
        // Longer-than check
        assertTrue(0 > ByteUtilities.compareBytes(new byte [] {0x0, 0x0, 0x0}, new byte [] {0x0, 0x0, 0x0, 0x0}));
        
        // Shorter-than check
        assertTrue(0 < ByteUtilities.compareBytes(new byte [] {0x0, 0x0, 0x0}, new byte [] {0x0, 0x0}));
        
        // Individual byte check
        assertTrue(0 > ByteUtilities.compareBytes(new byte [] {0x0}, new byte [] {0xF}));
        assertTrue(0 < ByteUtilities.compareBytes(new byte [] {0xF}, new byte [] {0x0}));
    }
    
    @Test
    public void testToDebugString()
    {
        {
            byte [] b = null;
            assertEquals("null", ByteUtilities.toDebugString(b));
        }
        {
            byte [] b = {};
            assertEquals("len=0", ByteUtilities.toDebugString(b));
        }
        {
            byte [] b = {0x01};
            assertEquals("1 len=1", ByteUtilities.toDebugString(b));
        }
        {
            byte [] b = {0x03, (byte) 0xFF};
            assertEquals("3 255 len=2", ByteUtilities.toDebugString(b));
        }
        {
            byte [] b = {0x03, (byte) 0xFF, 0x55, 0x01, 0x02, 0x03, 0x01, 0x02, 0x03, 0x01, 0x02, 0x03, 0x01, 0x02,
                    0x03, 0x40};
            assertEquals("3 255 85 1 2 3 1 2 | 3 1 2 3 1 2 3 64 len=16", ByteUtilities.toDebugString(b));
        }
        {
            byte [] b = {0x03, (byte) 0xFF, 0x55, 0x01, 0x02, 0x03, 0x01, 0x02, 0x03, 0x01, 0x02, 0x03, 0x01, 0x02,
                    0x03, 0x40, 0x0A};
            assertEquals("3 255 85 1 2 3 1 2 | 3 1 2 3 1 2 3 64 | 10 len=17", ByteUtilities.toDebugString(b));
        }
    }
}
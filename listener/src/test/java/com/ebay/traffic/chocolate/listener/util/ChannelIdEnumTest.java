package com.ebay.traffic.chocolate.listener.util;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("javadoc")
public class ChannelIdEnumTest {

    @Test
    public void testChannels() {
        assertNull(ChannelIdEnum.parse(null));
        assertNull(ChannelIdEnum.parse(""));
        assertNull(ChannelIdEnum.parse("23482"));
        assertNull(ChannelIdEnum.parse("3"));
        assertEquals(ChannelIdEnum.EPN, ChannelIdEnum.parse("1"));
        assertEquals(ChannelIdEnum.NINE, ChannelIdEnum.parse("9"));
        
        assertTrue(ChannelIdEnum.NINE.isTestChannel());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.NINE.getLogicalChannel());
        assertEquals("9", ChannelIdEnum.NINE.getValue());
        
        assertFalse(ChannelIdEnum.EPN.isTestChannel());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.EPN.getLogicalChannel());
        assertEquals("1", ChannelIdEnum.EPN.getValue());

        assertTrue(LogicalChannelEnum.EPN.isValidRoverAction(ChannelActionEnum.CLICK));
        assertFalse(LogicalChannelEnum.EPN.isValidRoverAction(ChannelActionEnum.parse(ChannelIdEnum.EPN,"roverroi")));
        assertEquals("DISPLAY", LogicalChannelEnum.DISPLAY.getAvro().name());

    }
}

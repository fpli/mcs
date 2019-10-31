package com.ebay.app.raptor.chocolate.constant;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

@SuppressWarnings("javadoc")
public class ChannelActionEnumTest {
    
    @Test
    public void testEnum() {
        
        assertTrue(Arrays.equals(new String[] {"1i", "roverimp", "ar"} , ChannelActionEnum.IMPRESSION.getActions()));
        assertTrue(Arrays.equals(new String[] {"1c", "rover"} , ChannelActionEnum.CLICK.getActions()));
        assertTrue(Arrays.equals(new String[] {"1v"} , ChannelActionEnum.VIMP.getActions()));
        assertTrue(Arrays.equals(new String[] {"1p"} , ChannelActionEnum.PAGE_IMP.getActions()));
        assertTrue(Arrays.equals(new String[] {"1s"} , ChannelActionEnum.SERVE.getActions()));
        assertTrue(Arrays.equals(new String[] {"1r"} , ChannelActionEnum.VIEW_ITEM.getActions()));
        assertTrue(Arrays.equals(new String[] {"1d"} , ChannelActionEnum.VIEW_TIME.getActions()));
        assertTrue(Arrays.equals(new String[] {"1m"} , ChannelActionEnum.APP_FIRST_START.getActions()));

        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, null));
        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, ""));
        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, "rove"));
        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, "roveri"));
        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, "roverimp2"));
        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, "v12"));
        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, "v"));
        assertNull(ChannelActionEnum.parse(ChannelIdEnum.EPN, "1"));

        assertEquals(ChannelActionEnum.CLICK, ChannelActionEnum.parse(ChannelIdEnum.EPN, "rover"));
        assertEquals(ChannelActionEnum.CLICK, ChannelActionEnum.parse(ChannelIdEnum.EPN, "ROVER"));
        assertEquals(ChannelActionEnum.CLICK, ChannelActionEnum.parse(ChannelIdEnum.EPN, "Rover"));
        assertEquals(ChannelActionEnum.CLICK, ChannelActionEnum.parse(ChannelIdEnum.EPN, "1c"));
        assertEquals(ChannelActionEnum.CLICK, ChannelActionEnum.parse(ChannelIdEnum.EPN, "1C"));
        assertEquals(ChannelAction.CLICK, ChannelActionEnum.CLICK.getAvro());

        assertEquals(ChannelActionEnum.IMPRESSION, ChannelActionEnum.parse(ChannelIdEnum.NINE, "ar"));
        assertEquals(ChannelActionEnum.IMPRESSION, ChannelActionEnum.parse(ChannelIdEnum.NINE,"AR"));
        assertEquals(ChannelActionEnum.IMPRESSION, ChannelActionEnum.parse(ChannelIdEnum.NINE,"roverimp"));
        assertEquals(ChannelActionEnum.IMPRESSION, ChannelActionEnum.parse(ChannelIdEnum.NINE,"ROVERIMP"));
        assertEquals(ChannelActionEnum.IMPRESSION, ChannelActionEnum.parse(ChannelIdEnum.NINE,"RoverImp"));
        assertEquals(ChannelActionEnum.IMPRESSION, ChannelActionEnum.parse(ChannelIdEnum.NINE,"1i"));
        assertEquals(ChannelActionEnum.IMPRESSION, ChannelActionEnum.parse(ChannelIdEnum.NINE,"1I"));
        assertEquals(ChannelAction.IMPRESSION, ChannelActionEnum.IMPRESSION.getAvro());
        
        assertEquals(ChannelActionEnum.VIMP, ChannelActionEnum.parse(ChannelIdEnum.NINE,"1v"));
        assertEquals(ChannelActionEnum.VIMP, ChannelActionEnum.parse(ChannelIdEnum.NINE,"1V"));
        assertEquals(ChannelAction.VIEWABLE, ChannelActionEnum.VIMP.getAvro());
    }
}

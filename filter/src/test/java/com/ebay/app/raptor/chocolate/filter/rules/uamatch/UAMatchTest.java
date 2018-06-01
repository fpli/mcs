package com.ebay.app.raptor.chocolate.filter.rules.uamatch;

import org.junit.Test;

import java.security.InvalidParameterException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by spugach on 11/16/16.
 */
public class UAMatchTest {
    /**
                 Whitelist
     */
    @Test(expected = InvalidParameterException.class)
    public void testWhitelistInit1() {
        TwoParamsListEntry entry = new TwoParamsListEntry("|1|1");          // Empty pattern
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testWhitelistInit2() {
        TwoParamsListEntry entry = new TwoParamsListEntry("foo|1");       // Not enough fields
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testWhitelistInit3() {
        TwoParamsListEntry entry = new TwoParamsListEntry("foo||1");     // Empty param
        assertNotNull(entry);
    }

    @Test
    public void testWhitelistSingleInactive() {
        TwoParamsListEntry entry = new TwoParamsListEntry("foo|0|1");
        assertEquals(false, entry.match("foo"));
    }

    @Test
    public void testWhitelistSingleActiveStart() {
        TwoParamsListEntry entry = new TwoParamsListEntry("foo|1|1");
        assertEquals(true, entry.match("foobar"));
        assertEquals(false, entry.match("bazfoo"));
    }

    @Test
    public void testWhitelistSingleActiveAnyCase() {
        TwoParamsListEntry entry = new TwoParamsListEntry("foo|1|0");
        assertEquals(true, entry.match("foobar"));
        assertEquals(true, entry.match("bazFOO"));
    }

    /**
                Blacklist
     */
    @Test(expected = InvalidParameterException.class)
    public void testBlacklistInit1() {
        FourParamsListEntry entry = new FourParamsListEntry("|0||0|2|0");          // Empty pattern
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testBlacklistInit2() {
        FourParamsListEntry entry = new FourParamsListEntry("bar|0||0|2");         // Not enough fields
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testBlacklistInit3() {
        FourParamsListEntry entry = new FourParamsListEntry("bar|||0|2|0");          // Empty param
        assertNotNull(entry);
    }

    @Test
    public void testBlacklistSingleInactive() {
        FourParamsListEntry entry = new FourParamsListEntry("bar|0||0|2|0");
        assertEquals(false, entry.match("bar"));
    }
    @Test
    public void testBlacklistSingleActiveStart() {
        FourParamsListEntry entry = new FourParamsListEntry("bar|1||0|2|1");
        assertEquals(true, entry.match("barzyx"));
        assertEquals(false, entry.match("bazbar"));
    }

    @Test
    public void testBlacklistSingleActiveAnyCase() {
        FourParamsListEntry entry = new FourParamsListEntry("bar|1||0|2|0");
        assertEquals(true, entry.match("barzyx"));
        assertEquals(true, entry.match("bazBAR"));
    }

    @Test
    public void testBlacklistSingleExceptions() {
        FourParamsListEntry entry1 = new FourParamsListEntry("obot|1||0|2|0");
        FourParamsListEntry entry2 = new FourParamsListEntry("obot|1|robotics|0|2|0");
        FourParamsListEntry entry3 = new FourParamsListEntry("obot|1|robotto-san, Robotics|0|2|0");
        assertEquals(false, entry2.match("SoRobotics!"));
        assertEquals(false, entry3.match("so robotics!"));
    }
}
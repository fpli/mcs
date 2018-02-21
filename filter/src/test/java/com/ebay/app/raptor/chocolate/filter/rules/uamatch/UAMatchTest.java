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
        WhitelistEntry entry = new WhitelistEntry("|1|1");          // Empty pattern
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testWhitelistInit2() {
        WhitelistEntry entry = new WhitelistEntry("foo|1");       // Not enough fields
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testWhitelistInit3() {
        WhitelistEntry entry = new WhitelistEntry("foo||1");     // Empty param
        assertNotNull(entry);
    }

    @Test
    public void testWhitelistSingleInactive() {
        WhitelistEntry entry = new WhitelistEntry("foo|0|1");
        assertEquals(false, entry.match("foo"));
    }

    @Test
    public void testWhitelistSingleActiveStart() {
        WhitelistEntry entry = new WhitelistEntry("foo|1|1");
        assertEquals(true, entry.match("foobar"));
        assertEquals(false, entry.match("bazfoo"));
    }

    @Test
    public void testWhitelistSingleActiveAnyCase() {
        WhitelistEntry entry = new WhitelistEntry("foo|1|0");
        assertEquals(true, entry.match("foobar"));
        assertEquals(true, entry.match("bazFOO"));
    }

    /**
                Blacklist
     */
    @Test(expected = InvalidParameterException.class)
    public void testBlacklistInit1() {
        BlacklistEntry entry = new BlacklistEntry("|0||0|2|0");          // Empty pattern
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testBlacklistInit2() {
        BlacklistEntry entry = new BlacklistEntry("bar|0||0|2");         // Not enough fields
        assertNotNull(entry);
    }

    @Test(expected = InvalidParameterException.class)
    public void testBlacklistInit3() {
        BlacklistEntry entry = new BlacklistEntry("bar|||0|2|0");          // Empty param
        assertNotNull(entry);
    }

    @Test
    public void testBlacklistSingleInactive() {
        BlacklistEntry entry = new BlacklistEntry("bar|0||0|2|0");
        assertEquals(false, entry.match("bar"));
    }
    @Test
    public void testBlacklistSingleActiveStart() {
        BlacklistEntry entry = new BlacklistEntry("bar|1||0|2|1");
        assertEquals(true, entry.match("barzyx"));
        assertEquals(false, entry.match("bazbar"));
    }

    @Test
    public void testBlacklistSingleActiveAnyCase() {
        BlacklistEntry entry = new BlacklistEntry("bar|1||0|2|0");
        assertEquals(true, entry.match("barzyx"));
        assertEquals(true, entry.match("bazBAR"));
    }

    @Test
    public void testBlacklistSingleExceptions() {
        BlacklistEntry entry1 = new BlacklistEntry("obot|1||0|2|0");
        BlacklistEntry entry2 = new BlacklistEntry("obot|1|robotics|0|2|0");
        BlacklistEntry entry3 = new BlacklistEntry("obot|1|robotto-san, Robotics|0|2|0");
        assertEquals(true, entry1.match("So robotics!"));
        assertEquals(false, entry2.match("So robotics!"));
        assertEquals(false, entry3.match("So Robotics!"));
    }
}

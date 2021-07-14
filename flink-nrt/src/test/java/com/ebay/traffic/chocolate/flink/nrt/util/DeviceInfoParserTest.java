package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by zhofan on 07/02/21
 */
public class DeviceInfoParserTest {

    @Test
    public void testParser() {
        UserAgentInfo agentInfo = new UserAgentParser().parse("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;" +
                "no-carrier;414x736;3.0");
        DeviceInfoParser parser = new DeviceInfoParser().parse(agentInfo);

        assertEquals("Other", parser.getDeviceFamily());
        assertEquals("iOS", parser.getDeviceType());
        assertNull(parser.getBrowserFamily());
        assertNull(parser.getBrowserVersion());
        assertEquals("iOS", parser.getOsFamily());
        assertEquals("11.2", parser.getOsVersion());
    }
}

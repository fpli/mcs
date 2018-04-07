package com.ebay.traffic.chocolate.mkttracksvc.snid;

import com.ebay.traffic.chocolate.mkttracksvc.util.Hostname;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("javadoc")
public class HostnameTest {
    @Test
    public void testHostname() {
        String hostname = Hostname.HOSTNAME;
        assertTrue(hostname != null && hostname.length() > 0);
        System.out.println("got hostname=" + hostname);
    }

    @Test
    public void testPid() {
        int pid = Hostname.PID;
        assertTrue(pid > 0);
        System.out.println("got pid=" + pid);
    }

    @Test
    public void testIp() {
        String ip = Hostname.IP;
        assertTrue(ip != null && ip.length() > 0);
        assertTrue(Hostname.IP_DECIMAL > 0l);
        assertEquals(Hostname.ipToDecimal(ip), Hostname.IP_DECIMAL);
    }

    @Test
    public void testIpToDecimal() {
        assertEquals(3631325266l, Hostname.ipToDecimal("216.113.160.82"));
        assertEquals(2130706433l, Hostname.ipToDecimal("127.0.0.1"));

        // The infamous shortened URI format
        assertEquals(2130706433l, Hostname.ipToDecimal("127.0.1"));
        assertEquals(2130706433l, Hostname.ipToDecimal("127.1"));

        // Should accept decimal string
        assertEquals(2130706433l, Hostname.ipToDecimal("2130706433"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testIpToDecimalNonnumeric() {
        Hostname.ipToDecimal("aldfjalsfj");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testIpToDecimalBadFormat() {
        Hostname.ipToDecimal("127.0..1");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testIpToDecimalNonNumeric() {
        Hostname.ipToDecimal("127.0.a.1");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testIpToDecimalTooManyAtoms() {
        Hostname.ipToDecimal("127.0.1.1.1");
    }
}



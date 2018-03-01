package com.ebay.traffic.chocolate.mkttracksvc.snid;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Return the "hostname -f" discovered via Linux, with an option for platform independence should we ever need it
 * 
 * @author jialili
 */
public class Hostname {
    /** Singleton hostname to set for this machine */
    public static final String HOSTNAME = getHostname();

    /** Singleton IP address for this machine */
    public static final String IP = getIp();
    
    /** Singleton decimal IP for this machine */
    public static final long IP_DECIMAL = ipToDecimal(IP);

    /** Singleton PID to set for this machine */
    public static final int PID = getPid();

    // Logging utility
    private static final Logger logger = Logger.getLogger(Hostname.class);
    
    /** @return the IP address of the current machine */
    public static String getIp()  {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            logger.fatal("Failed to get IP", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * IP address lookup conversion. 
     * 
     * @param ipAddress
     * @return the decimal value thereof
     */
    public static long ipToDecimal(String ipAddress) {
        // For a pure numeric value, just 
        if (StringUtils.isNumeric(ipAddress))
            return Long.parseLong(ipAddress);
        String[] atoms = ipAddress.split("\\.");
        Validate.isTrue(atoms.length >= 2 && atoms.length <= 4, "2,3,4 atoms must be present");
        for (String atom : atoms) Validate.isTrue(StringUtils.isNumeric(atom), "Atom must be numeric");
        
        // Start with the last atom. 
        long result = Long.parseLong(atoms[atoms.length-1]);
        
        // Go from the rest of the atoms.
        int shift = 3;
        for (int i = 0; i < (atoms.length-1); ++i) {
            result |= Long.parseLong(atoms[i]) << (shift-- * 8);
        }

        return result & 0xFFFFFFFF;
    }

    /**
     * Private method to calculate a hostname.
     * 
     * @return the hostname
     * @throws IOException if an error occurs in fetching a hostname
     */
    private static String getHostname() {
        try {
            if (SystemUtils.IS_OS_UNIX) {
                Process proc;
                proc = Runtime.getRuntime().exec("hostname -f");
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    if (!line.isEmpty()) {
                        in.close();
                        return line.trim();
                    }
                }
                in.close();
            }
            return InetAddress.getLocalHost().getHostName();
        }
        catch (IOException e) {
            logger.error("Caught exception", e);
            Validate.isTrue(false, "Must get a hostname from here on");
            throw new RuntimeException(e);
        }
    }

    private static int getPid() {
        /* This works with Oracle JVM on multiple OSes, but may not work with other JVM implementations */
        return Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
    }
}

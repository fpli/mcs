package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Enumeration;
import java.util.IllegalFormatConversionException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for parsing application options
 * 
 * @author jepounds
 */
public class ApplicationOptionsParser {

    /** Private logging instance */
    private static final Logger logger = Logger.getLogger(ApplicationOptionsParser.class);

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    static final int RANDOM_DRIVER_ID_MASK =
            SECURE_RANDOM.nextInt(Long.valueOf(SnapshotId.MAX_DRIVER_ID + 1l).intValue());

    /* don't initialize me, singleton class */
    private ApplicationOptionsParser() { }

    /**
     * Gets a file name for the given property string. 
     * 
     * @pre no null parameters
     * @param properties to use
     * @param property to use
     * @param isPidRequired if true, then there *must* be a %d syntax for PID.
     * @param repoName if non-null, then there must be a %s syntax. False means no %s syntax is allowed.
     * @return the given file name. 
     */
    public static String getFileName(final Properties properties, String property, boolean isPidRequired, String repoName) {
        String pidFile = getStringProperty(properties, property);

        if (isPidRequired && !pidFile.endsWith(".pid")) {
            logger.fatal("Configured file invalid! Pid file needs to end in .pid:" + property + " Got: "
                    + pidFile);
            throw new UnsupportedOperationException(property + " needs to end in .pid. Got: " + pidFile);
        }
        
        boolean hasPid = pidFile.contains("%d");
        if (isPidRequired && !hasPid) {
            logger.fatal("Configured pid file invalid! Pid file needs to contain %d option for PID:" + property
                    + " Got: " + pidFile);
            throw new UnsupportedOperationException(property + " needs %d option. Got: " + pidFile);
        }
        
        String fileName = pidFile;
        boolean hasStringFormat = fileName.contains("%s");
        if (StringUtils.isNotBlank(repoName)) {
            if (!hasStringFormat) {
                logger.fatal("String format %s required in property=" + property + "; got :" + fileName);
                throw new UnsupportedOperationException("String format %s required in property=" + property + "; got :" + fileName);
            }
        } else if (hasStringFormat) {
            logger.fatal("String format not supported for property=" + property);
            throw new UnsupportedOperationException(property + " does not support %s option; got:" + fileName);
        }
        
        if (hasPid && !hasStringFormat) {
            // This should throw an exception if not nicely formatted.
            fileName = String.format(pidFile, Hostname.PID);
        } else if (hasPid && hasStringFormat) {
            try {
                fileName = String.format(pidFile, repoName, Hostname.PID);
            } catch (IllegalFormatConversionException e) {
                fileName = String.format(pidFile, Hostname.PID, repoName);
            }
        } else if (hasStringFormat) {
            fileName = String.format(pidFile, repoName);
        }
        return fileName;
    }
    
    /**
     * Gets a file for the given property string. File must not exist at this point. 
     * 
     * @pre no null parameters
     * @param properties to use
     * @param property to use
     * @param isPidRequired if true, then there *must* be a %d syntax for PID.
     * @param repoName if non-null, then there must be a %s syntax. False means no %s syntax is allowed.
     * @param isTemp return true if this is marked a temp file, false otherwise.
     * @return the given file, 
     * @throws IOException when something bad happens. 
     */
    public static File getFile(final Properties properties, String property, boolean isPidRequired, String repoName, boolean isTemp) throws IOException {
        File file = new File(getFileName(properties, property, isPidRequired, repoName));

        // Now check and make sure we can write to the parent directory.
        File parentDir = null;
        if (file.getCanonicalFile() != null) parentDir = file.getCanonicalFile().getParentFile();
        if (parentDir == null) {
            logger.fatal("Parent directory null");
        }
        else if (!parentDir.canWrite()) {
            logger.fatal("Parent directory=" + parentDir.getCanonicalPath() + " cannot be written to");
            throw new UnsupportedOperationException(
                    "PID parent directory=" + parentDir.getCanonicalPath() + " cannot be written to by process");
        }
        
        if (isTemp) file.deleteOnExit();

        // All set!
        return file;        
    }
    
    /**
     * Debug utility to dump/read properties.
     * 
     * @pre no null parameters
     * @param properties to use in dumping
     */
    public static void dumpProperties(Properties properties) {
        Enumeration<?> e = properties.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            String value = properties.getProperty(key);
            logger.info("Loaded " + key + " value:" + value);
        }
    }
    
    /**
     * Ctor for deriving a numeric property
     * 
     * @pre properties cannot be null
     * @param properties to read from
     * @param property to get
     * @param defaultValue the default value. if Null, then implies this is required property
     * @return the boolean value or throws exception if not found.
     */
    public static boolean getBooleanProperty(final Properties properties, final String property, Boolean defaultValue) {
        if (!properties.containsKey(property)) {
            if (defaultValue == null) {
                logger.fatal(property + " not found in properties file");
                throw new UnsupportedOperationException(property + " not found in properties file!");
            } else {
                logger.info("property " + property + " not found in properties file, assuming default value of=" + defaultValue);
                return defaultValue.booleanValue();
            }
        }
        
        return Boolean.parseBoolean(properties.getProperty(property));
    }
    
    /**
     * Ctor for deriving a numeric property
     * 
     * @pre properties cannot be null
     * @param properties to read from
     * @param property to get
     * @param min this should accommodate, inclusive
     * @param max this should accommodate, inclusive
     * @return the integer value or throws exception if not found.
     */
    public static int getNumericProperty(final Properties properties, final String property, final int min, final int max) {
        Validate.isTrue(min <= max, "Min is not less than max");
        if (!properties.containsKey(property) || !StringUtils.isNumeric(properties.getProperty(property))) {
            logger.fatal(property + " not found in properties file");
            throw new UnsupportedOperationException(property + " not found in properties file!");
        }
        int value = Integer.parseInt(properties.getProperty(property));
        if (value < min || value > max) {
            logger.fatal(
                    property + " has invalid value " + value + "; ranges are [" + min + "-" + max + "]; inclusive");
            throw new UnsupportedOperationException(property + " has invalid value " + value);
        }
        return value;
    }

    /**
     * Dumps properties from external properties file.
     * 
     * @param location   of file to use in loading.
     * @param clazz      should be the class containing the "main" method. 
     */
    @SuppressWarnings("rawtypes")
    public static Properties readExternalProperties(final String location, final Class clazz) throws IOException {
        InputStream input = null;

        // If this Kafka file is handy and exist, we'll use it first. Otherwise, we'll use
        // the embedded Jar value.
        logger.info("Attempting to load properties at " + location);
        File propFile = new File(location);
        if (propFile.exists()) {
            Validate.isTrue(isValidFile(propFile), "Must be a valid file");
            try {
                logger.info("Reading properties file at " + propFile.getCanonicalPath());
                input = new FileInputStream(propFile.getCanonicalPath());
            }
            catch (IOException e) {
                // TODO Auto-generated catch block
                logger.error("Could not read kafka file at " + location, e);
            }
        }

        // If kafka file is still null at this point, attempt to read from the jar.
        if (input == null) {
            logger.info("Could not find Kafka properties file at " + location + "; attempting internal JAR read");
            input = clazz.getClassLoader().getResourceAsStream(location);
        }

        Validate.notNull(input, "Input stream cannot be null at this point");
        Properties prop = new Properties();
        prop.load(input);
        return prop;
    }
    
    /**
     * Dumps properties from Apache-based XML file. I don't know why HDP doesn't use Commons Configuration.
     * 
     * @throws ParserConfigurationException   When we can't parse the XML incorrectly
     * @throws IOException   When an error in opening the XML file occurs
     * @throws SAXException   When there's some other XML parser error
     */
    public static Properties readXmlProperties(final InputStream input) throws IOException {
        Validate.notNull(input, "input stream cannot be null at this point");
        Properties properties = new Properties();

        DocumentBuilder dBuilder;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            dBuilder = dbf.newDocumentBuilder();
        }
        catch (ParserConfigurationException e1) {
            logger.fatal("Error in creating document builder", e1);
            throw new IOException(e1);
        }
        Document doc;
        try {
            doc = dBuilder.parse(input);
        }
        catch (SAXException e) {
            logger.fatal("Error in parsing XML file", e);
            throw new IOException(e);
        }
        Validate.isTrue("configuration".equals(doc.getDocumentElement().getNodeName()),
                "Top XML element should be configuration");
        NodeList propertyNodes = doc.getElementsByTagName("property");

        for (int i = 0; i < propertyNodes.getLength(); ++i) {
            // Get the property node.
            Node n = propertyNodes.item(i);
            if (n.getNodeType() != Node.ELEMENT_NODE) continue;
            Element e = (Element) n;

            // Create the properties.
            String key = e.getElementsByTagName("name").item(0).getTextContent();
            String value = e.getElementsByTagName("value").item(0).getTextContent();
            properties.put(key, value);
        }

        return properties;
    }

    /** Pattern for a regex */
    private static final Pattern NUMBER_PATTERN = Pattern.compile("[0-9]+");
    
    /**
     * Refactored, host-indepedent code to derive a driver ID from the internet / hostname address. 
     * 
     * Here's how it works: 
     * 
     * 1. First, we get the largest number group in the FQDN. We mod that by 1024 and return that. 
     * 2. If there is no number in the FQDN, we mod the decimal IP by 1024 and return that. 
     * 
     * @param hostname   to use as primary source
     * @param ipDecimal  to fall back ono
     * @return the IP value. 
     */
    static int getDriverIdFromIp(String hostname, long ipDecimal) {
        Validate.isTrue(StringUtils.isNotBlank(hostname), "Hostname cannot be null at this point");
        Validate.isTrue(ipDecimal > 0l, "ipDecimal should be valid");

        // Get the largest number group in the FQDN
        long largest = -1l;
        Matcher matcher = NUMBER_PATTERN.matcher(hostname);
        while (matcher.find()) {
            String candidate = matcher.group();
            try {
                long entry = Long.parseLong(candidate);
                largest = Math.max(largest, entry);
            } catch (NumberFormatException e) {
                logger.warn("Could not parse string from hostname=" + hostname + " portion=" + candidate);
            }
        }
        
        // If we have a match, return the maximum value. 
        long numeric = largest >= 0 ? largest : ipDecimal; 
        int driverId = Long.valueOf(numeric % (SnapshotId.MAX_DRIVER_ID+1l)).intValue();
        logger.info("Parsed driver ID from IP=" + driverId);
        return driverId;
    }

    /**
     * driver_id = (decimal_iP / 1024) | RANDOM_DRIVER_ID_MASK
     *
     * @param ipDecimal  decimal ip
     * @return driver id
     */
    static int getDriverIdFromIp(long ipDecimal) {
        Validate.isTrue(ipDecimal > 0l, "ipDecimal should be valid");

        int driverId = Long.valueOf(ipDecimal % (SnapshotId.MAX_DRIVER_ID + 1l)).intValue() & RANDOM_DRIVER_ID_MASK;
        logger.info(String.format("ip: %s random driver id mask: %d driver id: %d", Hostname.getIp(), RANDOM_DRIVER_ID_MASK, driverId));
        return driverId;
    }
    
    /** 
     * Utility function to derive a driver ID, first going from the longest numeric sequence 
     * in the hostname, and falling back on the IP address. 
     */
    public static int getDriverIdFromIp() {
        return getDriverIdFromIp(Hostname.IP_DECIMAL);
    }
    
    /**
     * Tests to make sure a config file is readable, is file, and is not a symlink (SDElements T572 requirement) 
     * @return true iff a file is valid; false otherwise 
     * @throws IOException */
    public static boolean isValidFile(File file) throws IOException {
        Validate.notNull(file, "File cannot be null");
        if (!file.exists()) {
            // Often non-existing is a valid case so we only log INFO here
            logger.info("File " + file.getName() + " does not exist");
            return false;
        }
        
        if (!file.canRead()) {
            // Can't read is an error case.
            logger.error("File " + file.getPath() + " cannot be read");
            return false;
        }
        
        if (!file.isFile()) {
            logger.error("File " + file.getPath() + " is not a file");
            return false;
        }
        
        if (FileUtils.isSymlink(file)) {
            logger.error("File " + file.getPath() + " is a symlink - see SDElements T572");
            logger.error("Canonical path of symlink=" + file.getCanonicalPath());
            return false;
        }
        
        return true;
    }
    /**
     * Utility function to get a property's value
     * 
     * @pre no null property, no blank values
     * @param properties  the properties to use
     * @param property to get
     * @return property that has string value
     */
    public static String getStringProperty(final Properties properties, final String property) {
        if (!properties.containsKey(property)) {
            logger.fatal("Configured property does not exist! Missing property:" + property);
            throw new UnsupportedOperationException(property + " not found in properties file!");
        }
        
        String value = properties.getProperty(property);
        if (StringUtils.isBlank(value)) {
            logger.fatal("Configured property found, but value is null or blank! Required property:" + property);
            throw new UnsupportedOperationException(property + " is blank");
        }
        
        return value;
    }
    
    /**
     * Parse comma-separated values as an array of string.
     * 
     * @pre no null property, no blank values
     * @param properties  the properties to use
     * @param property to get, comma separated
     * @return property that has string array value
     */
    public static String [] getStringArrayProperty(final Properties properties, final String property) {
        // Get the unparsed value to begin with
        String unparsedValue = getStringProperty(properties, property);
        
        // Return the split values.
        return unparsedValue.split(",");
    }
}

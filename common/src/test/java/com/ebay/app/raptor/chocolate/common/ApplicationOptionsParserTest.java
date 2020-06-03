package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * @author jepounds
 */
@SuppressWarnings("javadoc")
public class ApplicationOptionsParserTest {

    @Test(expected=NullPointerException.class)
    public void testValidFileNull() throws IOException {
        ApplicationOptionsParser.isValidFile(null);
    }
    
    @Test
    public void testValidFileNonexistent() throws IOException {
        File file = File.createTempFile("ApplicationOptionsParserTest", Long.toString(SnapshotId.getNext(1).getRepresentation()));
        assertTrue(file.exists());
        assertTrue(file.delete());
        assertFalse(ApplicationOptionsParser.isValidFile(file));
    }
    
    @Test
    public void testValidFileReadable() throws IOException {
        File file = File.createTempFile("ApplicationOptionsParserTest", Long.toString(SnapshotId.getNext(1).getRepresentation()));
        file.deleteOnExit();
        assertTrue(file.exists());
        file.setReadable(false);
        assertFalse(ApplicationOptionsParser.isValidFile(file));
        file.setReadable(true);
    }
    
    @Test
    public void testValidFileDirectory() throws IOException {
        File file = File.createTempFile("ApplicationOptionsParserTest", Long.toString(SnapshotId.getNext(1).getRepresentation()));
        assertTrue(file.delete());
        assertTrue(file.mkdir());
        assertTrue(file.exists());
        assertTrue(file.isDirectory());
        assertFalse(ApplicationOptionsParser.isValidFile(file));
        assertTrue(file.delete());
    }
    
    @Test
    public void testValidFileSymlink() throws IOException {
        File file = File.createTempFile("ApplicationOptionsParserTest", Long.toString(SnapshotId.getNext(1).getRepresentation()));
        file.deleteOnExit();
        File link = File.createTempFile("ApplicationOptionsParserTest", Long.toString(SnapshotId.getNext(1).getRepresentation()));
        link.delete();
        Files.createSymbolicLink(link.toPath(), file.toPath());
        assertTrue(FileUtils.isSymlink(link));
        assertFalse(ApplicationOptionsParser.isValidFile(link));
        assertTrue(ApplicationOptionsParser.isValidFile(file));
        link.delete();
    }
    
    @Test
    public void testValidFileHappy() throws IOException {
        File file = File.createTempFile("ApplicationOptionsParserTest", Long.toString(SnapshotId.getNext(1).getRepresentation()));
        file.deleteOnExit();
        assertTrue(ApplicationOptionsParser.isValidFile(file));
    }
    
    @Test(expected=UnsupportedOperationException.class)
    public void testGetFileEmpty() throws IOException {
        Properties prop = new Properties();
        ApplicationOptionsParser.getFile(prop, "pid_file", true, null, true);
    }
    
    @Test(expected=UnsupportedOperationException.class)
    public void testGetFileInvalid() throws IOException {
        Properties prop = new Properties();
        prop.put("pid_file", " ");
        ApplicationOptionsParser.getFile(prop, "pid_file", true, null, true);
    }
    
    @Test(expected=UnsupportedOperationException.class)
    public void testGetFileNoExt() throws IOException {
        Properties prop = new Properties();
        prop.put("pid_file", "/tmp/not%dending");
        ApplicationOptionsParser.getFile(prop, "pid_file", true, null, true);
    }
    
    @Test(expected=UnsupportedOperationException.class)
    public void testGetFileNoPid() throws IOException {
        Properties prop = new Properties();
        prop.put("pid_file", "/tmp/not.pid");
        ApplicationOptionsParser.getFile(prop, "pid_file", true, null, true);
    }
    
    @Test(expected=UnsupportedOperationException.class)
    public void testGetFileNonpermDir() throws IOException {
        Properties prop = new Properties();
        // I really hope you are not running UTs as root.
        prop.put("pid_file", "/root/chocolate-test-%d.pid");
        ApplicationOptionsParser.getFile(prop, "pid_file", true, null, true);
    }
    
    @Test
    public void testGetFileHappy() throws IOException {
        Properties prop = new Properties();
        prop.put("pid_file", "/tmp/chocolate-common-test1-%d.pid");
        File file = ApplicationOptionsParser.getFile(prop, "pid_file", true, null, true);
        assertNotNull(file);
        assertTrue(file.getCanonicalFile().getParentFile().exists());
        assertTrue(file.getCanonicalFile().getParentFile().canWrite());
        assertFalse(file.getCanonicalPath().contains("%d"));
        assertTrue(file.getCanonicalPath().contains(Integer.toString(Hostname.PID)));
    }
    
    @Test
    public void testGetFileRepoHappy() throws IOException {
        Properties prop = new Properties();
        prop.put("pid_file", "/tmp/chocolate-common-test2-%s.pid");
        File file = ApplicationOptionsParser.getFile(prop, "pid_file", false, "stuff", true);
        assertNotNull(file);
        assertTrue(file.getCanonicalFile().getParentFile().exists());
        assertTrue(file.getCanonicalFile().getParentFile().canWrite());
        assertTrue(file.getName().contains("chocolate-common-test2-stuff.pid"));
        assertFalse(file.getCanonicalPath().contains(Integer.toString(Hostname.PID)));
    }
    
    @Test
    public void testGetFileRepoPiadHappy() throws IOException {
        Properties prop = new Properties();
        String template = "\\tmp\\chocolate-common-test3-%d-%s.pid";
        String expected = String.format(template, Hostname.PID, "stuff");
        prop.put("pid_file", template);
        File file = ApplicationOptionsParser.getFile(prop, "pid_file", false, "stuff", true);
        assertNotNull(file);
        assertTrue(file.getCanonicalFile().getParentFile().exists());
        assertTrue(file.getCanonicalFile().getParentFile().canWrite());
        assertTrue(file.getCanonicalPath().contains(expected));
    }
    
    @Test
    public void testGetFileRepoPiadHappy2() throws IOException {
        Properties prop = new Properties();
        String template = "\\tmp\\chocolate-common-test4-%s-%d.pid";
        String expected = String.format(template, "stuff", Hostname.PID);
        prop.put("pid_file", template);
        File file = ApplicationOptionsParser.getFile(prop, "pid_file", false, "stuff", true);
        assertNotNull(file);
        assertTrue(file.getCanonicalFile().getParentFile().exists());
        assertTrue(file.getCanonicalFile().getParentFile().canWrite());
        assertTrue(file.getCanonicalPath().contains(expected));
    }
    
    @Test
    public void testDumpProperties() {
        Properties properties = new Properties();
        ApplicationOptionsParser.dumpProperties(properties);
        properties.put("key", "value");
        ApplicationOptionsParser.dumpProperties(properties);
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testBooleanPropertiesNoDefaults() {
        Properties properties = new Properties();
        ApplicationOptionsParser.getBooleanProperty(properties, "nonexistent", null);
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testBooleanPropertiesBad() {
        Properties properties = new Properties();
        ApplicationOptionsParser.getBooleanProperty(properties, "mixxing", null);
    }
    
    @Test
    public void testBooleanPropertiesHappy() {
        Properties properties = new Properties();
        assertTrue(ApplicationOptionsParser.getBooleanProperty(properties, "key", true));
        assertFalse(ApplicationOptionsParser.getBooleanProperty(properties, "key", false));

        properties.put("key", Boolean.FALSE.toString());
        assertFalse(ApplicationOptionsParser.getBooleanProperty(properties, "key", true));
        assertFalse(ApplicationOptionsParser.getBooleanProperty(properties, "key", false));
        assertFalse(ApplicationOptionsParser.getBooleanProperty(properties, "key", null));
        
        properties.put("key", Boolean.TRUE.toString());
        assertTrue(ApplicationOptionsParser.getBooleanProperty(properties, "key", true));
        assertTrue(ApplicationOptionsParser.getBooleanProperty(properties, "key", false));
        assertTrue(ApplicationOptionsParser.getBooleanProperty(properties, "key", null));
    }
    
    @Test
    public void testNumericPropertiesHappy() {
        Properties properties = new Properties();
        properties.put("numeric", "5");
        assertEquals(5, ApplicationOptionsParser.getNumericProperty(properties, "numeric", 4, 6));
        assertEquals(5, ApplicationOptionsParser.getNumericProperty(properties, "numeric", 5, 5));
        assertEquals(5, ApplicationOptionsParser.getNumericProperty(properties, "numeric", 5, 10));
        assertEquals(5, ApplicationOptionsParser.getNumericProperty(properties, "numeric", 1, 5));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testNumericPropertiesLow() {
        Properties properties = new Properties();
        properties.put("numeric", "5");
        ApplicationOptionsParser.getNumericProperty(properties, "numeric", 6, 10); 
    }

    @Test(expected = UnsupportedOperationException.class) 
    public void testNumericPropertiesHigh() {
        Properties properties = new Properties();
        properties.put("numeric", "5");
        ApplicationOptionsParser.getNumericProperty(properties, "numeric", 1, 4); 
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNumericPropertiesBadRange() {
        Properties properties = new Properties();
        properties.put("numeric", "5");
        ApplicationOptionsParser.getNumericProperty(properties, "numeric", 10, 1);         
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testNumericPropertiesInvalid() {
        Properties properties = new Properties();
        properties.put("invalid", "kslfkjsf");
        ApplicationOptionsParser.getNumericProperty(properties, "invalid", 1, 10); 
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testNumericPropertiesMissing() {
        Properties properties = new Properties();
        ApplicationOptionsParser.getNumericProperty(properties, "missing", 1, 10); 
    }
    
    @Test
    public void testReadStringProperties() {
        Properties properties = new Properties();
        properties.put("stringkey", "value");
        assertEquals("value", ApplicationOptionsParser.getStringProperty(properties, "stringkey"));
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testReadStringPropertiesBlank() {
        Properties properties = new Properties();
        properties.put("stringkey", " ");
        ApplicationOptionsParser.getStringProperty(properties, "stringkey");
    }
    
    
    @Test(expected = NullPointerException.class) 
    public void testReadStringPropertiesNull() {
        Properties properties = new Properties();
        properties.put("stringkey", null);
        ApplicationOptionsParser.getStringProperty(properties, "stringkey");
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testReadStringPropertiesMissing() {
        Properties properties = new Properties();
        ApplicationOptionsParser.getStringProperty(properties, "stringkey");
    }

    @Test
    public void testReadStringArrayPropertiesSingle() {
        Properties properties = new Properties();
        properties.put("stringkey", "value");
        String [] array = ApplicationOptionsParser.getStringArrayProperty(properties, "stringkey");
        assertEquals(1, array.length);
        assertEquals("value", array[0]);
    }
    
    @Test
    public void testReadStringArrayPropertiesMulti() {
        Properties properties = new Properties();
        properties.put("stringkey", "10.1,10.2,10.3");
        String [] array = ApplicationOptionsParser.getStringArrayProperty(properties, "stringkey");
        assertEquals(3, array.length);
        assertEquals("10.1", array[0]);
        assertEquals("10.2", array[1]);
        assertEquals("10.3", array[2]);
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testReadStringArrayPropertiesBlank() {
        Properties properties = new Properties();
        properties.put("stringkey", " ");
        ApplicationOptionsParser.getStringProperty(properties, "stringkey");
    }
    
    
    @Test(expected = NullPointerException.class) 
    public void testReadStringArrayPropertiesNull() {
        Properties properties = new Properties();
        properties.put("stringkey", null);
        ApplicationOptionsParser.getStringProperty(properties, "stringkey");
    }
    
    @Test(expected = UnsupportedOperationException.class) 
    public void testReadStringArrayPropertiesMissing() {
        Properties properties = new Properties();
        ApplicationOptionsParser.getStringProperty(properties, "stringkey");
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testGetDriverIdNullHostname() {
        ApplicationOptionsParser.getDriverIdFromIp(null, 12l);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testGetDriverIdBadHostname() {
        ApplicationOptionsParser.getDriverIdFromIp(" ", 12l);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testGetDriverIdBadIp() {
        ApplicationOptionsParser.getDriverIdFromIp("localhost", 0l);
    }
    
    @Test
    public void testGetDriverIdFallbackToDecimal() {
        final int driverIdRange = Long.valueOf(SnapshotId.MAX_DRIVER_ID).intValue() + 1;
        boolean [] found = new boolean [driverIdRange];
        Arrays.fill(found, false);
        
        for (int i = 1; i < driverIdRange * 3; ++i) {
            int parsed = ApplicationOptionsParser.getDriverIdFromIp("localhost", i);
            assertEquals(i % driverIdRange, parsed);
            if (i > driverIdRange) {
                assertTrue(found[i%driverIdRange]);
            } else {
                found[i%driverIdRange]=true;
            }
            
            // Invalid number sequence should still fallback
            assertEquals(parsed, ApplicationOptionsParser.getDriverIdFromIp("localhost-12389471298347198327491874", i));
        }
    }
    
    @Test
    public void testDriverIdParseFromHost() {
        // String positions
        final int driverIdRange = Long.valueOf(SnapshotId.MAX_DRIVER_ID).intValue() + 1;
        assertEquals(7777l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("7777-localhost", 1l));
        assertEquals(7777l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("7777", 1l));
        assertEquals(7777l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost-7777-localhost", 1l));
        assertEquals(7777l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost7777", 1l));
        assertEquals(7777l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost1-7777", 1l));
        assertEquals(7777l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost7777-1", 1l));
        
        // Have a mixture of numbers and sequencing
        assertEquals(93423421142342423l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost7777-1-93423421142342423l-23423421423142342423", 1l));
        assertEquals(93423421142342423l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost93423421142342423-7777-1-l-23423421423142342423", 1l));
        assertEquals(93423421142342423l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost7777-1-23423421423142342423-93423421142342423", 1l));
        assertEquals(93423421142342423l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("localhost7777-93423421142342423-1-93423421142342423l-23423421423142342423", 1l));
        assertEquals(93423421142342423l % driverIdRange, ApplicationOptionsParser.getDriverIdFromIp("93423421142342423localhost7777-1-23423421423142342423", 1l));
        
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetDriverIdFromIp() {
        int randomDriverIdMask = ApplicationOptionsParser.RANDOM_DRIVER_ID_MASK;
        assertEquals(randomDriverIdMask & 1, ApplicationOptionsParser.getDriverIdFromIp(1L));
        assertEquals(randomDriverIdMask & 1, ApplicationOptionsParser.getDriverIdFromIp(1025L));
        assertEquals(randomDriverIdMask & 0, ApplicationOptionsParser.getDriverIdFromIp(1024L));
        assertEquals(randomDriverIdMask & 1, ApplicationOptionsParser.getDriverIdFromIp(-1L));
    }
}

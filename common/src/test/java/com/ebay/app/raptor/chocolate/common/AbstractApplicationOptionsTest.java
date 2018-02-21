package com.ebay.app.raptor.chocolate.common;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.ServiceConfigurationError;

import org.junit.Test;

/**
 * @author jepounds
 */
@SuppressWarnings("javadoc")
public class AbstractApplicationOptionsTest {
    
    public static class TestApplicationOptions extends AbstractApplicationOptions {
        
        public void initInstance(File file) throws IOException {
            super.initInstance(file);
        }
        
        public void initInstance(final Properties properties) {
            super.initInstance(properties);
        }
        
    }

    @Test(expected=NullPointerException.class)
    public void initFileNull() throws IOException {
        TestApplicationOptions options = new TestApplicationOptions();
        options.initInstance((File) null);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void initFileNonexistent() throws IOException {
        TestApplicationOptions options = new TestApplicationOptions();
        File file = File.createTempFile("test", Long.valueOf(SnapshotId.getNext(1).getRepresentation()).toString());
        assertTrue(file.delete());
        options.initInstance(file);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void initFileUnreadable() throws IOException {
        TestApplicationOptions options = new TestApplicationOptions();
        File file = File.createTempFile("test", Long.valueOf(SnapshotId.getNext(1).getRepresentation()).toString());
        assertTrue(file.setReadable(false));
        options.initInstance(file);
        file.deleteOnExit();
        assertTrue(file.setReadable(true));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInitInstanceBadString() throws IOException {
        TestApplicationOptions options = new TestApplicationOptions();
        options.initInstance("", this.getClass());
    }

    @Test
    public void initFromFile() throws IOException {
        TestApplicationOptions options = new TestApplicationOptions();
        File file = createTemporaryPropertiesFile();
        
        options.initInstance(file);
        assertEquals(30000, options.getByNameInteger("chocolate.common.ut.example.property"));
    }

    @Test
    public void initFromInputStream() throws IOException {
        TestApplicationOptions options = new TestApplicationOptions();
        File file = createTemporaryPropertiesFile();
        FileInputStream stream = new FileInputStream(file);

        options.initInstance(stream);
        assertEquals(30000, options.getByNameInteger("chocolate.common.ut.example.property"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void initFileDirectory() throws IOException {
        TestApplicationOptions options = new TestApplicationOptions();
        File file = File.createTempFile("test", Long.valueOf(SnapshotId.getNext(1).getRepresentation()).toString());
        assertTrue(file.delete());
        assertTrue(file.mkdir());
        file.deleteOnExit();
        options.initInstance(file);
    }
    
    @Test(expected=ServiceConfigurationError.class)
    public void testGetByNameBooleanMissing() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        options.initInstance(prop);
        options.getByNameBoolean("missing");
    }
    
    @Test
    public void testGetByNameBooleanHappy() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", Boolean.TRUE.toString());
        options.initInstance(prop);
        assertTrue(options.getByNameBoolean("key"));
    }
    
    @Test(expected=ServiceConfigurationError.class)
    public void testGetByNameIntegerMissing() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        options.initInstance(prop);
        options.getByNameInteger("missing");
    }
    
    @Test(expected=NumberFormatException.class)
    public void testGetByNameIntegerInvalid() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", Boolean.FALSE.toString());
        options.initInstance(prop);
        options.getByNameInteger("key");
    }
    
    @Test
    public void testGetByNameIntegerHappy() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", Integer.valueOf(3).toString());
        options.initInstance(prop);
        assertEquals(3, options.getByNameInteger("key"));
    }
    
    @Test(expected=ServiceConfigurationError.class)
    public void testGetByNameLongMissing() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        options.initInstance(prop);
        options.getByNameLong("missing");
    }
    
    @Test(expected=NumberFormatException.class)
    public void testGetByNameLongInvalid() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", Boolean.FALSE.toString());
        options.initInstance(prop);
        options.getByNameLong("key");
    }
    
    @Test
    public void testGetByNameLongHappy() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", Long.valueOf(3).toString());
        options.initInstance(prop);
        assertEquals(3, options.getByNameLong("key"));
    }
    
    @Test(expected=ServiceConfigurationError.class)
    public void testGetByNameFloatMissing() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        options.initInstance(prop);
        options.getByNameFloat("missing");
    }
    
    @Test(expected=NumberFormatException.class)
    public void testGetByNameFloatInvalid() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", Boolean.FALSE.toString());
        options.initInstance(prop);
        options.getByNameFloat("key");
    }
    
    @Test
    public void testGetByNameFloatHappy() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", Float.valueOf(3.3f).toString());
        options.initInstance(prop);
        assertEquals(3.3f, options.getByNameFloat("key"), 0.0001f);
    }
    
    @Test(expected=ServiceConfigurationError.class)
    public void testGetByNameStringMissing() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        options.initInstance(prop);
        options.getByNameString("missing");
    }
    
    @Test
    public void testGetByNameStringNumber() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", 1212432);
        options.initInstance(prop);
        assertNull(options.getByNameString("key"));
    }

    @Test
    public void testGetByNameStringHappy() {
        TestApplicationOptions options = new TestApplicationOptions();
        Properties prop = new Properties();
        prop.put("key", "foobar");
        options.initInstance(prop);
        assertEquals("foobar", options.getByNameString("key"));
    }

    // Helper method
    private File createTemporaryPropertiesFile() throws IOException {
        File file = File.createTempFile("test", Long.valueOf(SnapshotId.getNext(1).getRepresentation()).toString());
        file.deleteOnExit();

        PrintWriter writer = new PrintWriter(file.getCanonicalPath(), "UTF-8");
        writer.println("<?xml version=\"1.0\"?>");
        writer.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
        writer.println("<configuration supports_final=\"true\">");
        writer.println("  <property>");
        writer.println("    <name>chocolate.common.ut.example.property</name>");
        writer.println("    <value>30000</value>");
        writer.println("  </property>");
        writer.println("</configuration>");
        writer.close();
        return file;
    }
}

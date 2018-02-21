package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.ServiceConfigurationError;

/**
 * Controls the parsing of Chocolate application options.
 * 
 * @author jepounds
 */
public class AbstractApplicationOptions {
    /** Private logging instance */
    private static final Logger logger = Logger.getLogger(AbstractApplicationOptions.class);

    /** Properties file */
    protected Properties properties = new Properties();

    /**
     * Only created by ApplicationOptions
     */
    protected AbstractApplicationOptions() {}
    
    /**
     * Application options to load file from
     * 
     * @pre file cannot be null and must exist as a valid readable file.
     * @param inputFile to load file using
     * @throws IOException if properties could not be loaded
     */
    protected void initInstance(final File inputFile) throws IOException {
        Validate.notNull(inputFile, "File cannot be null");
        Validate.isTrue(ApplicationOptionsParser.isValidFile(inputFile), "Must be a valid file");

        initInstance(new FileInputStream(inputFile));
        logger.info("Finished loading properties from " + inputFile.getCanonicalPath());
    }

    /**
     * Load application options from an input stream
     * @param inputStream resource that has the options in it
     * @throws IOException in case of any filesystem problems
     */
    protected void initInstance(final InputStream inputStream) throws IOException {
        properties = ApplicationOptionsParser.readXmlProperties(inputStream);
        ApplicationOptionsParser.dumpProperties(properties);
    }

    /**
     * Application options to load from internal jar
     * 
     * @param propertiesPath to load file from
     * @param clazz that is initializing from main
     * @throws IOException if properties could not be loaded
     */
    @SuppressWarnings("rawtypes")
    protected void initInstance(String propertiesPath, Class clazz) throws IOException {
        Validate.isTrue(StringUtils.isNotBlank(propertiesPath), "Properties path cannot be null");
        logger.warn("Loading properties from JAR defaults:" + propertiesPath);
        // Get properties file
        InputStream inputStream = clazz.getClassLoader().getResourceAsStream(propertiesPath);
        if (inputStream != null) {
            properties = ApplicationOptionsParser.readXmlProperties(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propertiesPath + "' not found in the classpath");
        }
        ApplicationOptionsParser.dumpProperties(properties);
        logger.info("Finished loading properties from JAR");
    }

    /**
     * For UT purposes only
     * 
     * @param properties to initialize using
     */
    protected void initInstance(final Properties properties) {
        Validate.notNull(properties, "Properties cannot be null");
        this.properties = properties;
    }

    /**
     * Get an option value by name directly
     * @param key string key
     * @return option value if present
     */
    public String getByNameString(String key) {
        if (!properties.containsKey(key)) {
            throw new ServiceConfigurationError("Configuration property not found: " + key);
        }

        return properties.getProperty(key);
    }

    /**
     * Get an option value by name directly
     * @param key string key
     * @return option value if present
     */
    public boolean getByNameBoolean(String key) {
        if (!properties.containsKey(key)) {
            throw new ServiceConfigurationError("Configuration property not found: " + key);
        }

        return Boolean.parseBoolean(properties.getProperty(key));
    }

    /**
     * Get an option value by name directly
     * @param key string key
     * @return option value if present
     */
    public int getByNameInteger(String key) {
        if (!properties.containsKey(key)) {
            throw new ServiceConfigurationError("Configuration property not found: " + key);
        }

        return Integer.parseInt(properties.getProperty(key));
    }

    /**
     * Get an option value by name directly
     * @param key string key
     * @return option value if present
     */
    public long getByNameLong(String key) {
        if (!properties.containsKey(key)) {
            throw new ServiceConfigurationError("Configuration property not found: " + key);
        }

        return Long.parseLong(properties.getProperty(key));
    }

    /**
     * Get an option value by name directly
     * @param key string key
     * @return option value if present
     */
    public float getByNameFloat(String key) {
        if (!properties.containsKey(key)) {
            throw new ServiceConfigurationError("Configuration property not found: " + key);
        }

        return Float.parseFloat(properties.getProperty(key));
    }
}

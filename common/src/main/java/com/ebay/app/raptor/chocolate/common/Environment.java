package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum settings for various environments
 * 
 * @author jepounds
 */
public enum Environment {
    /** Production setting */
    PROD("prod", "production"),

    /** QA setting */
    QE("qa", "qe"),

    /** Staging setting */
    STAGING("stage", "staging"),

    /** Test setting */
    TEST("test"),

    /** Dev setting */
    DEV("dev", "developer");

    // Names of the setting. Allows for aliases and such
    private final String [] names;

    // Lookup map for quick access.
    private static final Map<String, Environment> LOOKUP_MAP = new HashMap<String, Environment>();

    // Initialize the map within a static block as enums are initialized
    // before
    // static blocks.
    static {
        for (Environment s : Environment.values()) {
            Validate.isTrue(s.names.length > 0, "must have valid names");
            Validate.noNullElements(s.names, "No null element in names");
            for (String name : s.names) {
                Validate.isTrue(name.toLowerCase().equals(name), "name should be lowercase");
                Validate.isTrue(!LOOKUP_MAP.containsKey(name.toLowerCase()), "No duplicate names");
                LOOKUP_MAP.put(name, s);
            }
        }
    }

    /**
     * Private enum ctor
     * 
     * @param name of settings to pass via command line, must be unique
     */
    private Environment(String... names) {
        this.names = names;
    }

    @Override
    public String toString() {
        return names[0];
    }

    /**
     * Get a setting from a string
     * 
     * @param name to lookup
     * @return the setting, or null if it's not matched
     */
    public static Environment fromString(final String name) {
        if (name == null) return null;
        String lc = name.toLowerCase();
        return LOOKUP_MAP.containsKey(lc) ? LOOKUP_MAP.get(lc) : null;
    }
}
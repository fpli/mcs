package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * This enum is used for HTTP methods as a way of mapping the stringified value of 
 * HTTP methods to their corresponding Avro enums because HttpServletRequest is 
 * not good at putting them in an enum instead of a string. 
 */
public enum HttpMethodEnum {
    /** CONNECT HTTP method */
    CONNECT("CONNECT", HttpMethod.CONNECT),
    
    /** DELETE HTTP method */
    DELETE("DELETE", HttpMethod.DELETE),

    /** GET HTTP method */
    GET("GET", HttpMethod.GET),

    /** HEAD HTTP method */
    HEAD("HEAD", HttpMethod.HEAD),
    
    /** OPTIONS HTTP method */
    OPTIONS("OPTIONS", HttpMethod.OPTIONS),
    
    /** PATCH HTTP method */
    PATCH("PATCH", HttpMethod.PATCH),

    /** POST HTTP method */
    POST("POST", HttpMethod.POST),

    /** PUT HTTP method */
    PUT("PUT", HttpMethod.PUT),

    /** PUT HTTP method */
    TRACE("TRACE", HttpMethod.TRACE);
    
    /** The human-parsable HTTP method name. */
    private final String value;
    
    /** The Avro HTTP method being represented. */
    private final HttpMethod avroMethod;
    
    /** Static mapping of HTTP method names to their enums. */
    private static final Map<String, HttpMethodEnum> MAP;
    static {
        // Create the mapping, checking duplicates as we go.
        Set<HttpMethod> seenMethods = new HashSet<>(HttpMethod.values().length);
        Map<String, HttpMethodEnum> map = new HashMap<>();
        
        for (HttpMethodEnum method : HttpMethodEnum.values()) {
            Validate.isTrue(StringUtils.isAllUpperCase(method.getValue()), "HTTP method must use caps");
            Validate.isTrue(!map.containsKey(method.getValue()), "No duplicate entries allowed");
            map.put(method.getValue(), method);
            
            // Check for duplicates against the Avro object to be sure we've captured them all
            Validate.isTrue(!seenMethods.contains(method.getAvro()), "Duplicate Avro entry");
            seenMethods.add(method.getAvro());
        }
        
        // Validate we've captured every Avro entry.
        Validate.isTrue(seenMethods.size() == HttpMethod.values().length, "Didn't get all of the Avro entries");
        
        // now that we have a mapping, create the immutable construct.
        MAP = Collections.unmodifiableMap(map);
    }

    /**
     * Enum ctor. 
     * 
     * @param value      The URL-parseable value of the HTTP method (PUT, POST...)
     * @param avroMethod the Avro method this HTTP method represents
     */
    HttpMethodEnum(String value, HttpMethod avroMethod) {
        Validate.isTrue(StringUtils.isAllUpperCase(value), "HTTP method must be uppercase");
        Validate.notNull(avroMethod, "Avro tag must not be null at this point");
        this.value = value;
        this.avroMethod = avroMethod;
    }
    
    /** @return the Avro method this HTTP method represents */
    public HttpMethod getAvro() { return this.avroMethod; }
    
    /** @return the String value of this method. */
    public String getValue() { return this.value; }
    
    /** @return the HTTP method given an input parameter or null if it doesn't match up */ 
    public static HttpMethodEnum parse(String candidate) {
        if (candidate == null) return null;
        candidate = candidate.trim().toUpperCase(); // Convert to uppercase
        return MAP.getOrDefault(candidate, null);
    }
}
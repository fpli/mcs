package com.ebay.traffic.chocolate.listener.util;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This enum is used for Listener channels.
 * Right now it only handles ePN channel
 * Will add more channels in the future
 */
public enum ChannelIdEnum {
    /** Channel one -- production ePN */
    EPN("1", LogicalChannelEnum.EPN, false),

    /** Channel four -- production DAP */
    DAP("4", LogicalChannelEnum.DISPLAY, false),

    /** Channel nine -- test ePN */
    NINE("9", LogicalChannelEnum.EPN, true);

    /** The human-parsable channel name. */
    private final String channelValue;
    
    /** The logical channel that this channel ID belongs to. */
    private final LogicalChannelEnum logicalChannel;
    
    /** Whether or not this is a "test" channel. */
    private final boolean isTestChannel;
    
    /** Static mapping of channel names to IDs. */
    private static final Map<String, ChannelIdEnum> MAP;
    static {
        // Create the mapping, checking duplicates as we go.
        Map<String, ChannelIdEnum> map = new HashMap<>();
        for (ChannelIdEnum c : ChannelIdEnum.values()) {
            Validate.isTrue(StringUtils.isNumeric(c.getValue()), "Channel ID must be numeric");
            Validate.isTrue(!map.containsKey(c.getValue()), "No duplicate entries allowed");
            map.put(c.getValue(), c);
        }
        
        // now that we have a mapping, create the immutable construct.
        MAP = Collections.unmodifiableMap(map);
    }

    /**
     * Enum ctor. 
     * 
     * @param channelStr      The URL-parseable value of the channel ID (1, 9...)
     * @param logicalChannel  The logical channel grouping this belongs to (EPN, display...)
     * @param isTest          True if it's a mock channel (e.g., 9), false otherwise
     */
    ChannelIdEnum(String channelStr, LogicalChannelEnum logicalChannel, boolean isTest) {
        Validate.isTrue(StringUtils.isNumeric(channelStr), "channelStr must be numeric");
        Validate.notNull(logicalChannel, "Logical channel must not be null at this point");
        this.channelValue = channelStr;
        this.logicalChannel = logicalChannel;
        this.isTestChannel = isTest;
    }
    
    /** @return the URL value of this channel. */
    public String getValue() { return this.channelValue; }
    
    /** @return true iff this is a mock (test purposes only) channel. */
    public boolean isTestChannel() { return this.isTestChannel; }
    
    /** @return the logical channel that this channel ID represents */
    public LogicalChannelEnum getLogicalChannel() { return this.logicalChannel; }
    
    /** @return the channel ID given an input parameter or null if it doesn't match up */ 
    public static ChannelIdEnum parse(String candidate) {
        return MAP.getOrDefault(candidate, null);
    }
}
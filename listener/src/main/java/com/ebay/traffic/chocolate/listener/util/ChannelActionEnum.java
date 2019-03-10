package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This enum is used for Listener tracking actions. Right now it only handles impression and click actions Will add more
 * actions in the future
 */
public enum ChannelActionEnum {
    /** The Rover click action */
    CLICK(ChannelAction.CLICK, "1c", "rover"),

    /** The viewable impression action */
    VIMP(ChannelAction.VIEWABLE, "1v"),

    /** The Rover impression action */
    IMPRESSION(ChannelAction.IMPRESSION, "1i", "roverimp", "ar"),

    /** Ad Serve action */
    SERVE(ChannelAction.SERVE, "1s"),

    /** ePN page impression */
    PAGE_IMP(ChannelAction.PAGE_IMP, "1p"),

    /** Item view on multipage ads (carousel rotation) */
    VIEW_ITEM(ChannelAction.VIEW_ITEM, "1r"),

    /** Ad view timer (dwell time tracking) */
    VIEW_TIME(ChannelAction.VIEW_TIME, "1d"),

    /** Mobile App first start */
    APP_FIRST_START(ChannelAction.APP_FIRST_START, "1m");

    /** The human-readable action name */
    private final String [] actions;

    /** The Avro equivalent in this */
    private final ChannelAction avroAction;

    /** Mapping from action to enum */
    private static final Map<String, ChannelActionEnum> MAP;
    static {
        // Create the mapping, checking duplicates as we go.
        Map<String, ChannelActionEnum> map = new HashMap<>();
        for (ChannelActionEnum a : ChannelActionEnum.values()) {
            for (String action : a.getActions()) {
                Validate.isTrue(StringUtils.isNotEmpty(action) && StringUtils.isAlphanumeric(action)
                        && action.toLowerCase().equals(action), "Action must be lowercase");
                Validate.isTrue(!map.containsKey(action), "No duplicate entries allowed");
                map.put(action, a);
            }
        }

        // now that we have a mapping, create the immutable construct.
        MAP = Collections.unmodifiableMap(map);
    }

    /** Ctor. Takes in the human-readable URI segment representing this value */
    ChannelActionEnum(ChannelAction avro, String ... actions) {
        Validate.notNull(avro, "Avro action cannot be null");
        Validate.isTrue(actions != null && actions.length > 0, "actions cannot be null");
        this.actions = actions;
        this.avroAction = avro;
    }

    /** @return the URI segment representing this action */
    String [] getActions() {
        return this.actions;
    }

    /** @return the Avro enum associated with this action */
    public ChannelAction getAvro() {
        return this.avroAction;
    }

    /** Return the Rover action given an URI segment candidate, or null if no matches are found. */
    public static ChannelActionEnum parse(ChannelIdEnum channel, String candidate) {
        if (candidate == null) return null;
        String converted = candidate.toLowerCase();
        ChannelActionEnum basicResult = MAP.getOrDefault(converted, null);

        return basicResult;
    }
}

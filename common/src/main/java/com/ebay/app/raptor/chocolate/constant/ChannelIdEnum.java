package com.ebay.app.raptor.chocolate.constant;

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
  /**
   * Channel one -- production ePN
   */
  EPN("1", LogicalChannelEnum.EPN, false),

  /**
   * Channel two -- production PAID SEARCH
   */
  PAID_SEARCH("2", LogicalChannelEnum.PAID_SEARCH, false),

  /**
   * Channel four -- production DAP
   */
  DAP("4", LogicalChannelEnum.DISPLAY, false),

  /**
   * Channel seven -- production SITE EMAIL
   */
  SITE_EMAIL("7", LogicalChannelEnum.SITE_EMAIL, false),

  /**
   * Channel eight -- production MARKETING EMAIL
   */
  MRKT_EMAIL("8", LogicalChannelEnum.MRKT_EMAIL, false),

  /**
   * Channel nine -- test ePN
   */
  NINE("9", LogicalChannelEnum.EPN, true),

  /**
   * Channel 16 -- production SOCIAL MEDIA
   */
  SOCIAL_MEDIA("16", LogicalChannelEnum.SOCIAL_MEDIA, false),

  /**
   * Channel 20 -- production PAID SOCIAL
   */
  PAID_SOCIAL("20", LogicalChannelEnum.PAID_SOCIAL, false),

  /**
   * Channel 24 -- production MARKETING SMS
   */
  MRKT_SMS("24", LogicalChannelEnum.MRKT_SMS, false),

  /**
   * Channel 25 -- production SITE SMS
   */
  SITE_SMS("25", LogicalChannelEnum.SITE_SMS, false),

  /**
   * Channel 28 -- production SEARCH ENGINE FREE LISTINGS
   */
  SEARCH_ENGINE_FREE_LISTINGS("28", LogicalChannelEnum.SEARCH_ENGINE_FREE_LISTINGS, false),

  /**
   * Channel 0 -- production ROI
   */
  ROI("0", LogicalChannelEnum.ROI, false);

  /**
   * Channel -1 -- production new ROI test channel
   *//*
  NEW_ROI("-1", LogicalChannelEnum.ROI, false);*/

  /**
   * The human-parsable channel name.
   */
  private final String channelValue;

  /**
   * The logical channel that this channel ID belongs to.
   */
  private final LogicalChannelEnum logicalChannel;

  /**
   * Whether or not this is a "test" channel.
   */
  private final boolean isTestChannel;

  /** @return true iff this is a mock (test purposes only) channel. */
  public boolean isTestChannel() { return this.isTestChannel; }

  /**
   * Static mapping of channel names to IDs.
   */
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
   * @param channelStr     The URL-parseable value of the channel ID (1, 9...)
   * @param logicalChannel The logical channel grouping this belongs to (EPN, display...)
   * @param isTest         True if it's a mock channel (e.g., 9), false otherwise
   */
  ChannelIdEnum(String channelStr, LogicalChannelEnum logicalChannel, boolean isTest) {
    Validate.isTrue(StringUtils.isNumeric(channelStr), "channelStr must be numeric");
    Validate.notNull(logicalChannel, "Logical channel must not be null at this point");
    this.channelValue = channelStr;
    this.logicalChannel = logicalChannel;
    this.isTestChannel = isTest;
  }

  /**
   * @return the URL value of this channel.
   */
  public String getValue() {
    return this.channelValue;
  }

  /**
   * @return the logical channel that this channel ID represents
   */
  public LogicalChannelEnum getLogicalChannel() {
    return this.logicalChannel;
  }

  /**
   * @return the channel ID given an input parameter or null if it doesn't match up
   */
  public static ChannelIdEnum parse(String candidate) {
    return MAP.getOrDefault(candidate, null);
  }
}
package com.ebay.app.raptor.chocolate.constant;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import org.apache.commons.lang.Validate;

/**
 * Logical channel enum -- as in the colloquial advertising channel terms used in ATM.
 * <p>
 * Note that the relation between logical channels and channel IDs is a one-to-many mapping.
 *
 * @author jepounds
 */
public enum LogicalChannelEnum {

  /**
   * EPN channel - valid Rover impressions
   */
  EPN(ChannelType.EPN, ChannelActionEnum.CLICK, ChannelActionEnum.PAGE_IMP, ChannelActionEnum.SERVE,
    ChannelActionEnum.IMPRESSION, ChannelActionEnum.VIMP),

  /**
   * PAID SEARCH channel - parallel tracking
   */
  PAID_SEARCH(ChannelType.PAID_SEARCH, ChannelActionEnum.CLICK, ChannelActionEnum.PAGE_IMP, ChannelActionEnum
    .IMPRESSION),

  /**
   * SOCIAL MEDIA channel
   */
  SOCIAL_MEDIA(ChannelType.SOCIAL_MEDIA, ChannelActionEnum.CLICK, ChannelActionEnum.IMPRESSION),

  /**
   * PAID SOCIAL channel
   */
  PAID_SOCIAL(ChannelType.PAID_SOCIAL, ChannelActionEnum.CLICK, ChannelActionEnum.IMPRESSION),

  /**
   * Display channel - no valid Rover actions as of yet
   */
  DISPLAY(ChannelType.DISPLAY, ChannelActionEnum.CLICK, ChannelActionEnum.SERVE, ChannelActionEnum.IMPRESSION,
    ChannelActionEnum.VIMP, ChannelActionEnum.VIEW_ITEM, ChannelActionEnum.VIEW_TIME),

  /**
   * SITE EMAIL channel
   */
  SITE_EMAIL(ChannelType.SITE_EMAIL, ChannelActionEnum.CLICK, ChannelActionEnum.EMAIL_OPEN),

  /**
   * MARKETING EMAIL channel
   */
  MRKT_EMAIL(ChannelType.MRKT_EMAIL, ChannelActionEnum.CLICK, ChannelActionEnum.EMAIL_OPEN),

  /**
   * ROI channel
   */
  ROI(ChannelType.ROI, ChannelActionEnum.ROI, ChannelActionEnum.ROI);

 /* *//**
   * New ROI Test channel
   *//*
  NEW_ROI(ChannelType.NEW_ROI, ChannelActionEnum.ROI, ChannelActionEnum.ROI);*/

  /**
   * Construct of valid Rover actions for this logical channel
   */
  private final ChannelActionEnum[] roverActions;


  /**
   * The corresponding Avro channel type.
   */
  private final ChannelType avroChannel;

  /**
   * Ctor. Includes a list of valid rover actions
   */
  LogicalChannelEnum(ChannelType channelType, ChannelActionEnum... roverActions) {
    Validate.notNull(channelType, "Avro type cannot be null");
    Validate.noNullElements(roverActions, "Rover actions cannot be null");
    this.roverActions = roverActions;
    this.avroChannel = channelType;
  }

  /**
   * @return true if the given Rover action is valid for this channel, false otherwise
   */
  public boolean isValidRoverAction(ChannelActionEnum a) {
    for (ChannelActionEnum action : roverActions) {
      if (a == action) return true;
    }
    return false;
  }

  /**
   * @return the Avro channel
   */
  public ChannelType getAvro() {
    return this.avroChannel;
  }
}

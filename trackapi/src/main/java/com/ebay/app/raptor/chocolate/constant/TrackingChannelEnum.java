package com.ebay.app.raptor.chocolate.constant;

import com.ebay.globalenv.SiteEnum;
import com.ebay.kernel.BaseEnum;

import java.io.ObjectStreamException;
import java.util.ListIterator;

/**
 * Identify the various marketing channels for which we track.
 */
public class TrackingChannelEnum extends BaseEnum {
  public static final String SOCIAL_MEDIA_CHANNEL_STR = "16";
  /**
   * AFFILIATES = 1
   */
  public static final TrackingChannelEnum AFFILIATES = new TrackingChannelEnum("AFFL", 1, true);
  /**
   * PAID_SEARCH = 2
   */
  public static final TrackingChannelEnum PAID_SEARCH = new TrackingChannelEnum("PSRCH", 2, true);
  /**
   * NATURAL_SEARCH = 3
   */
  public static final TrackingChannelEnum NATURAL_SEARCH =
      new TrackingChannelEnum("NSRCH", 3, false);
  /**
   * PORTAL = 4
   */
  public static final TrackingChannelEnum PORTAL = new TrackingChannelEnum("PORTAL", 4, true);
  /**
   * EBAY_API = 5
   */
  public static final TrackingChannelEnum EBAY_API =
      new TrackingChannelEnum("API", 5, true);
  /**
   * MISCELLANEOUS = 6
   */
  public static final TrackingChannelEnum MISCELLANEOUS =
      new TrackingChannelEnum("MISC", 6, false);
  /**
   * SITE_EMAIL = 7
   */
  public static final TrackingChannelEnum SITE_EMAIL =
      new TrackingChannelEnum("SEML", 7, false);
  /**
   * MRKT_EMAIL = 8
   */
  public static final TrackingChannelEnum MRKT_EMAIL = new TrackingChannelEnum("MEML", 8, false);

  /**
   * OFFSITE_MRKT = 6
   */
//	public static final TrackingChannelEnum OFFSITE_MRKT	=
//		new TrackingChannelEnum("OFMRKT", 6);
  /**
   * ONSITE = 9
   */
  public static final TrackingChannelEnum ONSITE =
      new TrackingChannelEnum("ONSITE", 9, false);
  /**
   * DC = 10 (Distributed Commerce)
   */
  public static final int DC_ID = 10;
  public static final TrackingChannelEnum DC =
      new TrackingChannelEnum("DC", DC_ID, false);
  /**
   * OffEbayDC = 11 (Off Ebay Distributed Commerce)
   */
  public static final int OFF_EBAY_DC_ID = 11;
  public static final TrackingChannelEnum OffEbayDC =
      new TrackingChannelEnum("OffEbayDC", OFF_EBAY_DC_ID, true);
  /**
   * DC IM channel
   */
  public static final TrackingChannelEnum DCIM =
      new TrackingChannelEnum("DCIM", 12, false);
  /**
   * DC MCO PAID search channel = 13
   */
  public static final TrackingChannelEnum DC_PAID_MCO =
      new TrackingChannelEnum("DC_PAID_MCO", 13, true);
  /**
   * EWA (Ebay Web Analytics) = 14 (previously LOW_VOLUME)
   */
  public static final TrackingChannelEnum EWA =
      new TrackingChannelEnum("EWA", 14, false);
  /**
   * PAY PAL (Channel For PayPal) = 15
   */
  public static final TrackingChannelEnum PAY_PAL =
      new TrackingChannelEnum("PAY_PAL", 15, false);
  /**
   * SOCIAL MEDIA (Channel For Social Media) = 16
   */
  public static final int SOCIAL_MEDIA_ID = 16;
  public static final TrackingChannelEnum SOCIAL_MEDIA = new TrackingChannelEnum("SOCIAL_MEDIA", 16, true);
  /**
   * SHOPPING.COM (Channel For Shopping.com) = 17
   */
  public static final TrackingChannelEnum SDC =
      new TrackingChannelEnum("SDC", 17, false);
  /**
   * CTD (Channel For Shopping.com for
   * Commerce Trading Desk initiative) = 18
   */
  public static final TrackingChannelEnum CTD =
      new TrackingChannelEnum("CTD", 18, false);
  /**
   * ECN (Channel For eBay Commerce Network) = 19
   */
  public static final int ECN_ID = 19;
  public static final TrackingChannelEnum ECN =
      new TrackingChannelEnum("ECN", 19, false);
  /**
   * Paid Social for direct facebook server integration = 20
   */
  public static final int PAID_SOCIAL_FB_ID = 20;
  public static final TrackingChannelEnum PAID_SOCIAL_FB = new TrackingChannelEnum("PAID_SOCIAL_FB", 20, false);
  /**
   * UNKNOWN = -1
   */
  public static final TrackingChannelEnum UNKNOWN =
      new TrackingChannelEnum("UNKNOWN", -1, false);
  /**
   * INTERNAL_NATURAL_SEARCH = -2
   */
  public static final TrackingChannelEnum INTERNAL_NATURAL_SEARCH =
      new TrackingChannelEnum("INTNSRCH", -2, false);
  public static final TrackingChannelEnum SITE_NOTIFICATION =
      new TrackingChannelEnum("SITE_NOTIFICATION", 22, false);
  public static final TrackingChannelEnum MARKETING_NOTIFICATION =
      new TrackingChannelEnum("MARKETING_NOTIFICATION", 23, false);


  //lzhang: we change the number to 21 to match what CAL-OLAP
  // 	script populates in db
  private static final long serialVersionUID = 34243521271314872L;
  private boolean m_setTpim;

  //-----------------------------------------------------------------//
  // Template code follows....do not modify other than to replace    //
  // enumeration class name with the name of this class.             //
  //-----------------------------------------------------------------//
  private TrackingChannelEnum(String name, int intValue, boolean setTpim) {
    super(intValue, name);
    m_setTpim = setTpim;
  }
  // Add new instances above this line

  /**
   * Get the enum with the given name or null.
   */
  public static TrackingChannelEnum get(String name) {
    return (TrackingChannelEnum) getEnum(TrackingChannelEnum.class, name);
  }

  /**
   * Get the enum with the given name or return else value.
   */
  public static TrackingChannelEnum getElseReturn(String name, TrackingChannelEnum elseEnum) {
    TrackingChannelEnum result = (TrackingChannelEnum) getEnum(TrackingChannelEnum.class, name);
    if (result == null) {
      result = elseEnum;
    }
    return result;
  }

  /**
   * Get the enumeration instance for a given value or null
   */
  public static TrackingChannelEnum get(int key) {
    return (TrackingChannelEnum) getEnum(TrackingChannelEnum.class, key);
  }

  /**
   * Get the enumeration instance for a given value or return the
   * elseEnum default.
   */
  public static TrackingChannelEnum getElseReturn(int key, TrackingChannelEnum elseEnum) {
    return (TrackingChannelEnum) getElseReturnEnum(TrackingChannelEnum.class, key, elseEnum);
  }

  /**
   * Return an bidirectional iterator that traverses the enumeration
   * instances in the order they were defined.
   */
  public static ListIterator iterator() {
    return getIterator(TrackingChannelEnum.class);
  }
  // ------- Type specific interfaces -------------------------------//

  //temporary fix that should be removed when we switch to WAS 5
  protected Object readResolve() throws ObjectStreamException {
    return super.readResolve();
  }

  /**
   * Return a String representation of this constant.
   */
  public String toString() {
    return getName();
  }

  public boolean getSetTpim() {
    return m_setTpim;
  }
}

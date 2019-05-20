package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import com.ebay.kernel.BaseEnum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

@SuppressWarnings("serial")
// copy from old EPN code
public class ClickFilterTypeEnum extends BaseEnum {
  public static final ClickFilterTypeEnum ENCRYPTION =
      new ClickFilterTypeEnum("ENCRYPTION", 1, ClickFilterSubTypeEnum.FILTER);

  public static final ClickFilterTypeEnum IMPRESSION_PIXEL_REQD =
      new ClickFilterTypeEnum("IMPRESSION_PIXEL_REQD", 2, ClickFilterSubTypeEnum.FILTER);

  public static final ClickFilterTypeEnum NO_RESTRICTIONS =
      new ClickFilterTypeEnum("NO_RESTRICTIONS", 3, ClickFilterSubTypeEnum.FILTER);

  public static final ClickFilterTypeEnum NOT_ON_NQ_WHITELIST =
      new ClickFilterTypeEnum("NOT_ON_NQ_WHITELIST", 4, ClickFilterSubTypeEnum.NOT_ON_NQ_WHITELIST, TrafficSourceEnum.Classic);

  public static final ClickFilterTypeEnum NOT_ON_NQ_WHITELIST_MOBILE_WEB =
      new ClickFilterTypeEnum("NOT_ON_NQ_WHITELIST_MOBILE_WEB", 6, ClickFilterSubTypeEnum.NOT_ON_NQ_WHITELIST, TrafficSourceEnum.MobileWeb);

  public static final ClickFilterTypeEnum NOT_ON_NQ_WHITELIST_MOBILE_APP =
      new ClickFilterTypeEnum("NOT_ON_NQ_WHITELIST_MOBILE_APP", 7, ClickFilterSubTypeEnum.NOT_ON_NQ_WHITELIST, TrafficSourceEnum.MobileApp);

  public static final ClickFilterTypeEnum MISSING_REFERRER_URL =
      new ClickFilterTypeEnum("MISSING_REFERRER_URL", 5, ClickFilterSubTypeEnum.MISSING_REFERRER_URL, TrafficSourceEnum.Classic);

  public static final ClickFilterTypeEnum MISSING_REFERRER_URL_MOBILE_WEB =
      new ClickFilterTypeEnum("MISSING_REFERRER_URL_MOBILE_WEB", 8, ClickFilterSubTypeEnum.MISSING_REFERRER_URL, TrafficSourceEnum.MobileWeb);

  public static final ClickFilterTypeEnum NOT_REGISTERED =
      new ClickFilterTypeEnum("NOT_REGISTERED", 9, ClickFilterSubTypeEnum.NOT_REGISTERED, TrafficSourceEnum.Classic);

  public static final ClickFilterTypeEnum NOT_REGISTERED_MOBILE_WEB =
      new ClickFilterTypeEnum("NOT_REGISTERED_MOBILE_WEB", 10, ClickFilterSubTypeEnum.NOT_REGISTERED, TrafficSourceEnum.MobileWeb);

  public static final ClickFilterTypeEnum NOT_REGISTERED_MOBILE_APP =
      new ClickFilterTypeEnum("NOT_REGISTERED_MOBILE_APP", 11, ClickFilterSubTypeEnum.NOT_REGISTERED, TrafficSourceEnum.MobileApp);

  public static final ClickFilterTypeEnum EBAY_REFERRAL =
      new ClickFilterTypeEnum("EBAY_REFERRAL", 12, ClickFilterSubTypeEnum.EBAY_REFERRER, TrafficSourceEnum.Classic);

  public static final ClickFilterTypeEnum EBAY_REFERRAL_MOBILE_WEB =
      new ClickFilterTypeEnum("EBAY_REFERRAL_MOBILE_WEB", 13, ClickFilterSubTypeEnum.EBAY_REFERRER, TrafficSourceEnum.MobileWeb);

  public static final ClickFilterTypeEnum ON_DOMAIN_BLIST =
      new ClickFilterTypeEnum("ON_DOMAIN_BLIST", 14, ClickFilterSubTypeEnum.ON_DOMAIN_BLACKLIST, TrafficSourceEnum.Classic);

  public static final ClickFilterTypeEnum ON_DOMAIN_BLIST_MOBILE_WEB =
      new ClickFilterTypeEnum("ON_DOMAIN_BLIST_MOBILE_WEB", 15, ClickFilterSubTypeEnum.ON_DOMAIN_BLACKLIST, TrafficSourceEnum.MobileWeb);

  public static final ClickFilterTypeEnum SDK_NOT_USED =
      new ClickFilterTypeEnum("ON_DOMAIN_BLIST_MOBILE_WEB", 16, ClickFilterSubTypeEnum.SDK_NOT_USED, TrafficSourceEnum.MobileApp);

  public static final ClickFilterTypeEnum FILTER_TRAFFIC_AND_ROI_EVENTS =
      new ClickFilterTypeEnum("FILTER_TRAFFIC_AND_ROI_EVENTS", 100, ClickFilterSubTypeEnum.FILTER_TRAFFIC_AND_ROI_EVENTS);

  public static final ClickFilterTypeEnum UNKNOWN =
      new ClickFilterTypeEnum("UNKNOWN", -999, ClickFilterSubTypeEnum.UNKNOWN);

  private TrafficSourceEnum m_trafficSourceEnum;

  //-----------------------------------------------------------------//
  // Template code follows....do not modify other than to replace    //
  // enumeration class name with the name of this class.             //
  //-----------------------------------------------------------------//
  private ClickFilterTypeEnum(String name, int intValue, ClickFilterSubTypeEnum filterSubTypeEnum, TrafficSourceEnum trafficSource) {
    super(intValue, name);
    m_clickFilterSubTypeEnum = filterSubTypeEnum;
    m_trafficSourceEnum = trafficSource;
  }

  private ClickFilterTypeEnum(String name, int intValue, ClickFilterSubTypeEnum filterSubTypeEnum) {
    this(name, intValue, filterSubTypeEnum, TrafficSourceEnum.Classic);
  }


  // ------- Type specific interfaces -------------------------------//
  /** Get the enumeration instance for a given value or null */
  public static ClickFilterTypeEnum get(int key) {
    return (ClickFilterTypeEnum)getElseReturn(key, UNKNOWN);
  }
  /** Get the enumeration instance for a given value or return the
   *  elseEnum default.
   */
  public static ClickFilterTypeEnum getElseReturn(int key, ClickFilterTypeEnum elseEnum) {
    return (ClickFilterTypeEnum)getElseReturnEnum(ClickFilterTypeEnum.class, key, elseEnum);
  }
  /** Return an bidirectional iterator that traverses the enumeration
   *  instances in the order they were defined.
   */
  @SuppressWarnings("unchecked")
  public static ListIterator<ClickFilterTypeEnum> iterator() {
    return getIterator(ClickFilterTypeEnum.class);
  }

  public static List<ClickFilterTypeEnum> getClickFilterTypes(ClickFilterSubTypeEnum filterSubTypeEnum) {
    List<ClickFilterTypeEnum> clickTypes = new ArrayList<ClickFilterTypeEnum>();

    for (Iterator<ClickFilterTypeEnum> iter = iterator(); iter.hasNext(); ) {
      ClickFilterTypeEnum type = iter.next();
      if (type.getClickFilterSubTypeEnum() == filterSubTypeEnum) {
        clickTypes.add(type);
      }
    }

    return clickTypes;
  }

  public static List<ClickFilterTypeEnum> getROIFilterTypes(){
    List<ClickFilterTypeEnum> roiFilters = new ArrayList<ClickFilterTypeEnum>();
    for (Iterator<ClickFilterTypeEnum> iter = iterator(); iter.hasNext(); ) {
      ClickFilterTypeEnum cf = iter.next();
      if(cf.isROIFilter()){
        roiFilters.add(cf);
      }
    }
    return roiFilters;
  }

  public boolean isROIFilter() {
    return getClickFilterSubTypeEnum().isROIFilter();
  }

  public String getLabel() {
    if(isROIFilter()){
      return getClickFilterSubTypeEnum().getLabel();
    }
    return "Pres" + getClass().getSimpleName()+".label."+ getName();
  }
  private ClickFilterSubTypeEnum m_clickFilterSubTypeEnum;

  public ClickFilterSubTypeEnum getClickFilterSubTypeEnum() {
    return m_clickFilterSubTypeEnum;
  }

  public TrafficSourceEnum getTrafficSourceEnum(){
    return m_trafficSourceEnum;
  }
}


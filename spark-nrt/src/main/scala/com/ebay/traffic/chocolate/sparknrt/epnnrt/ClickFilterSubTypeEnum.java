package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import com.ebay.kernel.BaseEnum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

@SuppressWarnings("serial")
// copy from old EPN code
public class ClickFilterSubTypeEnum extends BaseEnum {
  public static final ClickFilterSubTypeEnum NOT_ON_NQ_WHITELIST =
      new ClickFilterSubTypeEnum("NOT_ON_NQ_WHITELIST", 4);

  public static final ClickFilterSubTypeEnum NOT_REGISTERED =
      new ClickFilterSubTypeEnum("NOT_REGISTERED", 6);

  public static final ClickFilterSubTypeEnum MISSING_REFERRER_URL =
      new ClickFilterSubTypeEnum("MISSING_REFERRER_URL", 5);

  public static final ClickFilterSubTypeEnum ON_DOMAIN_BLACKLIST =
      new ClickFilterSubTypeEnum("ON_DOMAIN_BLACKLIST", 3);

  public static final ClickFilterSubTypeEnum EBAY_REFERRER =
      new ClickFilterSubTypeEnum("EBAY_REFERRER", 2);

  public static final ClickFilterSubTypeEnum SDK_NOT_USED =
      new ClickFilterSubTypeEnum("SDK_NOT_USED", 1);

  public static final ClickFilterSubTypeEnum FILTER =
      new ClickFilterSubTypeEnum("FILTER", 0, false);

  public static final ClickFilterSubTypeEnum FILTER_TRAFFIC_AND_ROI_EVENTS =
      new ClickFilterSubTypeEnum("FILTER_TRAFFIC_AND_ROI_EVENTS", 100, true);

  public static final ClickFilterSubTypeEnum UNKNOWN =
      new ClickFilterSubTypeEnum("UNKNOWN", -999, false);

  private boolean m_isROIFilter;

  //-----------------------------------------------------------------//
  // Template code follows....do not modify other than to replace    //
  // enumeration class name with the name of this class.             //
  //-----------------------------------------------------------------//
  private ClickFilterSubTypeEnum(String name, int intValue, boolean isROIFilter) {
    super(intValue, name);
    m_isROIFilter = isROIFilter;
  }

  private ClickFilterSubTypeEnum(String name, int intValue) {
    this(name, intValue, true);
  }

  // ------- Type specific interfaces -------------------------------//
  /** Get the enumeration instance for a given value or null */
  public static ClickFilterSubTypeEnum get(int key) {
    return (ClickFilterSubTypeEnum)getElseReturn(key, UNKNOWN);
  }
  /** Get the enumeration instance for a given value or return the
   *  elseEnum default.
   */
  public static ClickFilterSubTypeEnum getElseReturn(int key, ClickFilterSubTypeEnum elseEnum) {
    return (ClickFilterSubTypeEnum)getElseReturnEnum(ClickFilterSubTypeEnum.class, key, elseEnum);
  }
  /** Return an bidirectional iterator that traverses the enumeration
   *  instances in the order they were defined.
   */
  @SuppressWarnings("unchecked")
  public static ListIterator<ClickFilterSubTypeEnum> iterator() {
    return getIterator(ClickFilterSubTypeEnum.class);
  }

  public boolean isROIFilter(){
    return m_isROIFilter;
  }

  public String getLabel() {
    return "Pres" + getClass().getSimpleName()+".label."+ getName();
  }

  public static List<ClickFilterSubTypeEnum> getROIFilterTypes(){
    List<ClickFilterSubTypeEnum> roiFilters = new ArrayList<ClickFilterSubTypeEnum>();
    for (Iterator<ClickFilterSubTypeEnum> iter = iterator(); iter.hasNext(); ) {
      ClickFilterSubTypeEnum cf = iter.next();
      if(cf.isROIFilter()){
        roiFilters.add(cf);
      }
    }
    return roiFilters;
  }

}


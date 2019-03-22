package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import com.ebay.kernel.BaseEnum;

import java.util.ListIterator;

public class TrafficSourceEnum extends BaseEnum {

  private static final long serialVersionUID = -1L;

  public static final TrafficSourceEnum Classic = new TrafficSourceEnum("Classic",0);
  public static final TrafficSourceEnum MobileWeb = new TrafficSourceEnum("MobileWeb",2);
  public static final TrafficSourceEnum MobileApp = new TrafficSourceEnum("MobileApp",3);


  // Add new instances above this line

  //-----------------------------------------------------------------//
  // Template code follows....do not modify other than to replace    //
  // enumeration class name with the name of this class.             //
  //-----------------------------------------------------------------//

  private TrafficSourceEnum(String name, int intValue) {
    super(intValue, name);
  }
  // ------- Type specific interfaces -------------------------------//
  /** Get the enumeration instance for a given value or null */
  public static TrafficSourceEnum get(int key) {
    return (TrafficSourceEnum)getEnum(TrafficSourceEnum.class, key);
  }
  /** Get the enumeration instance for a given value or return the
   *  elseEnum default.
   */
  public static TrafficSourceEnum getElseReturn(int key, TrafficSourceEnum elseEnum) {
    return (TrafficSourceEnum)getElseReturnEnum(TrafficSourceEnum.class, key, elseEnum);
  }
  /** Return an bidirectional iterator that traverses the enumeration
   *  instances in the order they were defined.
   */
  @SuppressWarnings("unchecked")
  public static ListIterator<TrafficSourceEnum> iterator() {
    return getIterator(TrafficSourceEnum.class);
  }

  public String getLabel() {
    return "Pres" + getClass().getSimpleName()+".label."+ getName();
  }
}
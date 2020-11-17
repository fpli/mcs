package com.ebay.traffic.chocolate;

public enum ServiceEnum {
  DISPATCH("DISPATCH"),
  ECM("ECM"),
  OPTIMIZER("OPTIMIZER"),
  SPOCK("SPOCK"),
  KIRK("KIRK"),
  RENDER("RENDER"),
  IMAGE("IMAGE"),
  SMTP("SMTP"),
  MESSAGE_CENTER("MESSAGE_CENTER"),
  MC3("MC3"),
  MDNS("MDNS"),
  DELIVERY_SVC("DELIVERY_SVC"),
  DELSTATS("delstats"),
  MADRONA("MADRONA"),
  CHOCOLATE("CHOCOLATE");

  private final String mValue;

  ServiceEnum(String value) {
    mValue = value;
  }

  public String getValue() {
    return mValue;
  }

  public static ServiceEnum fromString(String value) {
    if (value != null) {
      for (ServiceEnum flag : ServiceEnum.values()) {
        if (value.equalsIgnoreCase(flag.mValue)) {
          return flag;
        }
      }
    }

    return ServiceEnum.DISPATCH;
  }
}

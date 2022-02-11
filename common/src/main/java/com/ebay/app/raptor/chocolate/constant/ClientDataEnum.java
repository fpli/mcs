package com.ebay.app.raptor.chocolate.constant;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * This enum is used for Client data feild
 */
public enum ClientDataEnum {
  USER_AGENT("user-agent", "Agent"),

  SEC_CH_UA_MODEL("sec-ch-ua-model", "chUaModel"),

  SEC_CH_UA_PLATFORM_VERSION("sec-ch-ua-platform-version", "chUaPlatformVersion"),

  SEC_CH_UA_FULL_VERSION("sec-ch-ua-full-version", "chUaFullVersion");

  private final String headerName;
  private final String headerAliasName;

  public String getHeaderName() {
    return headerName;
  }

  public String getHeaderAliasName() {
    return headerAliasName;
  }

  ClientDataEnum(String headerName, String headerAliasName) {
    this.headerName = headerName;
    this.headerAliasName = headerAliasName;
  }

  public static ClientDataEnum getByHeaderName(String headerName) {
    for (ClientDataEnum clientDataEnum : ClientDataEnum.values()) {
      if (clientDataEnum.headerName.equals(headerName)) {
        return clientDataEnum;
      }
    }
    return null;
  }

  public static List<ClientDataEnum> getClientHint() {
    return Arrays.asList(SEC_CH_UA_MODEL, SEC_CH_UA_PLATFORM_VERSION, SEC_CH_UA_FULL_VERSION);
  }

  public static String getClientHintHeaders() {
    StringJoiner clientHintHeaders = new StringJoiner(StringConstants.COMMA);
    clientHintHeaders.add(SEC_CH_UA_MODEL.headerName).add(SEC_CH_UA_PLATFORM_VERSION.headerName).add(SEC_CH_UA_FULL_VERSION.headerName);
    return clientHintHeaders.toString();
  }

}

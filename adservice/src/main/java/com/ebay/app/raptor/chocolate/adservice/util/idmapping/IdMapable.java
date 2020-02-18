package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import com.mysql.cj.util.StringUtils;
import org.springframework.stereotype.Service;

/**
 * Utility to add adguid to guid to the mapping system and retrive guid by adguid.
 * @author xiangli4
 */
@Service
public interface IdMapable {

  // format = guid, userid
  String format = "%s,%s";

  /**
   * Add adguid -> guid mapping
   * @param adguid adguid in cookie
   * @param guid guid from sync command
   * @return is successful or not
   */
  boolean addMapping(String adguid, String guid, String userId);

  /**
   * Get values by adguid
   * @param adguid adguid in cookie
   * @return values in String. If there is no values return empty string.
   */
  String getValues(String adguid);
  /**
   * Get guid by adguid
   * @param adguid adguid in cookie
   * @return guid in String. If there is no guid return empty string.
   */
  default String getGuid(String adguid) {

    String values = getValues(adguid);
    if(StringUtils.isNullOrEmpty(values)) {
      return "";
    }
    else {
      return values.split(",")[0];
    }
  }

  /**
   * Get user id by adguid
   * @param adguid adguid in cookie
   * @return user id in String. If there is no user id return empty string.
   */
  default String getUserid(String adguid) {
    String values = getValues(adguid);
    if(StringUtils.isNullOrEmpty(values))
      return "";
    else {
      String[] splitedValues = values.split(",");
      if(splitedValues.length > 1) {
        return splitedValues[1];
      }
      else {
        return "";
      }
    }
  }
}

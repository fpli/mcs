package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import org.springframework.stereotype.Service;

/**
 * Utility to add adguid to guid to the mapping system and retrive guid by adguid.
 * @author xiangli4
 */
@Service
public interface IdMapable {

  /**
   * Add adguid -> guid mapping
   * @param adguid adguid in cookie
   * @param guid guid from sync command
   * @return is successful or not
   */
  boolean addMapping(String adguid, String guid);

  /**
   * Get guid by adguid
   * @param adguid adguid in cookie
   * @return guid in String. If there is guid return empty string.
   */
  String getGuid(String adguid);
}

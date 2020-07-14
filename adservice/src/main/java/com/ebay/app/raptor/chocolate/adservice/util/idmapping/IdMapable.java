package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import org.springframework.stereotype.Service;

/**
 * Utility to add adguid to guid to the mapping system and retrive guid by adguid.
 * @author xiangli4
 */
@Service
public interface IdMapable {

  String ADGUID_GUID_PREFIX = "a_g_";
  String ADGUID_UID_PREFIX = "a_u_";
  String GUID_ADGUID_PREFIX = "g_a_";

  /**
   * Add adguid -> guid mapping
   * @param adguid adguid in cookie
   * @param guid guid from sync command
   * @return is successful or not
   */
  boolean addMapping(String adguid, String guid, String userId);

  /**
   * Get guid by adguid
   * @param adguid adguid in cookie
   * @return guid in String. If there is no guid return empty string.
   */
  String getGuid(String adguid);

  /**
   * Get user id by adguid
   * @param adguid adguid in cookie
   * @return user id in String. If there is no user id return empty string.
   */
  String getUid(String adguid);

  /**
   * Get adguid by guid
   * @param guid guid in cookie
   * @return adguid in String. If there is no adguid return empty string.
   */
  String getAdguid(String guid);
}

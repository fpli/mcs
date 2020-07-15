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
  String GUID_UID_PREFIX = "u_a_";
  String UID_GUID_PREFIX = "a_u_";

  /**
   * Add adguid -> guid mapping
   * @param adguid adguid in cookie
   * @param guid guid from sync command
   * @return is successful or not
   */
  boolean addMapping(String adguid, String guid, String userId);

  /**
   * Get guid by adguid
   * @param id adguid in cookie
   * @return guid in String. If there is no guid return empty string.
   */
  String getGuidByAdguid(String id);

  /**
   * Get user id by adguid
   * @param id adguid in cookie
   * @return user id in String. If there is no user id return empty string.
   */
  String getUidByAdguid(String id);

  /**
   * Get adguid by guid
   * @param id guid in cookie
   * @return adguid in String. If there is no adguid return empty string.
   */
  String getAdguidByGuid(String id);

  /**
   * Get uid by guid
   * @param id guid in cookie
   * @return uid in String. If there is no uid return empty string.
   */
  String getUidByGuid(String id);

  /**
   * Get guid by uid
   * @param id uid in cookie
   * @return guid in String. If there is no guid return empty string.
   */
  String getGuidByUid(String id);
}

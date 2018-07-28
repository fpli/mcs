package com.ebay.traffic.chocolate.mkttracksvc.constant;

public class ErrorMsgConstant {

  public static final String CB_CONNECTION_ISSUE = "Couchbase connection issue";

  public static final String CB_INSERT_ROTATION_ISSUE = "Can't create new rotation id, since rotationId already existed : ";

  public static final String CB_ACTIVATE_ROTATION_ISSUE = "Can't activate/deactivate new rotation id, since no rotationId in DB: %s";

  public static final String CB_GET_ROTATION_ISSUE_BY_ID = "Can't get any rotation information by rotationId : %s";

  public static final String CB_GET_ROTATION_ISSUE_BY_NAME = "Can't get any rotation information by rotationName : %s";

  public static final String CB_INSERT_CAMPAIGN_ISSUE = "This campaign %s already existed. CampaignName is: %s";
  public static final String CB_INSERT_CAMPAIGN_INFO = "This campaignId is not existed. you could use it.";


  public static final String ROTATION_INFO_JSON = "No rotation info was %s. Please set [%s] with correct json format.";
  public static final String ROTATION_INFO_FIELD_SAMPLE = "No rotation info was %s. Please set correct [%s]. Like: %s";
  public static final String ROTATION_INFO_REQUIRED = "No rotation info was created. [%s] is required field";
  public static final String ROTATION_INFO_REQUIRED_NUMBER  = "No rotation info was %s. [%s] can't be less than 0 or greater than %s";

  public static final String CREATED = "created";
  public static final String UPDATED = "updated";
  public static final String ACTIVATED = "activated";
  public static final String DEACTIVATED = "deactivated";
}

package com.ebay.traffic.chocolate.mkttracksvc.constant;

public class ErrorMsgConstant {

  public static final String CB_CONNECTION_ISSUE = "Couchbase connection issue";

  public static final String CB_INSERT_ROTATION_ISSUE = "Can't create new rotation id, since rotationId already existed : ";

  public static final String CB_ACTIVATE_ROTATION_ISSUE = "Can't activate/deactivate new rotation id, since no rotationId in DB: ";

  public static final String CB_GET_ROTATION_ISSUE = "Can't get any rotation information by rotationId : ";

  public static final String CB_GET_ROTATION_ISSUE2 = "Can't get any rotation information by rotationName : ";
}

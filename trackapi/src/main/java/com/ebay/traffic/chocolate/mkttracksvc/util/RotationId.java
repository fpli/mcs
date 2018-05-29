package com.ebay.traffic.chocolate.mkttracksvc.util;

import com.ebay.traffic.chocolate.mkttracksvc.constant.MPLXClientEnum;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generate rotation id which keeps legacy format and split by hyphen <br/>
 * ex: ClientId-CampaignId-CustomizedId1-CustomizedId2
 *     707-500000025-3000002-60304335
 * <br/>
 * only ClientId and CampaignId are required. other 2 ids could be automatically generated
 */
public class RotationId {
  private static final Logger logger = Logger.getLogger(RotationId.class);

  // Counter to track all incoming rotation Ids.
  private static final AtomicInteger ridCounter = new AtomicInteger(0);

  public static final String HYPHEN = "-";

  /**
   * Generate RotationId which contains 4 parts with hyphen like 477-1234-2345-12
   *
   * @param rotationReq request for rotation info
   * @return rotationId
   */
  public synchronized static String getNext(RotationInfo rotationReq) {
    long squence = ridCounter.incrementAndGet();
//    String identity = supplyDigit(rotationReq.getChannel_id(), 3) + supplyDigit(rotationReq.getSite_id(), 4);
    //Get Mediaplex ClientId for legacy rotationId support as 1st part of rotationId
    MPLXClientEnum mplxClientEnum = MPLXClientEnum.getBySiteId(rotationReq.getSite_id());
    String identity = String.valueOf(mplxClientEnum == null ? rotationReq.getSite_id() : mplxClientEnum.getMplxClientId());
    // new definition for rotation Id
    Integer driverId = DriverId.getDriverIdFromIp();
    Long campaignId = getUniqueId(rotationReq.getCampaign_id(), driverId);
    Long customizedId1 = getUniqueId(rotationReq.getCustomized_id1(), driverId);
    Long customizedId2 = getUniqueId(rotationReq.getCustomized_id2(), driverId);

    String rId = identity + HYPHEN + String.valueOf(campaignId) + HYPHEN + customizedId1 + HYPHEN + customizedId2;

    checkAndClearCounter();
    return rId;
  }

  private static Long getUniqueId(Long customizedId, Integer driverId){
    if(customizedId == null) {
      return RotationId18.getNext(driverId).getRepresentation();
    }
    return customizedId;
  }

  /**
   * Method which will check and clear the atomic counter if it exceeds some threshold. This is meant to be a poor
   * man's partial compare and swap.
   *
   * @return the counter's value. It will be set to 0 if the threshold is exceeded.
   */
  private static synchronized long checkAndClearCounter() {
    int state = ridCounter.get(); // Get the current counter.
    if (state >= Integer.MAX_VALUE) ridCounter.set(0);
    return state;
  }

  private static String supplyDigit(int original, int digit) {
    String ori = String.valueOf(original);
    if (ori.length() < digit) {
      int i = digit - ori.length();
      while (i > 0) {
        ori = "0" + ori;
        i--;
      }
    }
    return ori;
  }
}

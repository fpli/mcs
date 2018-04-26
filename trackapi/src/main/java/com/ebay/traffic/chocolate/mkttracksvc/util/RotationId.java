package com.ebay.traffic.chocolate.mkttracksvc.util;

import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import org.apache.commons.lang3.StringUtils;

/**
 *
 */
public class RotationId {
//
//  // Counter to track all incoming rotation Ids.
//  private static final AtomicInteger counter = new AtomicInteger(0);

  private static final String HYPHEN = "-";

  /**
   * Generate RotationId which contains 4 parts with hyphen like 477-1234-2345-12
   *
   * @param rotationReq request for rotation info
   * @return rotationId
   */
  public synchronized static String getNext(RotationInfo rotationReq) {
//    long squence = counter.incrementAndGet();
    String identity = supplyDigit(rotationReq.getChannel_id(), 3) + supplyDigit(rotationReq.getSite_id(), 4);
    String randomId = String.valueOf(System.currentTimeMillis() + DriverId.getDriverIdFromIp());
    String campaignId = StringUtils.isEmpty(rotationReq.getCampaign_id()) ? randomId : rotationReq.getCampaign_id();
    String customizedId1 = StringUtils.isEmpty(rotationReq.getCustomized_id1()) ? String.valueOf(Long.valueOf(randomId) + 1) : rotationReq.getCustomized_id1();
    String customizedId2 = StringUtils.isEmpty(rotationReq.getCustomized_id2()) ? String.valueOf(Long.valueOf(randomId) + 2) : rotationReq.getCustomized_id2();

    String rId = identity + HYPHEN + String.valueOf(campaignId) + HYPHEN + customizedId1 + HYPHEN + customizedId2;

//    checkAndClearCounter();
    return rId;
  }
//
//  /**
//   * Method which will check and clear the atomic counter if it exceeds some threshold. This is meant to be a poor
//   * man's partial compare and swap.
//   *
//   * @return the counter's value. It will be set to 0 if the threshold is exceeded.
//   */
//  private static synchronized long checkAndClearCounter() {
//    int state = counter.get(); // Get the current counter.
//    if (state >= Integer.MAX_VALUE) counter.set(0);
//    return state;
//  }

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

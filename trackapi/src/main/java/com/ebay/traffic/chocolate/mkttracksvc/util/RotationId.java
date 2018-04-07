package com.ebay.traffic.chocolate.mkttracksvc.util;

import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;

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
    String identity = supplyDigit(rotationReq.getChannelId(), 3) + supplyDigit(rotationReq.getSiteId(), 4);
    String randomId = String.valueOf(System.currentTimeMillis() + DriverId.getDriverIdFromIp());
    String campaignId = (rotationReq.getCampaignId() == null) ? randomId : rotationReq.getCampaignId();
    String customizedId = rotationReq.getCustomizedId() == null ? randomId : rotationReq.getCustomizedId();

    String rId = identity + HYPHEN + String.valueOf(campaignId) + HYPHEN + customizedId + HYPHEN + System.currentTimeMillis();

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

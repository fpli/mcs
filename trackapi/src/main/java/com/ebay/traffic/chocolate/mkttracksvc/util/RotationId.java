package com.ebay.traffic.chocolate.mkttracksvc.util;

import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.constant.MPLXClientEnum;
import com.ebay.traffic.chocolate.mkttracksvc.dao.imp.RotationCbDaoImp;
import com.ebay.traffic.chocolate.mkttracksvc.entity.MplxClientSite;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class RotationId {
  private static final Logger logger = Logger.getLogger(RotationId.class);

  // Counter to track all incoming rotation Ids.
  private static final AtomicInteger ridCounter = new AtomicInteger(0);

  private static final String HYPHEN = "-";

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
    String randomId = String.valueOf(System.currentTimeMillis()) + DriverId.getDriverIdFromIp() + squence;
    String campaignId = StringUtils.isEmpty(rotationReq.getCampaign_id()) ? randomId : rotationReq.getCampaign_id();
    String customizedId1 = StringUtils.isEmpty(rotationReq.getCustomized_id1()) ? String.valueOf(Long.valueOf(randomId) + 1) : rotationReq.getCustomized_id1();
    String customizedId2 = StringUtils.isEmpty(rotationReq.getCustomized_id2()) ? String.valueOf(Long.valueOf(randomId) + 2) : rotationReq.getCustomized_id2();

    String rId = identity + HYPHEN + String.valueOf(campaignId) + HYPHEN + customizedId1 + HYPHEN + customizedId2;

    checkAndClearCounter();
    return rId;
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

//  private static final String CVS_SPLITER = ",";
//
//  public static List<MplxClientSite> getStream(String filePath) throws IOException {
//    List<MplxClientSite> mplxClientEnumList = new ArrayList<MplxClientSite>();
//    BufferedReader br = null;
//    try {
//      br = new BufferedReader(new FileReader(filePath));
//      String line;
//      while ((line = br.readLine()) != null) {
//        String[] mplxClients = line.split(CVS_SPLITER);
//        MplxClientSite mplxClientSite = new MplxClientSite(Integer.valueOf(mplxClients[0]), mplxClients[1], Integer.valueOf(mplxClients[3]));
//        mplxClientEnumList.add(mplxClientSite);
//      }
//    } catch (FileNotFoundException e) {
//      e.printStackTrace();
//    } catch (IOException e) {
//      e.printStackTrace();
//    } finally {
//      if (br != null) {
//        try {
//          br.close();
//        } catch (IOException e) {
//          e.printStackTrace();
//        }
//      }
//    }
//    logger.info("Loaded " + mplxClientEnumList.size() + " clientId -> siteId mappings from file");
//    return mplxClientEnumList;
//  }
}

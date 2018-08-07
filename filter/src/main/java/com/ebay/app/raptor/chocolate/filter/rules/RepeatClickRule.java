package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

/**
 * Remember every event over a relatively short period to filter out:
 * - double clicks
 * - clicks that follow an impression too quickly
 */
public class RepeatClickRule extends BaseFilterRule {
  /**
   * Length of log for repeat click rule (ms).
   */
  public static final String FILTERING_REPEAT_CLICK_LOG_LENGTH = "chocolate.filter.repeat.loglength";
  private static final Object logSync = new Object();
  private static int timeoutMS;
  private static HashMap<Long, Long> oldLog;
  private static HashMap<Long, Long> newLog;
  private static long newLogEnds = 0;
  private static boolean initialized = false;
  
  private MessageDigest hasher;
  
  /**
   * If not yet existing, create the data structure
   */
  public RepeatClickRule(ChannelType channelType) {
    super(channelType);
    try {
      this.hasher = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new Error(e);
    }
    
    timeoutMS = this.filterRuleContent.getTimeoutMS();
    //timeoutMS = ApplicationOptions.getInstance().getByNameInteger(FILTERING_REPEAT_CLICK_LOG_LENGTH);
  }
  
  private static void init(long start) {
    synchronized (logSync) {
      if (initialized) {
        return;
      }
      
      oldLog = new HashMap<>();
      newLog = new HashMap<>();
      newLogEnds = start + timeoutMS;
      initialized = true;
    }
  }
  
  /**
   * For unit test purposes: clear the logs
   */
  public static void clear() {
    initialized = false;
  }
  
  /**
   * Test the event for repeated impression/click:
   * cut the head of the log based on the current event timestamp
   * check if the log already contains the fingerprint, return false if present
   * add to log if not present
   * <p>
   * Shared side effect.
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    long timestamp = event.getTimestamp();
    long fingerprint = calculateFingerprint(event);
    
    synchronized (logSync) {
      // Lazy init
      if (!initialized) {
        init(timestamp);
      }
      
      while (timestamp > newLogEnds) {
        oldLog = newLog;
        newLog = new HashMap<>();
        newLogEnds += timeoutMS;
      }
      
      long previousEventTS = 0;
      boolean found = false;
      
      if (event.getChannelAction() == ChannelAction.CLICK) {
        if (oldLog.containsKey(fingerprint)) {
          found = true;
          previousEventTS = oldLog.get(fingerprint);
        }
        
        if (newLog.containsKey(fingerprint)) {
          found = true;
          long newTS = newLog.get(fingerprint);
          previousEventTS = (newTS > previousEventTS) ? newTS : previousEventTS;
        }
      }
      
      newLog.put(fingerprint, timestamp);

      return (!found || timestamp - previousEventTS > timeoutMS) ? 0 : 1;
    }
  }
  
  private long calculateFingerprint(FilterRequest event) {
    StringBuilder sb = new StringBuilder();
    sb.append(event.getResponseCGUID());
    sb.append(event.getRotationID());
    sb.append(event.getCampaignId());
    
    byte[] md5 = this.hasher.digest(sb.toString().getBytes());
    
    long result = 0;
    int length = (md5.length > 8) ? 8 : md5.length;
    for (int i = 0; i < length; i++) {
      result += ((long) md5[i] & 0xffL) << (8 * i);
    }
    
    return result;
  }
}

package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.apache.commons.lang.StringUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.MalformedURLException;
import java.net.URL;

public class FilterUtils {
  private static String BID = "Bid";
  private static String BIN_ABIN = "BIN-ABIN";
  private static String BIN_FP = "BIN-FP";
  private static String BIN_STORE = "BIN-Store";
  private static final String TRANSACTION_TYPE = "transType";

  /**
   * get one param from the url query string
   * @param urlString url  string
   * @param key param name
   * @return param value
   */
  public static String getParamValueFromQuery(String urlString, String key){
    if(StringUtils.isEmpty(key)) {
      return null;
    }
    try {
      MultiValueMap<String, String> queryParams = UriComponentsBuilder.fromUriString(urlString).build().getQueryParams();
      return queryParams.get(key).get(0);
    } catch (Exception e) {
      return null;
    }
  }

  public static boolean isBESRoiTransType(String transactionType) {
    // Judging whether the transaction_type in ('Bid','BIN-ABIN','BIN-FP','BIN-Store') or not
    if (StringUtils.isEmpty(transactionType)) {
      return false;
    }
    return BID.equalsIgnoreCase(transactionType)
        || BIN_ABIN.equalsIgnoreCase(transactionType)
        || BIN_FP.equalsIgnoreCase(transactionType)
        || BIN_STORE.equalsIgnoreCase(transactionType);
  }

  public static boolean isRoverBESRoi(FilterMessage outMessage) {
    String transactionType = getParamValueFromQuery(outMessage.getUri(), TRANSACTION_TYPE);
    // Filter rule:
    // channel=ROI
    // transaction_type in ('Bid','BIN-ABIN','BIN-FP','BIN-Store')
    // rvr_url like '%ff1=ss%'
    // rvr_url like '%ff2%'
    return outMessage.getUri() != null
        && outMessage.getChannelType().toString().equals("ROI")
        && outMessage.getUri().contains("ff1=ss")
        && outMessage.getUri().contains("ff2")
        && isBESRoiTransType(transactionType);
  }
}

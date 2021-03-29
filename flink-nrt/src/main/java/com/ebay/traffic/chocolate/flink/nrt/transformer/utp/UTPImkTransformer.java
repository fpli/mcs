package com.ebay.traffic.chocolate.flink.nrt.transformer.utp;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.*;
import com.ebay.traffic.chocolate.flink.nrt.util.GenericRecordUtils;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.google.common.base.CaseFormat;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.flink.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * Transformer for all events.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/8
 */
@SuppressWarnings("unchecked")
public class UTPImkTransformer {
  public static final String GET_METHOD_PREFIX = "get";

  private static final Logger LOGGER = LoggerFactory.getLogger(UTPImkTransformer.class);

  public static final DateTimeFormatter EVENT_TS_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD_HH_MM_SS_SSS).withZone(ZoneId.systemDefault());

  public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());

  private static final Map<String, Integer> MFE_NAME_ID_MAP = new HashMap<>();

  private static final List<ImmutablePair<String, Integer>> USER_AGENT_LIST = new ArrayList<>();

  private static final List<String> USER_QUERY_PARAMS_OF_REFERRER = Collections.singletonList("q");

  private static final List<String> USER_QUERY_PARAMS_OF_LANDING_URL = Arrays.asList("uq", "satitle", "keyword", "item", "store");

  protected static final String MALFORMED_URL = "MalformedUrl";

  static {
    USER_AGENT_LIST.add(new ImmutablePair<>("roku", 15));
    USER_AGENT_LIST.add(new ImmutablePair<>("aol", 3));
    USER_AGENT_LIST.add(new ImmutablePair<>("mac", 8));
    USER_AGENT_LIST.add(new ImmutablePair<>("facebookexternalhit", 20));
    USER_AGENT_LIST.add(new ImmutablePair<>("tubidy", 14));
    USER_AGENT_LIST.add(new ImmutablePair<>("trident", 2));
    USER_AGENT_LIST.add(new ImmutablePair<>("opera", 7));
    USER_AGENT_LIST.add(new ImmutablePair<>("dvlvik", 26));
    USER_AGENT_LIST.add(new ImmutablePair<>("navigator", 1));
    USER_AGENT_LIST.add(new ImmutablePair<>("adsbot-google", 19));
    USER_AGENT_LIST.add(new ImmutablePair<>("ebayiphone", 22));
    USER_AGENT_LIST.add(new ImmutablePair<>("bingbot", 12));
    USER_AGENT_LIST.add(new ImmutablePair<>("ahc", 13));
    USER_AGENT_LIST.add(new ImmutablePair<>("webtv", 6));
    USER_AGENT_LIST.add(new ImmutablePair<>("ymobile", 16));
    USER_AGENT_LIST.add(new ImmutablePair<>("ebayandroid", 21));
    USER_AGENT_LIST.add(new ImmutablePair<>("netscape", 1));
    USER_AGENT_LIST.add(new ImmutablePair<>("UNKNOWN_USERAGENT", -99));
    USER_AGENT_LIST.add(new ImmutablePair<>("chrome", 11));
    USER_AGENT_LIST.add(new ImmutablePair<>("ucweb", 25));
    USER_AGENT_LIST.add(new ImmutablePair<>("ebaywinphocore", 24));
    USER_AGENT_LIST.add(new ImmutablePair<>("safari", 4));
    USER_AGENT_LIST.add(new ImmutablePair<>("ebayipad", 23));
    USER_AGENT_LIST.add(new ImmutablePair<>("NULL_USERAGENT", 10));
    USER_AGENT_LIST.add(new ImmutablePair<>("msie", 2));
    USER_AGENT_LIST.add(new ImmutablePair<>("msntv", 9));
    USER_AGENT_LIST.add(new ImmutablePair<>("pycurl", 17));
    USER_AGENT_LIST.add(new ImmutablePair<>("firefox", 5));
    USER_AGENT_LIST.add(new ImmutablePair<>("dailyme", 18));

    PropertyMgr.getInstance().loadAllLines("mfe_name_id_map.txt").stream()
            .map(line -> line.split("\\|"))
            .forEach(split -> MFE_NAME_ID_MAP.put(split[0], Integer.valueOf(split[1])));
  }

  private static final List<String> KEYWORD_PARAMS = Arrays.asList("_nkw", "keyword", "kw");


  /**
   * Used to cache method object to improve reflect performance
   */
  private static final Map<String, Method>  FIELD_GET_METHOD_CACHE = new ConcurrentHashMap<>(16);

  /**
   * Map field name to get method name, eg. batch_id -> getBatchId
   */
  private static final UnaryOperator<String> FIELD_GET_METHOD_MAP_FUNCTION = fieldName -> {
    String upperCamelCase = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName);
    return String.format("%s%s", GET_METHOD_PREFIX, upperCamelCase);
  };

  /**
   * Original record
   */
  protected UnifiedTrackingRheosMessage sourceRecord;
  protected String channelType;
  protected String actionType;
  protected String url;
  protected String query;
  protected String referer;
  protected Long rotationId;
  protected Map<String, String> payload;
  public static final DecimalFormat BATCH_ID_DECIMAL_FORMAT = new DecimalFormat("00");
  protected Map<String, Meter> etlMetrics;

  public UTPImkTransformer(UnifiedTrackingRheosMessage sourceRecord, String channelType, String actionType, Map<String, Meter> etlMetrics) {
    this.sourceRecord = sourceRecord;
    this.channelType = channelType;
    this.actionType = actionType;
    this.etlMetrics = etlMetrics;
    initFields();
  }

  protected void initFields() {
    url = parseUrl();
    query = parseQuery(url);
    referer = this.sourceRecord.getReferer() == null ? StringConstants.EMPTY : this.sourceRecord.getReferer();
    rotationId = parseRotationId();
    payload = this.sourceRecord.getPayload();
  }

  protected String parseUrl() {
    String sourceUrl = sourceRecord.getUrl();
    return sourceUrl == null ? StringConstants.EMPTY : sourceUrl;
  }

  protected String parseQuery(String url) {
    String q = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(url)) {
      try {
        q = new URL(url).getQuery();
        if (StringUtils.isEmpty(q)) {
          q = StringConstants.EMPTY;
        }
      } catch (Exception e) {
        etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_MALFORMED_RATE).markEvent();
        LOGGER.warn(MALFORMED_URL, e);
      }
    }
    return q;
  }

  /**
   * Set default value as -1 to keep consistent with MCS and imkETL
   * @return rotation id
   */
  @SuppressWarnings("UnstableApiUsage")
  protected Long parseRotationId() {
    String rid = sourceRecord.getRotationId();
    if (StringUtils.isEmpty(rid)) {
      return -1L;
    }
    Long aLong = Longs.tryParse(rid);
    if (aLong == null) {
      return -1L;
    }
    return aLong;
  }

  public void transform(SpecificRecordBase trackingEvent) {
    for (Schema.Field field : trackingEvent.getSchema().getFields()) {
      final String fieldName = field.name();
      if (fieldName.equals("rheosHeader")) {
        continue;
      }
      Object value = getField(fieldName);
      trackingEvent.put(fieldName, value);
    }
  }

  /**
   * Get field from source record
   *
   * @param fieldName field name
   * @return value
   */
  protected Object getField(String fieldName) {
    Method method = findMethod(fieldName);
    Object value;
    try {
      value = method.invoke(this);
    } catch (IllegalAccessException | InvocationTargetException e) {
      String message = String.format("invoke method %s failed, raw message %s", method, this.sourceRecord);
      throw new RuntimeException(message, e);
    }
    Validate.notNull(value, String.format("%s is null, raw message %s", fieldName, this.sourceRecord));
    return value;
  }

  private Method findMethod(String fieldName) {
    if (UTPImkTransformer.FIELD_GET_METHOD_CACHE.containsKey(fieldName)) {
      return UTPImkTransformer.FIELD_GET_METHOD_CACHE.get(fieldName);
    }
    String methodName = UTPImkTransformer.FIELD_GET_METHOD_MAP_FUNCTION.apply(fieldName);
    Method method = findMethodByName(methodName);
    Validate.notNull(method, String.format("cannot find method %s, raw message %s", methodName, this.sourceRecord));
    UTPImkTransformer.FIELD_GET_METHOD_CACHE.put(fieldName, method);
    return UTPImkTransformer.FIELD_GET_METHOD_CACHE.get(fieldName);
  }

  private Method findMethodByName(String getMethodName) {
    Method getMethod = null;
    for (Method declaredMethod : UTPImkTransformer.class.getDeclaredMethods()) {
      if (declaredMethod.getName().equals(getMethodName)) {
        getMethod = declaredMethod;
        break;
      }
    }
    return getMethod;
  }

  protected String getBatchId() {
    LocalDateTime date = LocalDateTime.now();
    return String.format("%s%s%s", BATCH_ID_DECIMAL_FORMAT.format(date.getHour()), BATCH_ID_DECIMAL_FORMAT.format(date.getMinute()), BATCH_ID_DECIMAL_FORMAT.format(date.getSecond()));
  }

  protected Integer getFileId() {
    return 0;
  }

  protected Integer getFileSchmVrsn() {
    return 4;
  }

  /**
   * Get rvr_id from payload.
   * @return rvr id
   */
  protected Long getRvrId() {
    return parseRvrId(payload.get("rvrid"));
  }

  @SuppressWarnings("UnstableApiUsage")
  protected Long parseRvrId(String rvrId) {
    if (StringUtils.isEmpty(rvrId)) {
      return 0L;
    }
    Long aLong = Longs.tryParse(rvrId);
    if (aLong == null) {
      return 0L;
    }
    return aLong;
  }

  protected String getEventDt() {
    return EVENT_DT_FORMATTER.format(Instant.ofEpochMilli((Long) sourceRecord.get("eventTs")));
  }

  protected Integer getSrvdPstn() {
    return 0;
  }

  protected String getRvrCmndTypeCd() {
    if (ActionTypeEnum.IMPRESSION.getValue().equals(actionType)) {
      return RvrCmndTypeCdEnum.IMPRESSION.getCd();
    }
    if (ActionTypeEnum.ROI.getValue().equals(actionType)) {
      return RvrCmndTypeCdEnum.ROI.getCd();
    }
    if (ActionTypeEnum.SERVE.getValue().equals(actionType)) {
      return RvrCmndTypeCdEnum.SERVE.getCd();
    }
    return RvrCmndTypeCdEnum.CLICK.getCd();
  }

  protected String getRvrChnlTypeCd() {
    if (ChannelTypeEnum.EPN.getValue().equals(channelType)) {
      return RvrChnlTypeCdEnum.EPN.getCd();
    }
    if (ChannelTypeEnum.PLA.getValue().equals(channelType)) {
      return RvrChnlTypeCdEnum.PAID_SEARCH.getCd();
    }
    if (ChannelTypeEnum.TEXT.getValue().equals(channelType)) {
      return RvrChnlTypeCdEnum.PAID_SEARCH.getCd();
    }
    if (ChannelTypeEnum.DISPLAY.getValue().equals(channelType)) {
      return RvrChnlTypeCdEnum.DISPLAY.getCd();
    }
    if (ChannelTypeEnum.SOCIAL.getValue().equals(channelType)) {
      return RvrChnlTypeCdEnum.SOCIAL_MEDIA.getCd();
    }
    if (ChannelTypeEnum.SEARCH_ENGINE_FREE_LISTINGS.getValue().equals(channelType)) {
      return RvrChnlTypeCdEnum.SEARCH_ENGINE_FREE_LISTINGS.getCd();
    }
    return RvrChnlTypeCdEnum.DEFAULT.getCd();
  }

  protected String getCntryCd() {
    return StringConstants.EMPTY;
  }

  protected String getLangCd() {
    return StringConstants.EMPTY;
  }

  protected Integer getTrckngPrtnrId() {
    return 0;
  }

  public String getCguid() {
    return StringConstants.EMPTY;
  }

  public String getGuid() {
    return GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.GUID);
  }

  public Long getUserId() {
    Long userId = (Long) sourceRecord.get("userId");
    if (userId == null) {
      return 0L;
    }
    return userId;
  }

  protected String getClntRemoteIp() {
    return GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, "remoteIp");
  }

  protected Integer getBrwsrTypeId() {
    String userAgent = sourceRecord.getUserAgent();
    if (StringUtils.isEmpty(userAgent)) {
      return UserAgentEnum.UNKNOWN_USERAGENT.getId();
    }

    if (StringUtils.isNotEmpty(userAgent)) {
      String agentStr = userAgent.toLowerCase();
      for (ImmutablePair<String, Integer> pair : USER_AGENT_LIST) {
        if (agentStr.contains(pair.getKey())) {
          return pair.getValue();
        }
      }
    }
    return UserAgentEnum.UNKNOWN_USERAGENT.getId();
  }

  protected String getBrwsrName() {
    return GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, "userAgent");
  }

  protected String getRfrrDmnName() {
    return getDomain(referer);
  }

  protected String getRfrrUrl() {
    return referer;
  }

  protected Integer getUrlEncrptdYnInd() {
    return 0;
  }

  protected Long getSrcRotationId() {
    return rotationId;
  }

  protected Long getDstRotationId() {
    return getSrcRotationId();
  }

  protected Integer getDstClientId() {
    return getClientIdFromRotationId(getParamValueFromQuery(query, TransformerConstants.MKRID));
  }

  protected String getPblshrId() {
    return StringConstants.EMPTY;
  }

  protected String getLndngPageDmnName() {
    return getDomain(url);
  }

  private String getDomain(String link) {
    String result = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(link)) {
      try {
        result = new URL(link).getHost();
      } catch (Exception e) {
        etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_MALFORMED_RATE).markEvent();
        LOGGER.warn(MALFORMED_URL, e);
      }
    }
    return result;
  }

  /**
   * Campaign Manager changes the url template for all PLA accounts, replace adtype=pla and*adgroupid=65058347419* with
   * new parameter mkgroupid={adgroupid} and mktype={adtype}. Tracking’s MCS data pipeline job replace back to adtype
   * and adgroupid and persist into IMK so there won't be impact to downstream like data and science.
   * See <a href="https://jirap.corp.ebay.com/browse/XC-1464">replace landing page url and rvr_url's mktype and mkgroupid</a>
   * @return new uri
   */
  protected String getLndngPageUrl() {
    String newUri = replaceMkgroupidMktype();
    if (newUri.startsWith("http://rover.ebay.com") || newUri.startsWith("https://rover.ebay.com")) {
      String newqQuery = parseQuery(newUri);
      String landingPageUrl = getParamValueFromQuery(newqQuery, "mpre");
      if (StringUtils.isNotEmpty(landingPageUrl)) {
        try{
          newUri = URLDecoder.decode(landingPageUrl, "UTF-8");
        } catch (Exception e) {
          etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_MALFORMED_RATE).markEvent();
          LOGGER.warn(MALFORMED_URL, e);
        }

      }
    }
    return newUri;
  }

  private String replaceMkgroupidMktype() {
    return url.replace(TransformerConstants.MKGROUPID, TransformerConstants.ADGROUPID).replace(TransformerConstants.MKTYPE, TransformerConstants.ADTYPE);
  }

  protected String getUserQuery() {
    String result = StringConstants.EMPTY;
    try {
      if (StringUtils.isNotEmpty(referer)) {
        String userQueryFromReferrer = getParamFromQuery(parseQuery(referer.toLowerCase()), USER_QUERY_PARAMS_OF_REFERRER);
        if (StringUtils.isNotEmpty(userQueryFromReferrer)) {
          result = userQueryFromReferrer;
        } else {
          result = getParamFromQuery(query.toLowerCase(), USER_QUERY_PARAMS_OF_LANDING_URL);
        }
      } else {
        result = StringConstants.EMPTY;
      }
    } catch (Exception e) {
      etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_GET_USER_QUERY_RATE).markEvent();
      LOGGER.warn("ErrorGetQuery", e);
    }
    return result;
  }

  public String getRuleBitFlagStrng() {
    return StringConstants.EMPTY;
  }

  public Integer getFlexFieldVrsnNum() {
    return 0;
  }

  public String getFlexField1() {
    return getParamValueFromQuery(query, "ff1");
  }

  public String getFlexField2() {
    return getParamValueFromQuery(query, "ff2");
  }

  public String getFlexField3() {
    return getParamValueFromQuery(query, "ff3");
  }

  public String getFlexField4() {
    return getParamValueFromQuery(query, "ff4");
  }

  public String getFlexField5() {
    return getParamValueFromQuery(query, "ff5");
  }

  public String getFlexField6() {
    return getParamValueFromQuery(query, "ff6");
  }

  public String getFlexField7() {
    return getParamValueFromQuery(query, "ff7");
  }

  public String getFlexField8() {
    return getParamValueFromQuery(query, "ff8");
  }

  public String getFlexField9() {
    return getParamValueFromQuery(query, "ff9");
  }

  public String getFlexField10() {
    return getParamValueFromQuery(query, "ff10");
  }

  public String getFlexField11() {
    return getParamValueFromQuery(query, "ff11");
  }

  public String getFlexField12() {
    return getParamValueFromQuery(query, "ff12");
  }

  public String getFlexField13() {
    return getParamValueFromQuery(query, "ff13");
  }

  public String getFlexField14() {
    return getParamValueFromQuery(query, "ff14");
  }

  public String getFlexField15() {
    return getParamValueFromQuery(query, "ff15");
  }

  public String getFlexField16() {
    return getParamValueFromQuery(query, "ff16");
  }

  public String getFlexField17() {
    return getParamValueFromQuery(query, "ff17");
  }

  public String getFlexField18() {
    return getParamValueFromQuery(query, "ff18");
  }

  public String getFlexField19() {
    return getParamValueFromQuery(query, "ff19");
  }

  public String getFlexField20() {
    return getParamValueFromQuery(query, "ff20");
  }

  private String getParamFromQuery(String query, List<String> keys) {
    try {
      if (StringUtils.isNotEmpty(query)) {
        for (String paramMapString : query.split(StringConstants.AND)) {
          String[] paramMapStringArray = paramMapString.split(StringConstants.EQUAL);
          String param = paramMapStringArray[0];
          for (String key : keys) {
            if (key.equalsIgnoreCase(param) && paramMapStringArray.length == 2) {
              return paramMapStringArray[1];
            }
          }
        }
      }
    } catch (Exception e) {
      etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_GET_PARAM_FROM_QUERY_RATE).markEvent();
      LOGGER.warn(MALFORMED_URL, e);
    }
    return StringConstants.EMPTY;
  }

  public String getEventTs() {
    return EVENT_TS_FORMATTER.format(Instant.ofEpochMilli((Long) sourceRecord.get("eventTs")));
  }

  protected Integer getDfltBhrvId() {
    return 0;
  }

  protected String getPerfTrackNameValue() {
    StringBuilder buf = new StringBuilder();
    try {
      if (StringUtils.isNotEmpty(query)) {
        for (String paramMapString : query.split(StringConstants.AND)) {
          String[] paramStringArray = paramMapString.split(StringConstants.EQUAL);
          if (paramStringArray.length == 2) {
            buf.append(StringConstants.CARET).append(paramMapString);
          }
        }
      }
    } catch (Exception e) {
      etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_GET_PERF_TRACK_NAME_VALUE_RATE).markEvent();
      LOGGER.warn(MALFORMED_URL, e);
    }
    return buf.toString();
  }

  public String getKeyword() {
    return getParamFromQuery(query, KEYWORD_PARAMS);
  }

  public Long getKwId() {
    return -999L;
  }

  public String getMtId() {
    return getDefaultNullNumParamValueFromQuery(query, TransformerConstants.MT_ID);
  }

  public String getGeoId() {
    Integer geoId = (Integer) sourceRecord.get("geoId");
    if (geoId == null) {
      return StringConstants.ZERO;
    }
    return String.valueOf(geoId);
  }

  private String getDefaultNullNumParamValueFromQuery(String query, String key) {
    String result = StringConstants.EMPTY;
    try {
      if (StringUtils.isNotEmpty(query)) {
        for (String paramMapString : query.split(StringConstants.AND)) {
          String[] paramStringArray = paramMapString.split(StringConstants.EQUAL);
          if (paramStringArray[0].trim().equalsIgnoreCase(key) && paramStringArray.length == 2) {
            if (StringUtils.isNumeric(paramStringArray[1].trim())) {
              result = paramStringArray[1].trim();
            }
          }
        }
      }
    } catch (Exception e) {
      etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_PARSE_MT_ID_RATE).markEvent();
      LOGGER.warn("ParseMtidError", e);
      LOGGER.warn("ParseMtidError query {}", query);
    }
    return result;
  }

  public String getCrlp() {
    return getParamValueFromQuery(query, TransformerConstants.CRLP);
  }

  protected String getParamValueFromQuery(String query, String key) {
    String result = StringConstants.EMPTY;
    try {
      if (StringUtils.isNotEmpty(query)) {
        for (String paramMapString : query.split(StringConstants.AND)) {
          String[] paramStringArray = paramMapString.split(StringConstants.EQUAL);
          if (ArrayUtils.isNotEmpty(paramStringArray) && paramStringArray[0].trim().equalsIgnoreCase(key) && paramStringArray.length == 2) {
            result = paramStringArray[1].trim();
          }
        }
      }
    } catch (Exception e) {
      etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_GET_PARAM_VALUE_FROM_QUERY_RATE).markEvent();
      LOGGER.warn(MALFORMED_URL, e);
    }
    return result;
  }

  protected String getMfeName() {
    return getParamValueFromQuery(query, TransformerConstants.CRLP);
  }

  protected Integer getMfeId() {
    String mfeName = getMfeName();
    if (StringUtils.isNotEmpty(mfeName)) {
      return MFE_NAME_ID_MAP.getOrDefault(mfeName, -999);
    } else {
      return -999;
    }
  }

  protected String getUserMapInd() {
    String userId = String.valueOf(getUserId());
    if (StringUtils.isEmpty(userId) || userId.equals(StringConstants.ZERO)) {
      return StringConstants.ZERO;
    } else {
      return StringConstants.ONE;
    }
  }

  protected Long getCreativeId() {
    return -999L;
  }

  protected Integer getTestCtrlFlag() {
    return 0;
  }

  protected String getTransactionType() {
    return StringConstants.EMPTY;
  }

  protected String getTransactionId() {
    return StringConstants.EMPTY;
  }

  protected String getItemId() {
    String itemId = getItemIdFromUri(url);

    if (StringUtils.isNotEmpty(itemId) && itemId.length() <= 18) {
      return itemId;
    } else{
      return StringConstants.EMPTY;
    }
  }

  protected String getRoiItemId() {
    return StringConstants.EMPTY;
  }

  protected String getCartId() {
    return StringConstants.EMPTY;
  }

  protected String getExtrnlCookie() {
    return StringConstants.EMPTY;
  }

  protected String getEbaySiteId() {
    return StringConstants.EMPTY;
  }

  protected String getRvrUrl() {
    return replaceMkgroupidMktype();
  }

  protected String getMgvalue() {
    return StringConstants.EMPTY;
  }

  protected String getMgvaluereason() {
    return StringConstants.EMPTY;
  }

  protected String getMgvalueRsnCd() {
    return StringConstants.EMPTY;
  }

  @SuppressWarnings("UnstableApiUsage")
  protected Integer getClientIdFromRotationId(String rotationId) {
    Integer result;
    if (StringUtils.isNotEmpty(rotationId)
            && rotationId.length() <= 25
            && StringUtils.isNumeric(rotationId.replace(StringConstants.HYPHEN, StringConstants.EMPTY))
            && rotationId.contains(StringConstants.HYPHEN)) {
      result = Ints.tryParse(rotationId.substring(0, rotationId.indexOf(StringConstants.HYPHEN)));
    } else {
      return 0;
    }
    if (result == null) {
      etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_PARSE_CLIENT_ID_RATE).markEvent();
      LOGGER.warn("cannot parse client id");
      return 0;
    } else {
      return result;
    }
  }

  private String getItemIdFromUri(String uri) {
    String path;
    try {
      path = new URL(uri).getPath();
      if (StringUtils.isNotEmpty(path) && (path.startsWith("/itm/") || path.startsWith("/i/"))) {
        String itemId = path.substring(path.lastIndexOf(StringConstants.SLASH) + 1);
        if (StringUtils.isNumeric(itemId)) {
          return itemId;
        }
      }
    } catch (Exception e) {
      etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_MALFORMED_RATE).markEvent();
      LOGGER.warn(MALFORMED_URL, e);
    }
    return StringConstants.EMPTY;
  }

  protected String getCreDate() {
    return StringConstants.EMPTY;
  }

  protected String getCreUser() {
    return StringConstants.EMPTY;
  }

  protected String getUpdDate() {
    return StringConstants.EMPTY;
  }

  protected String getUpdUser() {
    return StringConstants.EMPTY;
  }
}

package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.kernel.patternmatch.dawg.Dawg;
import com.ebay.kernel.patternmatch.dawg.DawgDictionary;
import com.ebay.traffic.chocolate.flink.nrt.constant.*;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.monitoring.ESMetrics;
import com.google.common.base.CaseFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

import static com.ebay.traffic.chocolate.flink.nrt.constant.MetricConstants.METRIC_IMK_DUMP_MALFORMED;

public class BaseTransformer {
  public static final String GET_METHOD_PREFIX = "get";
  public static final String SET_METHOD_PREFIX = "set";

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTransformer.class);

  public static final DateTimeFormatter EVENT_TS_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD_HH_MM_SS_SSS).withZone(ZoneId.systemDefault());

  public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());

  private static final Map<String, Integer> MFE_NAME_ID_MAP = new HashMap<String, Integer>() {
    {
      PropertyMgr.getInstance().loadAllLines("mfe_name_id_map.txt").stream()
              .map(line -> line.split("\\|")).forEach(split -> put(split[0], Integer.valueOf(split[1])));
    }
  };

  protected static DawgDictionary userAgentBotDawgDictionary = new DawgDictionary(
          PropertyMgr.getInstance().loadAllLines("dap_user_agent_robot.txt").toArray(new String[0]),
          true);

  protected static DawgDictionary ipBotDawgDictionary = new DawgDictionary(
          PropertyMgr.getInstance().loadAllLines("dap_ip_robot.txt").toArray(new String[0]),
          true);

  private static final List<String> USER_QUERY_PARAMS_OF_REFERRER = Collections.singletonList("q");

  private static final List<String> USER_QUERY_PARAMS_OF_LANDING_URL = Arrays.asList("uq", "satitle", "keyword", "item", "store");

  public static final String MALFORMED_URL = "MalformedUrl";

  /**
   * Used to cache temp fields
   */
  private final Map<String, Object> fieldCache = new HashMap<>(16);

  /**
   * Used to cache method object to improve reflect performance
   */
  private static final Map<String, Method> FIELD_GET_METHOD_CACHE = new HashMap<>(16);

  /**
   * Map field name to get method name, eg. batch_id -> getBatchId
   */
  private static final Function<String, String> FIELD_GET_METHOD_MAP_FUNCTION = fieldName -> {
    String upperCamelCase = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName);
    return String.format("%s%s", GET_METHOD_PREFIX, upperCamelCase);
  };

  /**
   * Map field name to set method name, eg. batch_id -> setBatchId
   */
  private static final Function<String, String> FIELD_SET_METHOD_MAP_FUNCTION = fieldName -> {
    String upperCamelCase = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName);
    return String.format("%s%s", SET_METHOD_PREFIX, upperCamelCase);
  };

  /**
   * Original record
   */
  protected GenericRecord sourceRecord;
  public static final DecimalFormat BATCH_ID_DECIMAL_FORMAT = new DecimalFormat("00");

  public BaseTransformer(FilterMessage sourceRecord) {
    this.sourceRecord = sourceRecord;
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
    if (fieldCache.containsKey(fieldName)) {
      return fieldCache.get(fieldName);
    }
    Method method = findMethod(fieldName);
    Object value;
    try {
      value = method.invoke(this);
    } catch (IllegalAccessException | InvocationTargetException e) {
      LOGGER.error("invoke method {} failed", method);
      throw new RuntimeException(e);
    }
    Validate.notNull(value, String.format("%s is null", fieldName));
    fieldCache.put(fieldName, value);
    return fieldCache.get(fieldName);
  }

  private Method findMethod(String fieldName) {
    if (BaseTransformer.FIELD_GET_METHOD_CACHE.containsKey(fieldName)) {
      return BaseTransformer.FIELD_GET_METHOD_CACHE.get(fieldName);
    }
    String methodName = BaseTransformer.FIELD_GET_METHOD_MAP_FUNCTION.apply(fieldName);
    Method method = findMethodByName(methodName);
    Validate.notNull(method, String.format("cannot find method %s", methodName));
    BaseTransformer.FIELD_GET_METHOD_CACHE.put(fieldName, method);
    return BaseTransformer.FIELD_GET_METHOD_CACHE.get(fieldName);
  }

  private Method findMethodByName(String getMethodName) {
    Method getMethod = null;
    for (Method declaredMethod : BaseTransformer.class.getDeclaredMethods()) {
      if (declaredMethod.getName().equals(getMethodName)) {
        getMethod = declaredMethod;
        break;
      }
    }
    return getMethod;
  }

  @SuppressWarnings("unchecked")
  protected String getTempUriQuery() {
    String query = StringConstants.EMPTY;
    String uri = (String) sourceRecord.get(TransformerConstants.URI);
    if (StringUtils.isNotEmpty(uri)) {
      try {
        query = new URL(uri).getQuery();
        if (StringUtils.isEmpty(query)) {
          query = StringConstants.EMPTY;
        }
      } catch (Exception e) {
        ESMetrics.getInstance().meter(METRIC_IMK_DUMP_MALFORMED, 1);
        LOGGER.warn(MALFORMED_URL, e);
      }
    }
    return query;
  }

  protected String getBatchId() {
    LocalDateTime date = LocalDateTime.now();
    return BATCH_ID_DECIMAL_FORMAT.format(date.getHour()) + BATCH_ID_DECIMAL_FORMAT.format(date.getMinute()) + BATCH_ID_DECIMAL_FORMAT.format(date.getSecond());
  }

  protected Integer getFileId() {
    return 0;
  }

  protected Integer getFileSchmVrsn() {
    return 4;
  }

  protected Long getRvrId() {
    return (Long) sourceRecord.get(TransformerConstants.SHORT_SNAPSHOT_ID);
  }

  protected String getEventDt() {
    return EVENT_DT_FORMATTER.format(Instant.ofEpochMilli((Long) sourceRecord.get(TransformerConstants.TIMESTAMP)));
  }

  protected Integer getSrvdPstn() {
    return 0;
  }

  protected String getRvrCmndTypeCd() {
    ChannelAction channelAction = (ChannelAction) sourceRecord.get(TransformerConstants.CHANNEL_ACTION);
    switch (channelAction) {
      case IMPRESSION:
        return RvrCmndTypeCdEnum.IMPRESSION.getCd();
      case ROI:
        return RvrCmndTypeCdEnum.ROI.getCd();
      case SERVE:
        return RvrCmndTypeCdEnum.SERVE.getCd();
      default:
        return RvrCmndTypeCdEnum.CLICK.getCd();
    }
  }

  protected String getRvrChnlTypeCd() {
    ChannelType channelType = (ChannelType) sourceRecord.get(TransformerConstants.CHANNEL_TYPE);
    switch (channelType) {
      case EPN:
        return RvrChnlTypeCdEnum.EPN.getCd();
      case DISPLAY:
        return RvrChnlTypeCdEnum.DISPLAY.getCd();
      case PAID_SEARCH:
        return RvrChnlTypeCdEnum.PAID_SEARCH.getCd();
      case SOCIAL_MEDIA:
        return RvrChnlTypeCdEnum.SOCIAL_MEDIA.getCd();
      case PAID_SOCIAL:
        return RvrChnlTypeCdEnum.PAID_SOCIAL.getCd();
      case NATURAL_SEARCH:
        return RvrChnlTypeCdEnum.NATURAL_SEARCH.getCd();
      default:
        return RvrChnlTypeCdEnum.DEFAULT.getCd();
    }
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

  // TODO
  public String getCguid() {
    return StringConstants.EMPTY;
  }

  public String getGuid() {
    return (String) sourceRecord.get(TransformerConstants.GUID);
  }

  // TODO
  public Long getUserId() {
    return 0L;
  }

  protected String getClntRemoteIp() {
    return (String) sourceRecord.get(TransformerConstants.REMOTE_IP);
  }

  protected Integer getBrwsrTypeId() {
    String userAgent = (String) sourceRecord.get(TransformerConstants.USER_AGENT);
    if (StringUtils.isEmpty(userAgent)) {
      return UserAgentEnum.UNKNOWN_USERAGENT.getId();
    }
    String agentStr = userAgent.toLowerCase();
    for (UserAgentEnum userAgentEnum : UserAgentEnum.values()) {
      if (agentStr.contains(userAgentEnum.getName())) {
        return userAgentEnum.getId();
      }
    }
    return UserAgentEnum.UNKNOWN_USERAGENT.getId();
  }

  protected String getBrwsrName() {
    return (String) sourceRecord.get(TransformerConstants.USER_AGENT);
  }

  protected String getRfrrDmnName() {
    String link = (String) sourceRecord.get(TransformerConstants.REFERER);
    return getDomain(link);
  }

  protected String getRfrrUrl() {
    return (String) sourceRecord.get(TransformerConstants.REFERER);
  }

  protected Integer getUrlEncrptdYnInd() {
    return 0;
  }

  protected Long getSrcRotationId() {
    return (Long) sourceRecord.get(TransformerConstants.SRC_ROTATION_ID);
  }

  protected Long getDstRotationId() {
    return (Long) sourceRecord.get(TransformerConstants.DST_ROTATION_ID);
  }

  public Integer getDstClientId() {
    String tempUriQuery = getTempUriQuery();
    String paramValueFromQuery = getParamValueFromQuery(tempUriQuery, TransformerConstants.MKRID);
    return getClientIdFromRotationId(paramValueFromQuery);
  }

  public String getPblshrId() {
    return StringConstants.EMPTY;
  }

  protected String getLndngPageDmnName() {
    String link = (String) sourceRecord.get(TransformerConstants.URI);
    return getDomain(link);
  }

  @SuppressWarnings("unchecked")
  private String getDomain(String link) {
    String result = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(link)) {
      try {
        result = new URL(link).getHost();
      } catch (Exception e) {
        ESMetrics.getInstance().meter(METRIC_IMK_DUMP_MALFORMED, 1);
        LOGGER.warn(MALFORMED_URL, e);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  protected String getLndngPageUrl() {
    String channelType = sourceRecord.get(TransformerConstants.CHANNEL_TYPE).toString();
    String uri = (String) sourceRecord.get(TransformerConstants.URI);
    String newUri = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(uri)) {
      try {
        newUri = uri.replace(TransformerConstants.MKGROUPID, TransformerConstants.ADGROUPID).replace(TransformerConstants.MKTYPE, TransformerConstants.ADTYPE);
      } catch (Exception e) {
        ESMetrics.getInstance().meter(METRIC_IMK_DUMP_MALFORMED, 1, com.ebay.traffic.monitoring.Field.of("channelType", channelType));
        LOGGER.warn(MALFORMED_URL, e);
      }
    }
    return newUri;
  }

  @SuppressWarnings("unchecked")
  public String getUserQuery() {
    String referrer = (String) sourceRecord.get(TransformerConstants.REFERER);
    String query = getTempUriQuery();
    String result = StringConstants.EMPTY;
    try {
      if (StringUtils.isNotEmpty(referrer)) {
        String userQueryFromReferrer = getParamFromQuery(getQueryString(referrer.toLowerCase()), USER_QUERY_PARAMS_OF_REFERRER);
        if (StringUtils.isNotEmpty(userQueryFromReferrer)) {
          result = userQueryFromReferrer;
        } else {
          result = getParamFromQuery(query.toLowerCase(), USER_QUERY_PARAMS_OF_LANDING_URL);
        }
      } else {
        result = StringConstants.EMPTY;
      }
    } catch (Exception e) {
      ESMetrics.getInstance().meter(MetricConstants.METRIC_IMK_DUMP_ERROR_GET_QUERY, 1);
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
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff1");
  }

  public String getFlexField2() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff2");
  }

  public String getFlexField3() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff3");
  }

  public String getFlexField4() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff4");
  }

  public String getFlexField5() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff5");
  }

  public String getFlexField6() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff6");
  }

  public String getFlexField7() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff7");
  }

  public String getFlexField8() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff8");
  }

  public String getFlexField9() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff9");
  }

  public String getFlexField10() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff10");
  }

  public String getFlexField11() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff11");
  }

  public String getFlexField12() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff12");
  }

  public String getFlexField13() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff13");
  }

  public String getFlexField14() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff14");
  }

  public String getFlexField15() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff15");
  }

  public String getFlexField16() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff16");
  }

  public String getFlexField17() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff17");
  }

  public String getFlexField18() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff18");
  }

  public String getFlexField19() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff19");
  }

  public String getFlexField20() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "ff20");
  }

  @SuppressWarnings("unchecked")
  private String getQueryString(String uri) {
    String query = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(uri)) {
      try {
        query = new URL(uri).getQuery();
        if (StringUtils.isEmpty(query)) {
          query = StringConstants.EMPTY;
        }
      } catch (Exception e) {
        ESMetrics.getInstance().meter(METRIC_IMK_DUMP_MALFORMED, 1);
        LOGGER.warn("MalformedUrl", e);
      }
    }
    return query;
  }

  @SuppressWarnings("unchecked")
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
      ESMetrics.getInstance().meter(MetricConstants.METRIC_IMK_DUMP_ERROR_GET_PARAM_FROM_QUERY, 1);
      LOGGER.warn(MALFORMED_URL, e);
    }
    return StringConstants.EMPTY;
  }

  public String getEventTs() {
    return EVENT_TS_FORMATTER.format(Instant.ofEpochMilli((Long) sourceRecord.get(TransformerConstants.TIMESTAMP)));
  }

  protected Integer getDfltBhrvId() {
    return 0;
  }

  @SuppressWarnings("unchecked")
  private String getPerfTrackNameValue() {
    String query = getTempUriQuery();
    StringBuilder buf = new StringBuilder();
    try {
      if (StringUtils.isNotEmpty(query)) {
        for (String paramMapString : query.split(StringConstants.AND)) {
          String[] paramStringArray = paramMapString.split(StringConstants.EQUAL);
          if (paramStringArray.length == 2) {
            buf.append(String.format(StringConstants.CARET, paramMapString));
          }
        }
      }
    } catch (Exception e) {
      ESMetrics.getInstance().meter(MetricConstants.METRIC_IMK_DUMP_ERROR_GET_PERF_TRACK_NAME_VALUE, 1);
      LOGGER.warn(MALFORMED_URL, e);
    }
    return buf.toString();
  }

  private static final List<String> KEYWORD_PARAMS = Arrays.asList("_nkw", "keyword", "kw");


  public String getKeyword() {
    String query = getTempUriQuery();
    return getParamFromQuery(query, KEYWORD_PARAMS);
  }

  // TODO
  public Long getKwId() {
    return -999L;
  }

  public String getMtId() {
    String query = getTempUriQuery();
    return getDefaultNullNumParamValueFromQuery(query, TransformerConstants.MT_ID);
  }

  public String getGeoId() {
    return String.valueOf(sourceRecord.get(TransformerConstants.GEO_ID));
  }

  @SuppressWarnings("unchecked")
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
      ESMetrics.getInstance().meter(MetricConstants.METRIC_IMK_DUMP_PARSEMTID_ERROR, 1);
      LOGGER.warn("ParseMtidError", e);
      LOGGER.warn("ParseMtidError query {}", query);
    }
    return result;
  }

  public String getCrlp() {
    String query = getTempUriQuery();
    return getParamValueFromQuery(query, TransformerConstants.CRLP);
  }

  @SuppressWarnings("unchecked")
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
      ESMetrics.getInstance().meter(MetricConstants.METRIC_IMK_DUMP_ERROR_GET_PARAM_VALUE_FROM_QUERY, 1);
      LOGGER.warn(MALFORMED_URL, e);
    }
    return result;
  }

  protected String getMfeName() {
    String query = getTempUriQuery();
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
    String userId = String.valueOf(sourceRecord.get(TransformerConstants.USER_ID));
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
    String uri = (String) sourceRecord.get(TransformerConstants.URI);
    String itemId = getItemIdFromUri(uri);

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
    return getLndngPageUrl();
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

  /**
   * Check if this request is bot with brwsr_name
   * @param brwsrName alias for user agent
   * @return is bot or not
   */
  protected boolean isBotByUserAgent(String brwsrName, DawgDictionary userAgentBotDawgDictionary) {
    return isBot(brwsrName, userAgentBotDawgDictionary);
  }

  /**
   * Check if this request is bot with clnt_remote_ip
   * @param clntRemoteIp alias for remote ip
   * @return is bot or not
   */
  protected boolean isBotByIp(String clntRemoteIp, DawgDictionary ipBotDawgDictionary) {
    return isBot(clntRemoteIp, ipBotDawgDictionary);
  }

  @SuppressWarnings("rawtypes")
  protected boolean isBot(String info, DawgDictionary dawgDictionary) {
    if (StringUtils.isEmpty(info)) {
      return false;
    } else {
      Dawg dawg = new Dawg(dawgDictionary);
      Map result = dawg.findAllWords(info.toLowerCase(), false);
      return !result.isEmpty();
    }
  }

  @SuppressWarnings("unchecked")
  public Integer getClientIdFromRotationId(String rotationId) {
    int result = 0;
    try {
      if (StringUtils.isNotEmpty(rotationId)
              && rotationId.length() <= 25
              && StringUtils.isNumeric(rotationId.replace(StringConstants.HYPHEN, StringConstants.EMPTY))
              && rotationId.contains(StringConstants.HYPHEN)) {
        result = Integer.parseInt(rotationId.substring(0, rotationId.indexOf(StringConstants.HYPHEN)));
      }
    } catch (Exception e) {
      ESMetrics.getInstance().meter(MetricConstants.METRIC_IMK_DUMP_ERROR_PARSE_CLIENTID, 1);
      LOGGER.warn("cannot parse client id", e);
    }

    return result;
  }

  @SuppressWarnings("unchecked")
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
      ESMetrics.getInstance().meter(METRIC_IMK_DUMP_MALFORMED, 1);
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

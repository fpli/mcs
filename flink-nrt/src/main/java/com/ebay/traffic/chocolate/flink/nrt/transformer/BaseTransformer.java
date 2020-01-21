package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.FlatMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.google.common.base.CaseFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

public class BaseTransformer {
  public static final String GET_METHOD_PREFIX = "get";
  public static final String SET_METHOD_PREFIX = "set";

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTransformer.class);
  private static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());
  private static final DateTimeFormatter EVENT_TS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  private static final Map<String, Integer> MFE_NAME_ID_MAP = new HashMap<String, Integer>() {
    {
      PropertyMgr.getInstance().loadAllLines("mfe_name_id_map.txt").stream()
              .map(line -> line.split("\\|")).forEach(split -> put(split[0], Integer.valueOf(split[1])));
    }
  };

  private static final List<String> USER_QUERY_PARAMS_OF_REFERRER = Collections.singletonList("q");

  private static final List<String> USER_QUERY_PARAMS_OF_LANDING_URL = Arrays.asList("uq", "satitle", "keyword", "item", "store");

  /**
   * Used to cache temp fields
   */
  private final Map<String, Object> fieldCache = new HashMap<>(16);

  /**
   * Used to cache method object to improve reflect performance
   */
  private static final Map<String, Method> FIELD_GET_METHOD_CACHE = new HashMap<>(16);

  private static final Map<String, Method> FIELD_SET_METHOD_CACHE = new HashMap<>(16);

  private static final Function<String, String> FIELD_GET_METHOD_MAP_FUNCTION = fieldName -> {
    String upperCamelCase = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName);
    return String.format("%s%s", GET_METHOD_PREFIX, upperCamelCase);
  };

  private static final Function<String, String> FIELD_SET_METHOD_MAP_FUNCTION = fieldName -> {
    String upperCamelCase = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName);
    return String.format("%s%s", SET_METHOD_PREFIX, upperCamelCase);
  };

  /**
   * Original record
   */
  protected GenericRecord sourceRecord;
  protected Schema schema;


  public BaseTransformer(FilterMessage sourceRecord) {
    this.sourceRecord = sourceRecord;
  }

  public void transform(FlatMessage flatMessage) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
    for (Schema.Field field : flatMessage.getSchema().getFields()) {
      String fieldName = field.name();
      Object value = getField(fieldName);
      setField(flatMessage, fieldName, value);
    }
  }

  protected Object getField(String fieldName) {
    if (fieldCache.containsKey(fieldName)) {
      return fieldCache.get(fieldName);
    }
    Method method = findMethod(BaseTransformer.class, fieldName, FIELD_GET_METHOD_MAP_FUNCTION, FIELD_GET_METHOD_CACHE);
    Object value = null;
    try {
      value = method.invoke(this);
    } catch (IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }
    Validate.notNull(value, String.format("%s is null", fieldName));
    fieldCache.put(fieldName, value);
    return fieldCache.get(fieldName);
  }

  protected void setField(FlatMessage flatMessage, String fieldName, Object value) throws InvocationTargetException, IllegalAccessException {
    Method setMethod = findMethod(flatMessage.getClass(), fieldName, FIELD_SET_METHOD_MAP_FUNCTION, FIELD_SET_METHOD_CACHE);
    setMethod.invoke(flatMessage, value);
  }

  private Method findMethod(Class<?> clazz, String fieldName, Function<String, String> fieldMethodMapFunction, Map<String, Method> methodCache) {
    if (methodCache.containsKey(fieldName)) {
      return methodCache.get(fieldName);
    }
    String methodName = fieldMethodMapFunction.apply(fieldName);
    Method method = findMethodByName(clazz, methodName);
    Validate.notNull(method, String.format("cannot find method %s", methodName));
    methodCache.put(fieldName, method);
    return methodCache.get(fieldName);
  }

  private Method findMethodByName(Class<?> clazz, String getMethodName) {
    Method getMethod = null;
    for (Method declaredMethod : clazz.getDeclaredMethods()) {
      if (declaredMethod.getName().equals(getMethodName)) {
        getMethod = declaredMethod;
        break;
      }
    }
    return getMethod;
  }

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
      }
    }
    return query;
  }

  protected String getBatchId() {
    LocalDateTime date = LocalDateTime.now();
    DecimalFormat formatter = new DecimalFormat("00");
    return formatter.format(date.getHour()) + formatter.format(date.getMinute()) + formatter.format(date.getSecond());
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
        return "4";
      case ROI:
        return "2";
      default:
        return "1";
    }
  }

  protected String getRvrChnlTypeCd() {
    ChannelType channelType = (ChannelType) sourceRecord.get(TransformerConstants.CHANNEL_TYPE);
    switch (channelType) {
      case EPN:
        return "1";
      case DISPLAY:
        return "4";
      case PAID_SEARCH:
        return "2";
      case SOCIAL_MEDIA:
        return "16";
      case PAID_SOCIAL:
        return "20";
      case NATURAL_SEARCH:
        return "3";
      default:
        return "0";
    }
  }

  protected String getCntryCd() {
    return StringConstants.EMPTY;
  }

  protected String getLangCd() {
    return (String) sourceRecord.get(TransformerConstants.LANG_CD);
  }

  protected Integer getTrckngPrtnrId() {
    return 0;
  }

  // TODO
  public String getCguid() {
    return "";
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
    if (StringUtils.isNotEmpty(userAgent)) {
      String agentStr = userAgent.toLowerCase();
      for (UserAgentEnum userAgentEnum : UserAgentEnum.values()) {
        if (agentStr.contains(userAgentEnum.getName())) {
          return userAgentEnum.getId();
        }
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

  private String getDomain(String link) {
    String result = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(link)) {
      try {
        result = new URL(link).getHost();
      } catch (Exception e) {
      }
    }
    return result;
  }

  protected String getLndngPageUrl() {
    String uri = (String) sourceRecord.get(TransformerConstants.URI);
    String newUri = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(uri)) {
      try {
        newUri = uri.replace(TransformerConstants.MKGROUPID, TransformerConstants.ADGROUPID).replace(TransformerConstants.MKTYPE, TransformerConstants.ADTYPE);
      } catch (Exception e) {
      }
    }
    return newUri;
  }

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
    return StringConstants.EMPTY;
  }

  public String getFlexField2() {
    return StringConstants.EMPTY;
  }

  public String getFlexField3() {
    return StringConstants.EMPTY;
  }

  public String getFlexField4() {
    return StringConstants.EMPTY;
  }

  public String getFlexField5() {
    return StringConstants.EMPTY;
  }

  public String getFlexField6() {
    return StringConstants.EMPTY;
  }

  public String getFlexField7() {
    return StringConstants.EMPTY;
  }

  public String getFlexField8() {
    return StringConstants.EMPTY;
  }

  public String getFlexField9() {
    return StringConstants.EMPTY;
  }

  public String getFlexField10() {
    return StringConstants.EMPTY;
  }

  public String getFlexField11() {
    return StringConstants.EMPTY;
  }

  public String getFlexField12() {
    return StringConstants.EMPTY;
  }

  public String getFlexField13() {
    return StringConstants.EMPTY;
  }

  public String getFlexField14() {
    return StringConstants.EMPTY;
  }

  public String getFlexField15() {
    return StringConstants.EMPTY;
  }

  public String getFlexField16() {
    return StringConstants.EMPTY;
  }

  public String getFlexField17() {
    return StringConstants.EMPTY;
  }

  public String getFlexField18() {
    return StringConstants.EMPTY;
  }

  public String getFlexField19() {
    return StringConstants.EMPTY;
  }

  public String getFlexField20() {
    return StringConstants.EMPTY;
  }

  private String getQueryString(String uri) {
    String query = StringConstants.EMPTY;
    if (StringUtils.isNotEmpty(uri)) {
      try {
        query = new URL(uri).getQuery();
        if (StringUtils.isEmpty(query)) {
          query = StringConstants.EMPTY;
        }
      } catch (Exception e) {
      }
    }
    return query;
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

    }
    return "";
  }

  public String getEventTs() {
    return EVENT_TS_FORMATTER.format(Instant.ofEpochMilli((Long) sourceRecord.get(TransformerConstants.TIMESTAMP)));
  }

  protected Integer getDfltBhrvId() {
    return 0;
  }

  private String getPerfTrackNameValue() {
    String query = getTempUriQuery();
    StringBuilder buf = new StringBuilder();
    try {
      if (StringUtils.isNotEmpty(query)) {
        for (String paramMapString : query.split(StringConstants.AND)) {
          String[] paramStringArray = paramMapString.split(StringConstants.EQUAL);
          if (paramStringArray.length == 2) {
            buf.append(String.format("^%s", paramMapString));
          }
        }
      }
    } catch (Exception e) {
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

  private String getDefaultNullNumParamValueFromQuery(String query, String key) {
    String result = "";
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
    }
    return result;
  }

  public String getCrlp() {
    String query = getTempUriQuery();
    return getParamValueFromQuery(query, TransformerConstants.CRLP);
  }

  protected String getParamValueFromQuery(String query, String key) {
    String result = "";
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
    if (StringUtils.isEmpty(userId) || userId.equals("0")) {
      return "0";
    } else {
      return "1";
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
    return getItemIdFromUri(uri);
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

  // TODO
  protected Integer getMgvalueRsnCd() {
    return 0;
  }

  public Integer getClientIdFromRotationId(String rotationId) {
    Integer result = 0;
    try {
      if (StringUtils.isNotEmpty(rotationId)
              && rotationId.length() <= 25
              && StringUtils.isNumeric(rotationId.replace(StringConstants.HYPHEN, StringConstants.EMPTY))
              && rotationId.contains(StringConstants.HYPHEN)) {
        result = Integer.valueOf(rotationId.substring(0, rotationId.indexOf(StringConstants.HYPHEN)));
      } else {
        result = 0;
      }
    } catch (Exception e) {

    }
    return result;
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

  /**
   * This class
   *
   * @author Zhiyuan Wang
   * @since 2019/12/8
   */
  public enum UserAgentEnum {
    MSIE("msie", 2),
    FIREFOX("firefox", 5),
    CHROME("chrome", 11),
    SAFARI("safari", 4),
    OPERA("opera", 7),
    NETSCAPE("netscape", 1),
    NAVIGATOR("navigator", 1),
    AOL("aol", 3),
    MAC("mac", 8),
    MSNTV("msntv", 9),
    WEBTV("webtv", 6),
    TRIDENT("trident", 2),
    BINGBOT("bingbot", 12),
    ADSBOT_GOOGLE("adsbot-google", 19),
    UCWEB("ucweb", 25),
    FACEBOOKEXTERNALHIT("facebookexternalhit", 20),
    DVLVIK("dvlvik", 26),
    AHC("ahc", 13),
    TUBIDY("tubidy", 14),
    ROKU("roku", 15),
    YMOBILE("ymobile", 16),
    PYCURL("pycurl", 17),
    DAILYME("dailyme", 18),
    EBAYANDROID("ebayandroid", 21),
    EBAYIPHONE("ebayiphone", 22),
    EBAYIPAD("ebayipad", 23),
    EBAYWINPHOCORE("ebaywinphocore", 24),
    NULL_USERAGENT("NULL_USERAGENT", 10),
    UNKNOWN_USERAGENT("UNKNOWN_USERAGENT", -99);

    private final String name;
    private final Integer id;

    UserAgentEnum(final String name, final Integer id) {
      this.name = name;
      this.id = id;
    }

    public String getName() {
      return this.name;
    }

    public Integer getId() {
      return this.id;
    }
  }
}

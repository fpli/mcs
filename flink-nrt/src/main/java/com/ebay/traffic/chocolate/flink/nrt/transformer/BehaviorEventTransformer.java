package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.BehaviorEvent;
import com.ebay.traffic.chocolate.flink.nrt.constant.*;
import com.google.common.base.CaseFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;

public class BehaviorEventTransformer {
  public static final String GET_METHOD_PREFIX = "get";

  private static final Logger LOGGER = LoggerFactory.getLogger(BehaviorEventTransformer.class);

  /**
   * Used to cache method object to improve reflect performance
   */
  private static final Map<String, Method>  FIELD_GET_METHOD_CACHE = new HashMap<>(16);

  /**
   * Map field name to get method name, eg. batch_id -> getBatchId
   */
  private static final Function<String, String> FIELD_GET_METHOD_MAP_FUNCTION = fieldName -> {
    String upperCamelCase = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName);
    return String.format("%s%s", GET_METHOD_PREFIX, upperCamelCase);
  };

  private static final String REFERER = "Referer";
  private static final String USER_ID = "userId";
  private static final String CHANNEL_TYPE = "channelType";
  private static final String CHANNEL_ACTION = "channelAction";
  private static final String ADGUID = "adguid";
  private static final String SNAPSHOT_ID = "snapshotId";
  private static final String SEQ_NUM = "seqNum";
  private static final String PAGE_ID = "pageId";
  private static final String PAGE_NAME = "pageName";
  private static final String REFERER_HASH = "refererHash";
  private static final String EVENT_TIMESTAMP = "eventTimestamp";
  private static final String URL_QUERY_STRING = "urlQueryString";
  private static final String CLIENT_DATA = "clientData";
  private static final String APPLICATION_PAYLOAD = "applicationPayload";
  private static final String WEB_SERVER = "webServer";
  private static final String RDT = "rdt";
  private static final String DISPATCH_ID = "dispatchId";
  private static final String DATA = "data";

  /**
   * Original record
   */
  protected GenericRecord sourceRecord;

  public BehaviorEventTransformer(GenericRecord sourceRecord) {
    this.sourceRecord = sourceRecord;
  }

  public void transform(BehaviorEvent behaviorEvent) {
    for (Schema.Field field : behaviorEvent.getSchema().getFields()) {
      final String fieldName = field.name();
      Object value = getField(fieldName);
      if (value == null) {
        continue;
      }
      behaviorEvent.put(fieldName, value);
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
    } catch (NullPointerException | IllegalAccessException | InvocationTargetException e) {
      if (e instanceof NullPointerException) {
        String message = String.format("%s no mapping method, source record %s", fieldName, this.sourceRecord);
        throw new NullPointerException(message);
      } else {
        String message = String.format("invoke method %s failed, source record %s", method, this.sourceRecord);
        throw new RuntimeException(message, e);
      }
    }
    return value;
  }

  private Method findMethod(String fieldName) {
    if (BehaviorEventTransformer.FIELD_GET_METHOD_CACHE.containsKey(fieldName)) {
      return BehaviorEventTransformer.FIELD_GET_METHOD_CACHE.get(fieldName);
    }
    String methodName = BehaviorEventTransformer.FIELD_GET_METHOD_MAP_FUNCTION.apply(fieldName);
    Method method = findMethodByName(methodName);
    Validate.notNull(method, String.format("cannot find method %s, source record %s", methodName, this.sourceRecord));
    BehaviorEventTransformer.FIELD_GET_METHOD_CACHE.put(fieldName, method);
    return BehaviorEventTransformer.FIELD_GET_METHOD_CACHE.get(fieldName);
  }

  private Method findMethodByName(String getMethodName) {
    Method getMethod = null;
    for (Method declaredMethod : BehaviorEventTransformer.class.getDeclaredMethods()) {
      if (declaredMethod.getName().equals(getMethodName)) {
        getMethod = declaredMethod;
        break;
      }
    }
    return getMethod;
  }

  protected String getGuid() {
    Utf8 guid = (Utf8) sourceRecord.get(TransformerConstants.GUID);
    return guid == null ? null : String.valueOf(guid);
  }

  protected String getAdguid() {
    Utf8 adguid = (Utf8) sourceRecord.get(ADGUID);
    return adguid == null ? null : String.valueOf(adguid);
  }

  protected Long getSessionskey() {
    return getSnapshotid();
  }

  protected Long getSnapshotid() {
    Utf8 snapshotId = (Utf8) sourceRecord.get(SNAPSHOT_ID);
    if (snapshotId == null) {
      return null;
    }
    try {
      return Long.valueOf(String.valueOf(snapshotId));
    } catch (NumberFormatException e) {
      LOGGER.info("cannot parse snapshotId {}, source record {}", snapshotId, this.sourceRecord);
      return null;
    }
  }

  protected Integer getSeqnum() {
    Utf8 seqNum = (Utf8) sourceRecord.get(SEQ_NUM);
    if (seqNum == null) {
      return null;
    }
    try {
      return Integer.valueOf(String.valueOf(seqNum));
    } catch (NumberFormatException e) {
      LOGGER.info("cannot parse seqNum {}, source record {}", seqNum, this.sourceRecord);
      return null;
    }
  }

  protected Integer getSiteid() {
    Utf8 siteId = (Utf8) sourceRecord.get(TransformerConstants.SITE_ID);
    if (siteId == null) {
      return null;
    }
    try {
      return Integer.valueOf(String.valueOf(siteId));
    } catch (NumberFormatException e) {
      LOGGER.info("cannot parse siteId {}, source record {}", siteId, this.sourceRecord);
      return null;
    }
  }

  protected Integer getPageid() {
    return (Integer) sourceRecord.get(PAGE_ID);
  }

  protected String getPagename() {
    Utf8 pageName = (Utf8) sourceRecord.get(PAGE_NAME);
    return pageName == null ? null : String.valueOf(pageName);
  }

  protected Long getRefererhash() {
    Utf8 refererHash = (Utf8) sourceRecord.get(REFERER_HASH);
    if (refererHash == null) {
      return null;
    }
    try {
      return Long.valueOf(String.valueOf(refererHash));
    } catch (NumberFormatException e) {
      LOGGER.info("cannot parse refererHash {}, source record {}", refererHash, this.sourceRecord);
      return null;
    }
  }

  protected Long getEventtimestamp() {
    return (Long) sourceRecord.get(EVENT_TIMESTAMP);
  }

  protected String getUrlquerystring() {
    Utf8 urlQueryString = (Utf8) sourceRecord.get(URL_QUERY_STRING);
    return urlQueryString == null ? null : String.valueOf(urlQueryString);
  }

  @SuppressWarnings("unchecked")
  protected String getClientdata() {
    HashMap<Utf8, Utf8> clientData = (HashMap<Utf8, Utf8>) sourceRecord.get(CLIENT_DATA);
    return convertMap(clientData);
  }

  @SuppressWarnings("unchecked")
  protected String getApplicationpayload() {
    HashMap<Utf8, Utf8> applicationPayload = (HashMap<Utf8, Utf8>) sourceRecord.get(APPLICATION_PAYLOAD);
    return convertMap(applicationPayload);
  }

  private String convertMap(HashMap<Utf8, Utf8> value) {
    StringJoiner joiner = new StringJoiner(StringConstants.AND);
    value.forEach((k, v) -> joiner.add(k + StringConstants.EQUAL + v));
    return joiner.toString();
  }

  protected String getWebserver() {
    Utf8 webServer = (Utf8) sourceRecord.get(WEB_SERVER);
    return webServer == null ? null : String.valueOf(webServer);
  }

  @SuppressWarnings("unchecked")
  protected String getReferrer() {
    HashMap<Utf8, Utf8> clientData = (HashMap<Utf8, Utf8>) sourceRecord.get(CLIENT_DATA);
    Utf8 referer = clientData.get(new Utf8(REFERER));
    return referer == null ? null : String.valueOf(referer);
  }

  protected String getUserid() {
    Utf8 userId = (Utf8) sourceRecord.get(USER_ID);
    return userId == null ? null : String.valueOf(userId);
  }

  protected Integer getRdt() {
    return (Integer) sourceRecord.get(RDT);
  }

  protected String getChanneltype() {
    Utf8 channelType = (Utf8) sourceRecord.get(CHANNEL_TYPE);
    return channelType == null ? null : String.valueOf(channelType);
  }

  protected String getChannelaction() {
    Utf8 channelAction = (Utf8) sourceRecord.get(CHANNEL_ACTION);
    return channelAction == null ? null : String.valueOf(channelAction);
  }

  protected String getDispatchid() {
    Utf8 dispatchId = (Utf8) sourceRecord.get(DISPATCH_ID);
    return dispatchId == null ? null : String.valueOf(dispatchId);
  }

  @SuppressWarnings("unchecked")
  protected List<Map<String, String>> getData() {
    List<Map<Utf8, Utf8>> data = (List<Map<Utf8, Utf8>>) sourceRecord.get(DATA);
    List<Map<String, String>> list = new ArrayList<>();
    data.forEach(map -> {
      Map<String, String> newMap = new HashMap<>();
      map.forEach((k, v) -> newMap.put(String.valueOf(k), String.valueOf(v)));
      list.add(newMap);
    });
    return list;
  }
}

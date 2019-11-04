package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.BullseyeConstants;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.LBSConstants;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSQueryResult;
import com.ebay.app.raptor.chocolate.common.DAPRvrId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.constants.KernelConstants;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.kernel.util.FastURLEncoder;
import com.ebay.kernel.util.RequestUtil;
import com.ebay.kernel.util.guid.Guid;
import com.ebay.raptor.geo.context.GeoCtx;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Component;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Zhiyuan Wang
 * @since 2019/9/24
 */
@Component
public class DAPResponseHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DAPResponseHandler.class);

  private static List<TwoParamsListEntry> mobileUserAgentList = new ArrayList<>();

  private static final String MOBILE_USER_AGENT_CONFIG_FILE = "/config/mobile_user_agent.txt";

  static {
    List<String> mobileUserAgent = new ArrayList<>();
    try {
      mobileUserAgent = Files.readAllLines(Paths.get(RuntimeContext.getConfigRoot().getFile() + MOBILE_USER_AGENT_CONFIG_FILE));
    } catch (IOException e) {
      LOGGER.error("read mobile user agent config file failed {}", e.getMessage());
    }
    for (String userAgent : mobileUserAgent) {
      String t = userAgent.trim();
      if (t.isEmpty() || t.startsWith("#")) {
        continue;
      }
      mobileUserAgentList.add(new TwoParamsListEntry(t));
    }
  }

  private static final List<String> BULLSEYE_MODEL_911_ATTRIBUTES = Arrays.asList(
          BullseyeConstants.LAST_PRODUCTS_PURCHASED,
          BullseyeConstants.LAST_PRODUCTS_WATCHED,
          BullseyeConstants.ZIP_CODE,
          BullseyeConstants.FEEDBACK_SCORE
  );

  public void sendDAPResponse(HttpServletRequest request, HttpServletResponse response, CookieReader cookieReader, ContainerRequestContext requestContext)
          throws URISyntaxException {
    long dapRvrId = getDAPRvrId();
    Map<String, String[]> params = request.getParameterMap();
    String cguid = getCguid(cookieReader, requestContext);
    String guid = getGuid(cookieReader, requestContext);
    String accountId = getAccountId(cookieReader, requestContext);
    String udid = getUdid(params);
    Map<String, String> userAttributes = getUserAttributes(cguid);
    String referrer = request.getHeader(Constants.REFERER);
    String remoteIp = getRemoteIp(request);
    Map<String, String> lbsParameters = getLBSParameters(request, remoteIp);
    String hLastLoggedInUserId = getHLastLoggedInUserId(accountId);
    String userAgent = request.getHeader(Constants.USER_AGENT);
    String uaPrime = getUaPrime(params);
    boolean isMobile = isMobileUserAgent(userAgent) || isMobileSDK(uaPrime, udid);
    int siteId = getSiteId(requestContext);

    LOGGER.info("dapRvrId: {} cguid: {} guid: {} accountId: {} udid: {} userAttributes: {} referrer: {} remoteIp: {} " +
                    "lbsParameters: {} hLastLoggedInUserId: {} userAgent: {} uaPrime: {} isMobile: {} siteId: {}",
            dapRvrId, cguid, guid, accountId, udid, userAttributes, referrer, remoteIp, lbsParameters,
            hLastLoggedInUserId, userAgent, uaPrime, isMobile, siteId);

    URIBuilder dapUriBuilder = new URIBuilder();

    setSiteId(dapUriBuilder, siteId);
    setRequestParameters(dapUriBuilder, params);
    setRvrId(dapUriBuilder, dapRvrId);
    setReferrer(dapUriBuilder, referrer);
    setGeoInfo(dapUriBuilder, lbsParameters);
    setUserAttributes(dapUriBuilder, userAttributes);
    setIsMobile(dapUriBuilder, isMobile);
    setCguid(dapUriBuilder, cguid);
    setGuid(dapUriBuilder, guid);
    setRoverUserid(dapUriBuilder, accountId);
    setHLastLoggedInUserId(dapUriBuilder, hLastLoggedInUserId);
    setUdid(dapUriBuilder, udid);

    callDAPResponse(dapUriBuilder.build().toString(), response);

    sendToMCS(request, dapRvrId, cguid, guid);
  }

  private int getSiteId(ContainerRequestContext requestContext) {
    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
    if (userPrefsCtx == null) {
      return 0;
    }
    GeoCtx geoContext = userPrefsCtx.getGeoContext();
    if (geoContext == null) {
      return 0;
    }
    return geoContext.getSiteId();
  }

  private void setSiteId(URIBuilder dapUriBuilder, int siteId) {
    addParameter(dapUriBuilder, Constants.SITE_ID, String.valueOf(siteId));
  }

  private String getUaPrime(Map<String, String[]> params) {
    if (!params.containsKey(Constants.UA_PARAM)) {
      return null;
    }
    String[] strings = params.get(Constants.UA_PARAM);
    if (ArrayUtils.isEmpty(strings)) {
      return null;
    }
    return strings[0];
  }

  private String getRemoteIp(HttpServletRequest request) {
    String remoteIp = null;
    String xForwardFor = request.getHeader("X-Forwarded-For");
    if (xForwardFor != null && !xForwardFor.isEmpty()) {
      remoteIp = xForwardFor.split(",")[0];
    }

    if (remoteIp == null || remoteIp.isEmpty()) {
      remoteIp = RequestUtil.getRemoteAddr(request);
    }

    return remoteIp == null ? "" : remoteIp;
  }

  @SuppressWarnings("unchecked")
  private String getUdid(Map<String, String[]> params) {
    if (!params.containsKey(Constants.UNIQUE_DEVICE_ID)) {
      ESMetrics.getInstance().meter("NoUdid", 1, Field.of(Constants.CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()));
      return null;
    }
    String[] strings = params.get(Constants.UNIQUE_DEVICE_ID);
    if (ArrayUtils.isEmpty(strings)) {
      ESMetrics.getInstance().meter("NoUdid", 1, Field.of(Constants.CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()));
      return null;
    }
    return strings[0];
  }

  private void setHLastLoggedInUserId(URIBuilder dapUriBuilder, String hLastLoggedInUserId) {
    addParameter(dapUriBuilder, Constants.H_LAST_LOGGED_IN_USER_ID, hLastLoggedInUserId);
  }

  /**
   * Dap uses hashed user id to finding bucket id, dap has hash function which calculates 0-99 value from hashedloggedinUserid, e.g 0-49 50-99
   * these buckets are used for A/B testing
   * @param userId userId
   * @return hash userId
   */
  private String getHLastLoggedInUserId(String userId) {
    if (StringUtils.isEmpty(userId)) {
      return null;
    }
    if (!StringUtils.isNumeric(userId)) {
      return null;
    }

    Long userIdLong = null;
    try {
      userIdLong = Long.valueOf(userId);
    } catch (Exception e){
      LOGGER.error(e.getMessage());
    }

    if (userIdLong == null) {
      return null;
    }
    if (userIdLong <= 0) {
      return null;
    }
    return IdMapUrlBuilder.hashData(userId, IdMapUrlBuilder.HASH_ALGO_SHA_256);
  }

  private void setIsMobile(URIBuilder dapUriBuilder, boolean isMobile) {
    if (isMobile) {
      addParameter(dapUriBuilder, Constants.IS_MOB, Constants.IS_MOB_TRUE);
    }
  }

  private String getDecodedUA(String ua, String udid) {
    if (StringUtils.isEmpty(ua)) {
      return null;
    }
    if (StringUtils.isEmpty(udid)) {
      return null;
    }
    if (udid.length() < Constants.UDID_MIN_LENGTH) {
      return null;
    }

    String decodedUA = null;
    String key = "" + udid.charAt(1) + udid.charAt(4) + udid.charAt(5) + udid.charAt(8);
    try {
      decodedUA = TrackingUtil.decrypt(key, ua);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return decodedUA;
  }

  private boolean isMobileUserAgent(String userAgent) {
    if (StringUtils.isEmpty(userAgent)) {
      return false;
    }

    for (TwoParamsListEntry entry : mobileUserAgentList) {
      if (entry.match(userAgent)) {
        return true;
      }
    }

    return false;
  }

  private boolean isMobileSDK(String uaPrime, String udid) {
    String decodedUaPrime = getDecodedUA(uaPrime, udid);
    if (StringUtils.isEmpty(decodedUaPrime)) {
      return false;
    }
    return decodedUaPrime.contains("sdkN=mSDK");
  }

  /**
   * Currently, we can only get cguid from cookie.
   */
  @SuppressWarnings("unchecked")
  private String getCguid(CookieReader cookieReader, ContainerRequestContext requestContext) {
    String readerCguid = cookieReader.getCguid(requestContext);
    if (StringUtils.isEmpty(readerCguid)) {
      ESMetrics.getInstance().meter("NoCguid", 1, Field.of(Constants.CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()));
      return null;
    }
    if (readerCguid.length() < Constants.CGUID_LENGTH) {
      ESMetrics.getInstance().meter("InvalidCguidLength", 1, Field.of(Constants.CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()));
      return null;
    }
    return readerCguid.substring(0, Constants.CGUID_LENGTH);
  }

  /**
   * Currently, we can only get guid from cookie.
   */
  @SuppressWarnings("unchecked")
  private String getGuid(CookieReader cookieReader, ContainerRequestContext requestContext) {
    String readerGuid = cookieReader.getGuid(requestContext);
    if (StringUtils.isEmpty(readerGuid)) {
      ESMetrics.getInstance().meter("NoGuid", 1, Field.of(Constants.CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()));
      return null;
    }
    if (readerGuid.length() < Constants.GUID_LENGTH) {
      ESMetrics.getInstance().meter("InvalidGuidLength", 1, Field.of(Constants.CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()));
      return null;
    }
    return readerGuid.substring(0, Constants.GUID_LENGTH);
  }

  private void setCguid(URIBuilder dapUriBuilder, String cguid) {
    addParameter(dapUriBuilder, Constants.CGUID, cguid);
  }

  private void setGuid(URIBuilder dapUriBuilder, String guid) {
    addParameter(dapUriBuilder, Constants.GUID, guid);
  }

  /**
   * Currently, we can only get account id from cookie.
   */
  @SuppressWarnings("unchecked")
  private String getAccountId(CookieReader cookieReader, ContainerRequestContext requestContext) {
    String accountId = cookieReader.getAccountId(requestContext);
    if (StringUtils.isEmpty(accountId)) {
      ESMetrics.getInstance().meter("NoAccountId", 1, Field.of(Constants.CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()));
      return null;
    }
    return accountId;
  }

  private void setUserAttributes(URIBuilder dapUriBuilder, Map<String, String> userAttributes) {
    userAttributes.forEach((key, value) -> addParameter(dapUriBuilder, key, value));
  }

  /**
   * Get user attributes from Bullseye Model 910
   * @param cguid cuid
   */
  private Map<String, String> getUserAttributes(String cguid) {
    if (StringUtils.isEmpty(cguid)) {
      return new HashMap<>();
    }

    String msg = getBullseyeUserAttributesResponse(cguid);
    if (StringUtils.isEmpty(msg)) {
      return new HashMap<>();
    }

    Map<String, String> map = new HashMap<>();
    try {
      map = parseUserAttributes(msg);
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private String getBullseyeUserAttributesResponse(String cguid) {
    String msg = null;
    Configuration config = ConfigurationBuilder.newConfig("beclntsrv.adservice");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    try (Response response = mktClient.target(endpoint).path("/timeline")
            .queryParam("modelid","911")
            .queryParam(Constants.CGUID, cguid)
            .queryParam("attrs", StringUtils.join(BULLSEYE_MODEL_911_ATTRIBUTES, StringConstants.COMMA))
            .request()
            .get()) {
      int status = response.getStatus();
      if (status == Response.Status.OK.getStatusCode()) {
        msg = response.readEntity(String.class);
      } else {
        LOGGER.error("Failed to call Bullseye {}", status);
      }
      ESMetrics.getInstance().meter("BullseyeStatus", 1, Field.of("status", status));
    } catch(Exception e) {
      LOGGER.error("Failed to call Bullseye {}", e.getMessage());
      ESMetrics.getInstance().meter("BullseyeException");
    }
    return msg;
  }

  private Map<String, String> parseUserAttributes(String msg) {
    JSONArray jsonArray = new JSONArray(msg);
    if (jsonArray.length() == 0) {
      return new HashMap<>();
    }
    JSONObject first = jsonArray.getJSONObject(0);
    if (first.isNull("results")) {
      return new HashMap<>();
    }
    JSONObject results = first.getJSONObject("results");
    if (results.isNull("response")) {
      return new HashMap<>();
    }
    JSONObject bullseyeResponse = results.getJSONObject("response");
    Map<String, String> map = new HashMap<>();
    for (String key : bullseyeResponse.keySet()) {
      extract(map, key, bullseyeResponse.get(key));
    }
    return map;
  }

  private void setLastProducts(Map<String, String> map, String key, Object value) {
    JSONArray jsonArray = (JSONArray) value;
    List<String> itemList = new ArrayList<>();
    for (int i = 0; i < jsonArray.length(); i++) {
      if (i == 5) {
        break;
      }
      List<Object> list = new ArrayList<>();
      JSONObject object = (JSONObject) jsonArray.get(i);
      list.add(object.get("timestamp"));
      list.add(object.get("productid"));
      list.add(object.get("itemtitle"));
      itemList.add(StringUtils.join(list, ":"));
    }
    map.put(key, StringUtils.join(itemList, StringConstants.COMMA));
  }

  @SuppressWarnings("unchecked")
  private void extract(Map<String, String> dapUriBuilder, String key, Object value) {
    if (StringConstants.EMPTY.equals(value)) {
      return;
    }
    switch (key) {
      case BullseyeConstants.ZIP_CODE:
        dapUriBuilder.put(BullseyeConstants.ZIP_CODE, String.valueOf(value));
        break;
      case BullseyeConstants.FEEDBACK_SCORE:
        dapUriBuilder.put(BullseyeConstants.FEEDBACK_SCORE, String.valueOf(value));
        break;
      case BullseyeConstants.LAST_PRODUCTS_PURCHASED:
        setLastProducts(dapUriBuilder, BullseyeConstants.LAST_PRODUCTS_PURCHASED, value);
        break;
      case BullseyeConstants.LAST_PRODUCTS_WATCHED:
        setLastProducts(dapUriBuilder, BullseyeConstants.LAST_PRODUCTS_WATCHED, value);
        break;
      case BullseyeConstants.USER_ID:
        break;
      case BullseyeConstants.MODEL_ID:
        break;
      default:
        LOGGER.error("Unknown Attribute {}", key);
        ESMetrics.getInstance().meter("UnknownAttribute", 1, Field.of("attribute", key));
    }
  }

  private void setUdid(URIBuilder dapUriBuilder, String deviceId) {
    addParameter(dapUriBuilder, Constants.UNIQUE_DEVICE_ID, deviceId);
  }

  private void setRvrId(URIBuilder dapUriBuilder, long dapRvrId) {
    addParameter(dapUriBuilder, Constants.RVR_ID, String.valueOf(dapRvrId));
  }

  private void addParameter(URIBuilder dapuUriBuilder, String key, String value) {
    if (StringUtils.isEmpty(key)) {
      return;
    }
    if (StringUtils.isEmpty(value)) {
      return;
    }
    dapuUriBuilder.addParameter(key, FastURLEncoder.encode(value.trim(), KernelConstants.UTF8_ENCODING));
  }

  /**
   * Set rover_useid, DAP uses this id to get user attributes from Bullseye model 630
   * @param dapUriBuilder dapUrlBuilder
   * @param roverUserid rover_userid
   */
  private void setRoverUserid(URIBuilder dapUriBuilder, String roverUserid) {
    addParameter(dapUriBuilder, Constants.ROVER_USERID, roverUserid);
  }

  /**
   * Generate rvr_id, DAP uses this id to join imk tables.
   * @return rvr_id
   */
  private long getDAPRvrId() {
    long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();
    return new DAPRvrId(snapshotId).getRepresentation();
  }

  /**
   * Append all request parameters to url
   */
  private void setRequestParameters(URIBuilder dapUriBuilder, Map<String, String[]> params) {
    params.forEach((key, values) -> {
      if (StringUtils.isEmpty(key)) {
        return;
      }
      // skip unused parameters
      if (key.equals(Constants.IPN) || key.equals(Constants.MPT)) {
        return;
      }
      if (ArrayUtils.isEmpty(values)) {
        return;
      }
      // strip key startswith ICEP_
      if (key.toUpperCase().startsWith(Constants.ICEP_PREFIX)) {
        key = key.substring(Constants.ICEP_PREFIX.length());
      }
      for (String value : values) {
        if (StringUtils.isEmpty(value)) {
          continue;
        }
        dapUriBuilder.addParameter(key, FastURLEncoder.encode(value.trim(), KernelConstants.UTF8_ENCODING));
      }
    });
  }

  /**
   * Call DAP interface and send response to browser
   */
  @SuppressWarnings("unchecked")
  private void callDAPResponse(String dapUri, HttpServletResponse response) {
    Configuration config = ConfigurationBuilder.newConfig("dapio.adservice");
    Client client = GingerClientBuilder.newClient(config);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String targetUri = endpoint + dapUri;
    LOGGER.info("call DAP {}", targetUri);
    long startTime = System.currentTimeMillis();
    try (Response dapResponse = client.target(targetUri).request().get();
         OutputStream os = response.getOutputStream()) {
      int status = dapResponse.getStatus();
      if (status == Response.Status.OK.getStatusCode()) {
        for (Map.Entry<String, List<Object>> entry : dapResponse.getHeaders().entrySet()) {
          List<Object> list = entry.getValue();
          if (CollectionUtils.isEmpty(list)) {
            continue;
          }
          response.setHeader(entry.getKey(), String.valueOf(list.get(0)));
        }
        byte[] bytes = dapResponse.readEntity(byte[].class);
        os.write(bytes);
      } else {
        LOGGER.error("Failed to call DAP {}", status);
      }
      ESMetrics.getInstance().meter("DAPStatus", 1, Field.of("status", status));
    } catch (Exception e) {
      LOGGER.error("Failed to call DAP {}", e.getMessage());
      ESMetrics.getInstance().meter("DAPException");
    }

    ESMetrics.getInstance().mean("DAPLatency", System.currentTimeMillis() - startTime);
  }

  private String constructTrackingHeader(String rawCguid, String rawGuid) {
    String cookie = "";
    if (!StringUtils.isEmpty(rawGuid))
      cookie += "guid=" + rawGuid;
    else {
      try {
        cookie += "guid=" + new Guid().nextPaddedGUID();
      } catch (UnknownHostException e) {
        LOGGER.warn("Create guid failure: ", e);
        ESMetrics.getInstance().meter("CreateGuidFailed");
      }
      LOGGER.warn("No guid");
    }
    if (!StringUtils.isEmpty(rawCguid))
      cookie += ",cguid=" + rawCguid;
    else {
      LOGGER.warn("No cguid");
    }

    return cookie;
  }

  /**
   * Send to MCS to track this request
   */
  @SuppressWarnings("unchecked")
  private void sendToMCS(HttpServletRequest request, long dapRvrId, String cguid, String guid) throws URISyntaxException {
    Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    // add all headers except Cookie
    Invocation.Builder builder = mktClient.target(endpoint).path("/impression/").request();
    final Enumeration<String> headers = request.getHeaderNames();
    while (headers.hasMoreElements()) {
      String header = headers.nextElement();
      if ("Cookie".equalsIgnoreCase(header)) {
        continue;
      }
      String values = request.getHeader(header);
      builder = builder.header(header, values);
    }

    // construct X-EBAY-C-TRACKING header
    String trackingHeader = constructTrackingHeader(cguid, guid);
    builder = builder.header("X-EBAY-C-TRACKING", trackingHeader);
    LOGGER.info("set MCS X-EBAY-C-TRACKING {}", trackingHeader);

    // add uri and referer to marketing event body
    MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();

    URIBuilder targetUrlBuilder = new URIBuilder(new ServletServerHttpRequest(request).getURI());
    targetUrlBuilder.addParameter(Constants.MKEVT, String.valueOf(MKEVT.AD_REQUEST.getId()));
    targetUrlBuilder.addParameter(Constants.MKRVRID, String.valueOf(dapRvrId));
    String targetUrl = targetUrlBuilder.build().toString();
    mktEvent.setTargetUrl(targetUrl);
    String referer = request.getHeader(Constants.REFERER);
    mktEvent.setReferrer(referer);

    LOGGER.info("call MCS targetUrl {} referer {}", targetUrl, referer);

    // call marketing collection service to send ubi event or send kafka
    long startTime = System.currentTimeMillis();
    try (Response res = builder.post(Entity.json(mktEvent))) {
      int status = res.getStatus();
      if (status == Response.Status.OK.getStatusCode()) {
        LOGGER.info("Send to MCS success.");
      } else {
        LOGGER.error("Send to MCS failed. {}", status);
      }
      ESMetrics.getInstance().meter("MCSStatus", 1, Field.of("status", status));
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      ESMetrics.getInstance().meter("MCSException");
    }

    ESMetrics.getInstance().mean("MCSLatency", System.currentTimeMillis() - startTime);
  }

  private void setReferrer(URIBuilder dapUriBuilder, String referrer) {
    if (referrer == null) {
      return;
    }
    addParameter(dapUriBuilder, Constants.REF_URL, referrer);
    String referrerDomain = getHostFromUrl(referrer);
    if (referrerDomain == null) {
      return;
    }
    addParameter(dapUriBuilder, Constants.REF_DOMAIN, referrerDomain);
  }

  private String getCountryFromBrowserLocale(HttpServletRequest request) {
    String acceptLangs = request.getHeader(Constants.HTTP_ACCEPT_LANGUAGE);
    if (StringUtils.isEmpty(acceptLangs)) {
      return null;
    }

    String[] acceptLangsArray = acceptLangs.split(StringConstants.COMMA);
    if (ArrayUtils.isEmpty(acceptLangsArray)) {
      return null;
    }

    String localeName = acceptLangsArray[0];
    Locale locale = convertLocaleNameToLocale(localeName);
    if (locale == null) {
      return null;
    }

    String countryCode = locale.getCountry();
    if (!isValidCountryCode(countryCode)) {
      return null;
    }

    return countryCode;
  }

  private boolean isValidCountryCode(String countryCode) {
    if (countryCode == null) {
      return false;
    }
    return countryCode.length() == Constants.ISO_COUNTRY_CODE_LENGTH;
  }

  private Locale convertLocaleNameToLocale(String localeName) {
    if (localeName == null) {
      return null;
    }

    if (localeName.trim().length() <= 0) {
      return null;
    }

    String[] localeNamePieces = localeName.split(StringConstants.HYPHEN);
    if (ArrayUtils.isEmpty(localeNamePieces)) {
      return null;
    }

    String langCode = localeNamePieces[0];
    if (localeNamePieces.length == 1) {
      return new Locale(langCode, StringConstants.EMPTY);
    }

    return new Locale(langCode, localeNamePieces[1]);
  }

  private String getHostFromUrl(String url) {
    try {
      URL u = new URL(url);
      return u.getHost().toLowerCase();
    } catch (Exception e) {
      return null;
    }
  }

  private void setGeoInfo(URIBuilder dapuUriBuilder, Map<String, String> lbsParameters) {
    lbsParameters.forEach((key, value) -> addParameter(dapuUriBuilder, key, value));
  }

  /**
   * Get geo info from location base service and pass all the info to DAP
   */
  private Map<String, String> getLBSParameters(HttpServletRequest request, String remoteIp) {
    Map<String, String> map = new HashMap<>();

    LBSQueryResult lbsResponse = LBSClient.getInstance().getLBSInfo(remoteIp);
    if (lbsResponse == null) {
      return map;
    }
    map.put(LBSConstants.GEO_COUNTRY_CODE, lbsResponse.getIsoCountryCode2());
    map.put(LBSConstants.GEO_DMA, lbsResponse.getStateCode());
    map.put(LBSConstants.GEO_CITY, lbsResponse.getCity());
    map.put(LBSConstants.GEO_ZIP_CODE, lbsResponse.getPostalCode());
    map.put(LBSConstants.GEO_LATITUDE, String.valueOf(lbsResponse.getLatitude()));
    map.put(LBSConstants.GEO_LONGITUDE, String.valueOf(lbsResponse.getLongitude()));
    map.put(LBSConstants.GEO_METRO_CODE, lbsResponse.getMetroCode());
    map.put(LBSConstants.GEO_AREA_CODE, lbsResponse.getAreaCodes());

    String countryFromBrowserLocale = getCountryFromBrowserLocale(request);
    if (StringUtils.isEmpty(countryFromBrowserLocale)) {
      return map;
    }

    map.put(LBSConstants.GEO_COUNTRY_CODE, countryFromBrowserLocale);
    return map;
  }
}

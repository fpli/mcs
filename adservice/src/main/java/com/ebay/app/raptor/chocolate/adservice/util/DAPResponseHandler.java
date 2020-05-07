package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.*;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSQueryResult;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.common.DAPRvrId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.constants.KernelConstants;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.kernel.presentation.UrlUtils;
import com.ebay.kernel.util.FastURLEncoder;
import com.ebay.kernel.util.RequestUtil;
import com.ebay.kernel.util.guid.Guid;
import com.ebay.raptor.geo.context.GeoCtx;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Component;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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

  @Autowired
  private AdserviceCookie adserviceCookie;

  @Autowired
  @Qualifier("cb")
  private IdMapable idMapping;

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

  public void sendDAPResponse(HttpServletRequest request, HttpServletResponse response, ContainerRequestContext requestContext)
          throws URISyntaxException {
    long dapRvrId = getDAPRvrId();
    Map<String, String[]> params = request.getParameterMap();
    String guid = adserviceCookie.getGuid(request);
    String accountId = adserviceCookie.getUserId(request);
    // no need anymore
    // Map<String, String> userAttributes = getUserAttributes(cguid);
    String referrer = request.getHeader(Constants.REFERER);
    String remoteIp = getRemoteIp(request);
    Map<String, String> lbsParameters = getLBSParameters(request, remoteIp);
    String hLastLoggedInUserId = getHLastLoggedInUserId(accountId);
    String userAgent = request.getHeader(Constants.USER_AGENT);
    String uaPrime = getUaPrime(params);
    boolean isMobile = isMobileUserAgent(userAgent);
    int siteId = getSiteId(requestContext);

    LOGGER.info("dapRvrId: {} guid: {} accountId: {} referrer: {} remoteIp: {} " +
                    "lbsParameters: {} hLastLoggedInUserId: {} userAgent: {} uaPrime: {} isMobile: {} siteId: {}",
            dapRvrId, guid, accountId, referrer, remoteIp, lbsParameters,
            hLastLoggedInUserId, userAgent, uaPrime, isMobile, siteId);

    URIBuilder dapUriBuilder = new URIBuilder();

    setSiteId(dapUriBuilder, siteId);
    setRequestParameters(dapUriBuilder, params);
    setRvrId(dapUriBuilder, dapRvrId);
    setReferrer(dapUriBuilder, referrer);
    setGeoInfo(dapUriBuilder, lbsParameters);
    setIsMobile(dapUriBuilder, isMobile);
    setGuid(dapUriBuilder, guid);
    setRoverUserid(dapUriBuilder, accountId);
    setHLastLoggedInUserId(dapUriBuilder, hLastLoggedInUserId);

    // call dap to get response
    MultivaluedMap<String, Object> dapResponseHeaders = callDAPResponse(dapUriBuilder.build().toString(), request, response);

    // send to mcs with cguid equal to guid
    if(StringUtils.isEmpty(guid)) {
      guid = Constants.EMPTY_GUID;
    }
    sendToMCS(request, dapRvrId, guid, guid, dapResponseHeaders);
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
   * Get guid from mapping
   */
  private String getGuid(HttpServletRequest request) {
    String adguid = adserviceCookie.readAdguid(request);
    String guid = idMapping.getGuid(adguid);
    if(StringUtils.isEmpty(guid)) {
      guid = "";
    }
    return guid;
  }

  private String getUserId(HttpServletRequest request) {
    String adguid = adserviceCookie.readAdguid(request);
    String encryptedUserid = idMapping.getUid(adguid);
    if(StringUtils.isEmpty(encryptedUserid)) {
      encryptedUserid = "0";
    }
    return String.valueOf(decryptUserId(encryptedUserid));
  }

  /**
   * Decrypt user id from encrypted user id
   * @param encryptedStr encrypted user id
   * @return actual user id
   */
  public long decryptUserId(String encryptedStr) {
    long xorConst = 43188348269L;

    long encrypted = 0;

    try {
      encrypted = Long.parseLong(encryptedStr);
    }
    catch (NumberFormatException e) {
      return -1;
    }

    long decrypted = 0;

    if(encrypted > 0){
      decrypted  = encrypted ^ xorConst;
    }

    return decrypted;
  }

  private void setCguid(URIBuilder dapUriBuilder, String cguid) {
    addParameter(dapUriBuilder, Constants.CGUID, cguid);
  }

  private void setGuid(URIBuilder dapUriBuilder, String guid) {
    addParameter(dapUriBuilder, Constants.GUID, guid);
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

    long startTime = System.currentTimeMillis();
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
    ESMetrics.getInstance().mean("BullseyeLatency", System.currentTimeMillis() - startTime);
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
      if (key.equals(Constants.IPN) || key.equals("cguid") || key.equals("guid") || key.equals("rover_userid")) {
        return;
      }
      // skip marketing tracking parameters
      if (key.equals(Constants.MKRID) || key.equals(Constants.MKCID) || key.equals(Constants.MKEVT)) {
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
  private MultivaluedMap<String, Object> callDAPResponse(String dapUri, HttpServletRequest request, HttpServletResponse response) {
    MultivaluedMap<String, Object> headers = null;
    Configuration config = ConfigurationBuilder.newConfig("dapio.adservice");
    Client client = GingerClientBuilder.newClient(config);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String targetUri = endpoint + dapUri;
    LOGGER.info("call DAP {}", targetUri);
    long startTime = System.currentTimeMillis();
    String body = null;
    int status = -1;
    try (Response dapResponse = client.target(targetUri).request().get()) {
      status = dapResponse.getStatus();
      body = getBody(dapResponse);
      headers = dapResponse.getHeaders();
    } catch (Exception e) {
      LOGGER.error("Failed to call DAP {}", e.getMessage());
      ESMetrics.getInstance().meter("DAPException");
    }

    ESMetrics.getInstance().meter("DAPStatus", 1, Field.of("status", status));

    if (status != Response.Status.OK.getStatusCode()) {
      LOGGER.error("Failed to call DAP {}", status);
      return headers;
    }

    String redirectUrl = headers == null ? null : (String) headers.getFirst(Headers.REDIRECT_URL);
    if (redirectUrl != null && redirectUrl.trim().length() > 0) {
      response.setStatus(HttpServletResponse.SC_MOVED_PERMANENTLY);
      String reqContextType = request.getHeader(Headers.ACCEPT);
      if (reqContextType != null && reqContextType.contains("image/")) {
        response.setContentType("image/gif");
      }
      response.setHeader(Headers.CACHE_CONTROL, "private,no-cache,no-store");
      String safeForRedirect = UrlUtils.makeSafeForRedirect(redirectUrl);
      response.setHeader(Headers.LOCATION, safeForRedirect);
      return headers;
    }

    String contentType = headers == null ? null : (String) headers.getFirst(Headers.CONTENT_TYPE);
    try (OutputStream os = response.getOutputStream()) {
      String encoding = StandardCharsets.UTF_8.name();
      if (contentType != null && (contentType.contains(Headers.CHARSET))) {
        int startIdx = contentType.indexOf(Headers.CHARSET) + Headers.CHARSET.length();
        int searchidx = contentType.indexOf(Headers.SEMICOLON, startIdx);
        // we may have multiple match points in the body for each of the search strings. Locate the closest one
        if (searchidx >= 0) {
          encoding = contentType.substring(startIdx, searchidx);
        } else {
          encoding = contentType.substring(startIdx);
        }
      }

      if (body == null) {
        body = StringConstants.EMPTY;
      }
      byte[] data = body.getBytes(encoding);
      // Set content headers and then write content to response
      response.setHeader(Headers.CACHE_CONTROL, "private, no-cache");
      response.setHeader(Headers.PRAGMA, "no-cache");
      response.setContentType(contentType != null ? contentType : Headers.CONTENT_TYPE_HTML);
      response.setContentLength(data.length);
      response.setStatus(HttpServletResponse.SC_OK);
      os.write(data);
    } catch (Exception e) {
      LOGGER.error("Failed to send response {}", e.getMessage());
    }

    ESMetrics.getInstance().mean("DAPLatency", System.currentTimeMillis() - startTime);
    return headers;
  }

  private String getBody(Response dapResponse) throws IOException {
    String body;
    InputStream is = (InputStream) dapResponse.getEntity();

    StringBuilder sb = new StringBuilder();
    String line;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    }
    body = sb.toString();
    return body;
  }

  private String constructTrackingHeader(String rawCguid, String rawGuid) {
    String cookie = "";
    if (!StringUtils.isEmpty(rawGuid)) {
      cookie += "guid=" + rawGuid;
    } else {
      try {
        cookie += "guid=" + new Guid().nextPaddedGUID();
      } catch (UnknownHostException e) {
        LOGGER.warn("Create guid failure: ", e);
        ESMetrics.getInstance().meter("CreateGuidFailed");
      }
      LOGGER.warn("No guid");
    }
    if (!StringUtils.isEmpty(rawCguid)) {
      cookie += ",cguid=" + rawCguid;
    } else {
      LOGGER.warn("No cguid");
    }

    return cookie;
  }

  /**
   * Send to MCS to track this request
   */
  @SuppressWarnings("unchecked")
  private void sendToMCS(HttpServletRequest request, long dapRvrId, String cguid, String guid, MultivaluedMap<String, Object> dapResponseHeaders) throws URISyntaxException {
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
    // set mkevt as 6, overriding existing value if set
    targetUrlBuilder.setParameter(Constants.MKEVT, MKEVT.AD_REQUEST.getId());
    targetUrlBuilder.addParameter(Constants.MKRVRID, String.valueOf(dapRvrId));
    // add flex fields of dap response headers, these fields start with "ff"
    if (dapResponseHeaders != null) {
      dapResponseHeaders.forEach((key, values) -> {
        if (key.startsWith("ff")) {
          values.forEach(value -> targetUrlBuilder.addParameter(key, String.valueOf(value)));
        }
      });
    }
    String targetUrl = targetUrlBuilder.build().toString();
    mktEvent.setTargetUrl(targetUrl);
    String referer = request.getHeader(Constants.REFERER);
    mktEvent.setReferrer(referer);

    LOGGER.info("call MCS targetUrl {} referer {}", targetUrl, referer);

    // async call mcs to record ubi
    builder.async().post(Entity.json(mktEvent));

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

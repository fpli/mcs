package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.LBSConstants;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSQueryResult;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.kernel.domain.lookup.biz.LookupBoHelperCfg;
import com.ebay.kernel.patternmatch.dawg.Dawg;
import com.ebay.kernel.patternmatch.dawg.DawgDictionary;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.raptor.device.fingerprint.client.api.DeviceFingerPrintClient;
import com.ebay.raptor.device.fingerprint.client.api.DeviceFingerPrintClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Zhiyuan Wang
 * @since 2019/9/24
 */
public class DAPResponseHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DAPResponseHandler.class);
  private static final String CONFIG_SUBFOLDER = "config/";

  private static final DawgDictionary userAgentBotDawgDictionary;
  private static final DawgDictionary ipBotDawgDictionary;

  private static final String MGVALUEREASON = "mgvaluereason";
  private static final String MGVALUE = "mgvalue";
  private static final String DEFAULT_MGVALUE = "0";

  private static final String DAP_USER_AGENT_ROBOT_FILE = "dap_user_agent_robot.txt";
  private static final String DAP_IP_ROBOT_FILE = "dap_ip_robot.txt";

  private static final List<String> BULLSEYE_MODEL_911_ATTRIBUTES = Arrays.asList(
          "LastItemsViewed2",
          "LastItemsWatched2",
          "LastItemsBidOrBin",
          "LastItemsLost2",
          "LastItemsPurchased2",
          "LastItemsBidOn2",
          "LastQueriesUsed",
          "CouponData",
          "MaritalStatus",
          "NumChildren",
          "EstIncome",
          "Gender",
          "Occupation",
          "Age",
          "LeftNegativeFeedBack",
          "AdChoice",
          "HasUserFiledINR",
          "HasUserContactedCS",
          "LastCategoriesAccessed_Agg",
          "MainCategories"
  );

  static {
    List<String> userAgentBotList = null;
    List<String> ipBotList = null;

    try {
      userAgentBotList = Files.readAllLines(Paths.get(RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + DAP_USER_AGENT_ROBOT_FILE));
      ipBotList = Files.readAllLines(Paths.get(RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + DAP_IP_ROBOT_FILE));
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }

    userAgentBotDawgDictionary = new DawgDictionary(Objects.requireNonNull(userAgentBotList).toArray(new String[0]), true);
    ipBotDawgDictionary = new DawgDictionary(Objects.requireNonNull(ipBotList).toArray(new String[0]), true);
  }

  private HttpServletRequest request;
  private HttpServletResponse response;
  private CookieReader cookieReader;
  private IEndUserContext endUserContext;
  private ContainerRequestContext requestContext;
  private URIBuilder uriBuilder;

  public DAPResponseHandler(HttpServletRequest request, HttpServletResponse response, CookieReader cookieReader,
                            IEndUserContext endUserContext, ContainerRequestContext requestContext) {
    this.request = request;
    this.response = response;
    this.cookieReader = cookieReader;
    this.endUserContext = endUserContext;
    this.requestContext = requestContext;
    init();
  }

  private void init() {
    this.uriBuilder = new URIBuilder()
            .setScheme(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.schema"))
            .setHost(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.host"))
            .setPath(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.path"));
  }

  public void sendDAPResponse()
          throws URISyntaxException, IOException {
    String cguid = getCguid();
    String guid = getGuid();

    setRequestParameters();
    setRvrId(System.currentTimeMillis());
    setReferrer();
    setGeoInfo();
    setCguid(cguid);
    setGuid(guid);
    setUdid();
    setMgInfo(cguid);
    setUserInfo(cguid);
    callDAPResponse();
  }

  private String getCguid() {
    String readerCguid = cookieReader.getCguid(requestContext);
    if (StringUtils.isEmpty(readerCguid)) {
      return null;
    }
    if (readerCguid.length() < Constants.CGUID_LENGTH) {
      return null;
    }
    return readerCguid.substring(0, Constants.CGUID_LENGTH);
  }

  private String getGuid() {
    String readerGuid = cookieReader.getGuid(requestContext);
    if (StringUtils.isEmpty(readerGuid)) {
      return null;
    }
    if (readerGuid.length() < Constants.GUID_LENGTH) {
      return null;
    }
    return readerGuid.substring(0, Constants.GUID_LENGTH);
  }

  private void setMgInfo(String cguid) {
    boolean isBot = isBotByIp(endUserContext.getIPAddress()) || isBotByUserAgent(endUserContext.getUserAgent());
    if (isBot) {
      uriBuilder.setParameter(MGVALUE, DEFAULT_MGVALUE);
      uriBuilder.setParameter(MGVALUEREASON, String.valueOf(MgvalueReason.BOT.getId()));
      return;
    }
    if (StringUtils.isEmpty(cguid)) {
      uriBuilder.setParameter(MGVALUE, DEFAULT_MGVALUE);
      uriBuilder.setParameter(MGVALUEREASON, String.valueOf(MgvalueReason.TRACKING_ERROR.getId()));
    } else {
      ImmutablePair<String, MgvalueReason> mgvalueAndMgvalueReason = getMgvalueAndMgvalueReason(cguid);
      uriBuilder.setParameter(MGVALUE, mgvalueAndMgvalueReason.left);
      uriBuilder.setParameter(MGVALUEREASON, String.valueOf(mgvalueAndMgvalueReason.right.getId()));
      if (!mgvalueAndMgvalueReason.left.equals(DEFAULT_MGVALUE)) {
        return;
      }
    }

    String mid = getMid();
    if (StringUtils.isEmpty(mid)) {
      // replace mgvaluereason
      uriBuilder.setParameter(MGVALUEREASON, String.valueOf(MgvalueReason.TRUST_CALL_ERROR.getId()));
      return;
    }
    uriBuilder.setParameter(Constants.MID, mid);
  }

  private ImmutablePair<String, MgvalueReason> getMgvalueAndMgvalueReason(String cguid) {
    // TODO remove hacked cguid
    cguid = "49bb481e14d0af4285d7b4b7fffffff6";
    String mgvalue = null;
    MgvalueReason mgvalueReason = null;
    Configuration config = ConfigurationBuilder.newConfig("idlinksvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:bf59aef8-25de-4140-acc5-2d7ddc290ecb");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    try (Response ress = mktClient.target(endpoint).path("/idlink")
            .queryParam("id", cguid)
            .queryParam("type", Constants.CGUID)
            .request()
            .get()) {
      switch (ress.getStatus()) {
        case HttpStatus.SC_NOT_FOUND:
          mgvalue = DEFAULT_MGVALUE;
          mgvalueReason = MgvalueReason.NEW_USER;
          break;
        case HttpStatus.SC_OK:
          String msg = ress.readEntity(String.class);
          JSONObject obj = new JSONObject(msg);
          JSONArray type = (JSONArray) obj.get("identityLinking");
          int size = type.length();
          for (int j = 0; j < size; j++) {
            JSONObject first = (JSONObject) type.get(j);
            String value = first.getString("type");
            if (value.equalsIgnoreCase("MGVALUE")) {
              JSONArray arr = first.getJSONArray("ids");
              // fetch the last inserted mgvalue
              mgvalue = arr.get(arr.length() - 1).toString();
              mgvalueReason = MgvalueReason.SUCCESS;
              break;
            }
          }
          if (mgvalue == null) {
            mgvalue = DEFAULT_MGVALUE;
            mgvalueReason = MgvalueReason.NEW_USER;
          }
          break;
        default:
          mgvalue = DEFAULT_MGVALUE;
          mgvalueReason = MgvalueReason.IDLINK_CALL_ERROR;
      }
    } catch(Exception exception) {
      mgvalue = DEFAULT_MGVALUE;
      mgvalueReason = MgvalueReason.IDLINK_CALL_ERROR;
    }

    return new ImmutablePair<>(mgvalue, mgvalueReason);
  }

  private String getMid() {
    String mId = null;
    LookupBoHelperCfg.initialize();
    try {
      DeviceFingerPrintClient dfpClient = DeviceFingerPrintClientFactory.getDeviceFingerPrintClient();
      mId = dfpClient.getMid();
    } catch (Exception e) {
      LOGGER.info(e.getMessage());
    }
    return mId;
  }

  private void setCguid(String cguid) {
    if (cguid != null) {
      uriBuilder.setParameter(Constants.CGUID, cguid);
    }
  }

  private void setGuid(String guid) {
    if (guid != null) {
      uriBuilder.setParameter(Constants.GUID, guid);
    }
  }

  private void setUserInfo(String cguid) {
    // TODO remove hacked cguid
    cguid = "de22859016c0ada3c18276d3ef68d372";
    Configuration config = ConfigurationBuilder.newConfig("bullseyesvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:e029558f-eff6-4f09-932d-9b774d2ec8bb");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    try (Response ress = mktClient.target(endpoint).path("/timeline")
            .queryParam("modelid","911")
            .queryParam("cguid", cguid)
            .queryParam("attrs", StringUtils.join(BULLSEYE_MODEL_911_ATTRIBUTES, ","))
            .request()
            .get()) {
      if (ress.getStatus() == HttpStatus.SC_OK) {
        String msg = ress.readEntity(String.class);
        JSONArray results = new JSONArray(msg);
        if (results.length() != 0) {
          JSONObject first = results.getJSONObject(0);
          JSONObject results1 = first.getJSONObject("results");
          JSONObject bullseyeResponse = results1.getJSONObject("response");
          for (String key : bullseyeResponse.keySet()) {
            extract(key, bullseyeResponse.get(key));
          }
        }
      }
    } catch(Exception e) {
      LOGGER.error(e.getMessage());
    }
  }

  private void setLastItems(String key, Object value) {
    JSONArray value1 = (JSONArray) value;
    List<String> itemList = new ArrayList<>();
    for (int i = 0; i < value1.length(); i++) {
      if (i == 5) {
        break;
      }
      List<Object> list = new ArrayList<>();
      JSONObject object = (JSONObject) value1.get(i);
      list.add(object.get("timestamp"));
      list.add(object.get("itemid"));
      list.add(object.get("categoryid"));
      list.add(object.get("siteid"));
      list.add(object.get("itemtitle"));
      itemList.add(StringUtils.join(list, ":"));
    }
    uriBuilder.setParameter(key, StringUtils.join(itemList, ","));
  }

  private void setLastQuery(String key, Object value) {
    JSONArray value1 = (JSONArray) value;
    List<String> itemList = new ArrayList<>();
    for (int i = 0; i < value1.length(); i++) {
      if (i == 5) {
        break;
      }
      List<Object> list = new ArrayList<>();
      JSONObject object = (JSONObject) value1.get(i);
      list.add(object.get("timestamp"));
      list.add(object.get("searchquery"));
      list.add(object.get("categoryid"));
      itemList.add(StringUtils.join(list, ":"));
    }
    uriBuilder.setParameter(key, StringUtils.join(itemList, ","));
  }

  private void setCategory(String key, Object value) {
    JSONArray value1 = (JSONArray) value;
    List<String> itemList = new ArrayList<>();
    for (int i = 0; i < value1.length(); i++) {
      if (i == 5) {
        break;
      }
      List<Object> list = new ArrayList<>();
      JSONObject object = (JSONObject) value1.get(i);
      list.add(object.get("categoryid"));
      list.add(object.get("siteid"));
      itemList.add(StringUtils.join(list, ":"));
    }
    uriBuilder.setParameter(key, StringUtils.join(itemList, ","));
  }

  private void extract(String key, Object value) {
    if (StringConstants.EMPTY.equals(value)) {
      return;
    }
    switch (key) {
      case "LastItemsViewed2":
        setLastItems("LastItemsViewed2",  value);
        break;
      case "LastItemsWatched2":
        setLastItems("LastItemsWatched2",  value);
        break;
      case "LastItemsBidOrBin":
        setLastItems("LastItemsBidOrBin",  value);
        break;
      case "LastItemsLost2":
        setLastItems("LastItemsLost2",  value);
        break;
      case "LastItemsPurchased2":
        setLastItems("LastItemsPurchased2",  value);
        break;
      case "LastItemsBidOn2":
        setLastItems("LastItemsBidOn2",  value);
        break;
      case "LastQueriesUsed":
        setLastQuery("LastQueriesUsed", value);
        break;
      case "CouponData":
        uriBuilder.setParameter("CouponData", String.valueOf(value));
        break;
      case "MaritalStatus":
        uriBuilder.setParameter("MaritalStatus", String.valueOf(value));
        break;
      case "NumChildren":
        uriBuilder.setParameter("NumChildren", String.valueOf(value));
        break;
      case "EstIncome":
        uriBuilder.setParameter("EstIncome", String.valueOf(value));
        break;
      case "Gender":
        uriBuilder.setParameter("Gender", String.valueOf(value));
        break;
      case "Occupation":
        uriBuilder.setParameter("Occupation", String.valueOf(value));
        break;
      case "Age":
        uriBuilder.setParameter("Age", String.valueOf(value));
        break;
      case "LeftNegativeFeedBack":
        uriBuilder.setParameter("LeftNegativeFeedBack", String.valueOf(value));
        break;
      case "AdChoice":
        if ("1".equals(value)) {
          uriBuilder.setParameter("AdChoicePreference", "true");
        }
        break;
      case "HasUserFiledINR":
        uriBuilder.setParameter("HasUserFiledINR", String.valueOf(value));
        break;
      case "HasUserContactedCS":
        uriBuilder.setParameter("HasUserContactedCS", String.valueOf(value));
        break;
      case "LastCategoriesAccessed_Agg":
        setCategory("LastCategoriesAccessed_Agg", value);
        break;
      case "MainCategories":
        setCategory("MainCategories", value);
        break;
      case "UserId":
        break;
      case "ModelId":
        break;
      default:
        throw new IllegalArgumentException("Invalid Attribute");
    }
  }

  private void setUdid() {
    String deviceId = endUserContext.getDeviceId();
    if (deviceId != null) {
      uriBuilder.setParameter(Constants.UNIQUE_DEVICE_ID, deviceId);
    }
  }

  // TODO send rvr id to MCS
  private void setRvrId(long startTime) {
    uriBuilder.setParameter(Constants.RVR_ID, String.valueOf(getRvrId(startTime)));
  }

  /**
   * Append all request parameters to url
   */
  private void setRequestParameters() {
    Map<String, String[]> params = request.getParameterMap();
    MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
    params.forEach((key, values) -> Arrays.stream(values).forEach(value -> parameters.add(key, value)));

    parameters.remove(Constants.IPN);
    parameters.remove(Constants.MPT);

    parameters.forEach((name, values) -> {
      if (name.toUpperCase().startsWith(Constants.ICEP_PREFIX)) {
        values.forEach(value -> uriBuilder.setParameter(name.substring(Constants.ICEP_PREFIX.length()), value));
      } else {
        values.forEach(value -> uriBuilder.setParameter(name, value));
      }
    });
  }

  private void callDAPResponse() throws URISyntaxException, IOException {
    HttpContext context = HttpClientContext.create();
    HttpGet httpget = new HttpGet(uriBuilder.build());

    try (CloseableHttpClient httpclient = HttpClients.createDefault();
         CloseableHttpResponse dapResponse = httpclient.execute(httpget, context);
         OutputStream os = response.getOutputStream()) {
      for (Header header : dapResponse.getAllHeaders()) {
        response.setHeader(header.getName(), header.getValue());
      }
      byte[] bytes = EntityUtils.toByteArray(dapResponse.getEntity());
      os.write(bytes);
    }
  }

  private void setReferrer() {
    String referrer = request.getHeader(Constants.REFERER);
    if (referrer == null) {
      return;
    }
    uriBuilder.setParameter(Constants.REF_URL, referrer);
    String referrerDomain = getHostFromUrl(referrer);
    if (referrerDomain == null) {
      return;
    }
    uriBuilder.setParameter(Constants.REF_DOMAIN, referrerDomain);
  }

  private String getCountryFromBrowserLocale(HttpServletRequest request) {
    String countryCode;

    String acceptLangs;
    String[] acceptLangsArray = null;
    String localeName;

    acceptLangs = request.getHeader(Constants.HTTP_ACCEPT_LANGUAGE);

    if (!StringUtils.isEmpty(acceptLangs)) {
      acceptLangsArray = acceptLangs.split(StringConstants.COMMA);
    }

    if (acceptLangsArray != null && acceptLangsArray.length > 0) {
      localeName = acceptLangsArray[0];
      Locale locale = convertLocaleNameToLocale(localeName);
      if (locale != null) {
        countryCode = locale.getCountry();
        if (isValidCountryCode(countryCode)) {
          return countryCode;
        }
      }
    }
    return null;
  }

  private boolean isValidCountryCode(String countryCode) {
    if (countryCode == null) {
      return false;
    }
    return countryCode.length() == Constants.ISO_COUNTRY_CODE_LENGTH;
  }

  private Locale convertLocaleNameToLocale(String localeName) {
    String[] localeNamePieces;
    String langCode;
    String countryCode = StringConstants.EMPTY;

    if (localeName != null && localeName.trim().length() > 0) {
      localeNamePieces = localeName.split(StringConstants.HYPHEN);
      langCode = localeNamePieces[0];
      if (localeNamePieces.length > 1) {
        countryCode = localeNamePieces[1];
      }
      return new Locale(langCode, countryCode);
    }
    return null;
  }

  private String getHostFromUrl(String url) {
    try {
      URL u = new URL(url);
      return u.getHost().toLowerCase();
    } catch (Exception e) {
      return null;
    }
  }

  // TODO
  private long getRvrId(long startTime) {
    return 2056960579986L;
  }

  /**
   * Set LBS info
   */
  private void setGeoInfo() {
    Map<String, String> lbsParameters = getLBSParameters(endUserContext.getIPAddress());

    String countryFromBrowserLocale = getCountryFromBrowserLocale(request);
    if (!StringUtils.isEmpty(countryFromBrowserLocale)) {
      uriBuilder.setParameter(LBSConstants.GEO_COUNTRY_CODE, countryFromBrowserLocale);
      lbsParameters.remove(LBSConstants.GEO_COUNTRY_CODE);
    }
    lbsParameters.forEach(uriBuilder::setParameter);
  }

  private Map<String, String> getLBSParameters(String ip) {
    Map<String, String> map = new HashMap<>();

    LBSQueryResult lbsResponse = LBSClient.getInstance().getLBSInfo(ip);
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
    return map;
  }

  /**
   * Check if this request is bot with userAgent
   * @param userAgent alias for user agent
   * @return is bot or not
   */
  private boolean isBotByUserAgent(String userAgent) {
    return isBot(userAgent, userAgentBotDawgDictionary);
  }

  /**
   * Check if this request is bot with ip
   * @param ip alias for remote ip
   * @return is bot or not
   */
  private boolean isBotByIp(String ip) {
    return isBot(ip, ipBotDawgDictionary);
  }

  private boolean isBot(String info, DawgDictionary dawgDictionary) {
    if (StringUtils.isEmpty(info)) {
      return false;
    }
    Dawg dawg = new Dawg(dawgDictionary);
    Map result = dawg.findAllWords(info.toLowerCase(), false);
    return !result.isEmpty();
  }
}

package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.LBSConstants;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSQueryResult;
import com.ebay.app.raptor.chocolate.common.DAPRvrId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
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
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

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
import java.util.*;

/**
 * @author Zhiyuan Wang
 * @since 2019/9/24
 */
@Component
public class DAPResponseHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DAPResponseHandler.class);

  private URIBuilder getDapURIBuilder() {
    return new URIBuilder()
            .setScheme(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.schema"))
            .setHost(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.host"))
            .setPath(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.path"));
  }

  public void sendDAPResponse(HttpServletRequest request, HttpServletResponse response, CookieReader cookieReader,
                                     IEndUserContext endUserContext, ContainerRequestContext requestContext)
          throws URISyntaxException, IOException {
    Map<String, String[]> params = request.getParameterMap();
    String cguid = getCguid(cookieReader, requestContext);
    String guid = getGuid(cookieReader, requestContext);
    String accountId = getAccountId(cookieReader, requestContext);
    String deviceId = endUserContext.getDeviceId();
    long dapRvrId = getDAPRvrId();
    String referrer = request.getHeader(Constants.REFERER);
    Map<String, String> lbsParameters = getLBSParameters(request, endUserContext);
    URIBuilder dapUriBuilder = getDapURIBuilder();

    setCguid(dapUriBuilder, cguid);
    setGuid(dapUriBuilder, guid);
    setRvrId(dapUriBuilder, dapRvrId);
    setRoverUserid(dapUriBuilder, accountId);
    setRequestParameters(dapUriBuilder, params);
    setReferrer(dapUriBuilder, referrer);
    setGeoInfo(dapUriBuilder, lbsParameters);
    setUdid(dapUriBuilder, deviceId);

    callDAPResponse(dapUriBuilder, response);

    sendToMCS(request, cookieReader, requestContext, params, dapRvrId);
  }

  private String getCguid(CookieReader cookieReader, ContainerRequestContext requestContext) {
    String readerCguid = cookieReader.getCguid(requestContext);
    if (StringUtils.isEmpty(readerCguid)) {
      return null;
    }
    if (readerCguid.length() < Constants.CGUID_LENGTH) {
      return null;
    }
    return readerCguid.substring(0, Constants.CGUID_LENGTH);
  }

  private String getGuid(CookieReader cookieReader, ContainerRequestContext requestContext) {
    String readerGuid = cookieReader.getGuid(requestContext);
    if (StringUtils.isEmpty(readerGuid)) {
      return null;
    }
    if (readerGuid.length() < Constants.GUID_LENGTH) {
      return null;
    }
    return readerGuid.substring(0, Constants.GUID_LENGTH);
  }

  private void setCguid(URIBuilder dapuUriBuilder, String cguid) {
    if (cguid != null) {
      dapuUriBuilder.setParameter(Constants.CGUID, cguid);
    }
  }

  private void setGuid(URIBuilder dapuUriBuilder, String guid) {
    if (guid != null) {
      dapuUriBuilder.setParameter(Constants.GUID, guid);
    }
  }

  private String getAccountId(CookieReader cookieReader, ContainerRequestContext requestContext) {
    return cookieReader.getAccountId(requestContext);
  }

  private void setUdid(URIBuilder dapuUriBuilder, String deviceId) {
    if (deviceId != null) {
      dapuUriBuilder.setParameter(Constants.UNIQUE_DEVICE_ID, deviceId);
    }
  }

  private void setRvrId(URIBuilder dapuUriBuilder, long dapRvrId) {
    dapuUriBuilder.setParameter(Constants.RVR_ID, String.valueOf(dapRvrId));
  }

  /**
   * Set rover_useid, DAP uses this id to get user attributes from Bullseye model 630
   * @param dapuUriBuilder dapUrlBuilder
   * @param roverUserid rover_userid
   */
  private void setRoverUserid(URIBuilder dapuUriBuilder, String roverUserid) {
    if (roverUserid != null) {
      dapuUriBuilder.setParameter(Constants.ROVER_USERID, roverUserid);
    }
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
  private void setRequestParameters(URIBuilder dapuUriBuilder, Map<String, String[]> params) {
    MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
    params.forEach((key, values) -> Arrays.stream(values).forEach(value -> parameters.add(key, value)));

    parameters.remove(Constants.IPN);
    parameters.remove(Constants.MPT);

    parameters.forEach((name, values) -> {
      if (name.toUpperCase().startsWith(Constants.ICEP_PREFIX)) {
        values.forEach(value -> dapuUriBuilder.setParameter(name.substring(Constants.ICEP_PREFIX.length()), value));
      } else {
        values.forEach(value -> dapuUriBuilder.setParameter(name, value));
      }
    });
  }

  /**
   * Call DAP interface and send response to browser
   */
  private void callDAPResponse(URIBuilder dapuUriBuilder, HttpServletResponse response) throws URISyntaxException, IOException {
    HttpContext context = HttpClientContext.create();
    HttpGet httpget = new HttpGet(dapuUriBuilder.build());

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

  private void sendToMCS(HttpServletRequest request, CookieReader cookieReader, ContainerRequestContext requestContext,
                         Map<String, String[]> params, long dapRvrId) throws URISyntaxException {
    Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    // add all headers except Cookie
    Invocation.Builder builder = mktClient.target(endpoint).path("/impression/").request();
    final Enumeration<String> headers = request.getHeaderNames();
    while (headers.hasMoreElements()) {
      String header = headers.nextElement();
      if ("Cookie".equalsIgnoreCase(header)) {
        String cguid = cookieReader.getCguid(requestContext).substring(0, 32);
        String guid = cookieReader.getGuid(requestContext).substring(0, 32);
        builder = builder.header("X-EBAY-C-TRACKING", "guid=" + guid + "," + "cguid=" + cguid);
        continue;
      }
      String values = request.getHeader(header);
      builder = builder.header(header, values);
    }

    // add uri and referer to marketing event body
    MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();

    URIBuilder mcsUriBuilder = new URIBuilder()
            .setScheme("http")
            .setHost("www.ebay.com")
            .setPath("/test");

    mcsUriBuilder.setParameter(Constants.MKEVT, String.valueOf(MKEVT.AD_REQUEST.getId()));
    params.forEach((name, values) -> Arrays.stream(values).forEach(value -> mcsUriBuilder.setParameter(name, value)));
    mcsUriBuilder.setParameter(Constants.RVRID, String.valueOf(dapRvrId));

    mktEvent.setTargetUrl(mcsUriBuilder.build().toString());
    mktEvent.setReferrer(request.getHeader(Constants.REFERER));

    // call marketing collection service to send ubi event or send kafka
    try (Response res = builder.post(Entity.json(mktEvent))) {
      if (res.getStatus() == Response.Status.OK.getStatusCode()) {
        LOGGER.info("Send to MCS success.");
      } else {
        LOGGER.error("Send to MCS failed.");
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
  }

  private void setReferrer(URIBuilder dapuUriBuilder, String referrer) {
    if (referrer == null) {
      return;
    }
    dapuUriBuilder.setParameter(Constants.REF_URL, referrer);
    String referrerDomain = getHostFromUrl(referrer);
    if (referrerDomain == null) {
      return;
    }
    dapuUriBuilder.setParameter(Constants.REF_DOMAIN, referrerDomain);
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
    lbsParameters.forEach(dapuUriBuilder::setParameter);
  }

  /**
   * Get geo info from location base service and pass all the info to DAP
   */
  private Map<String, String> getLBSParameters(HttpServletRequest request, IEndUserContext endUserContext) {
    Map<String, String> map = new HashMap<>();

    LBSQueryResult lbsResponse = LBSClient.getInstance().getLBSInfo(endUserContext.getIPAddress());
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

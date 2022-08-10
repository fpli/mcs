package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.CollectionService;
import com.ebay.app.raptor.chocolate.adservice.component.GdprConsentHandler;
import com.ebay.app.raptor.chocolate.adservice.constant.*;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSQueryResult;
import com.ebay.app.raptor.chocolate.adservice.util.*;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.ClientDataEnum;
import com.ebay.app.raptor.chocolate.gen.api.*;
import com.ebay.app.raptor.chocolate.gen.model.AkamaiEvent;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.jaxrs.client.config.bean.spi.JaxrsClientBaseConfigBean;
import com.ebay.kernel.util.RequestUtil;
import com.ebay.raptor.geo.utils.GeoUtils;
import com.ebay.raptor.opentracing.SpanEventHelper;
import com.ebay.traffic.monitoring.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.client.*;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.CompletableFuture;


/**
 * Resource class
 *
 * @author xiangli4
 */

@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
public class AdserviceResource implements ArApi, ImpressionApi, RedirectApi, GuidApi, UseridApi, SyncApi, EpntApi, AkamaiApi {
  private static final Logger logger = LoggerFactory.getLogger(AdserviceResource.class);

  private static final String TYPE_INFO = "Info";
  private static final String STATUS_OK = "0";

  @Autowired
  private CollectionService collectionService;

  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

  @Context
  private ContainerRequestContext requestContext;

  @Autowired
  private AdserviceCookie adserviceCookie;

  @Autowired
  @Qualifier("cb")
  private IdMapable idMapping;

  @Autowired
  private GdprConsentHandler gdprConsentHandler;

  private static final String METRIC_ADD_MAPPING_SUCCESS = "METRIC_ADD_MAPPING_SUCCESS";
  private static final String METRIC_ADD_MAPPING_FAIL = "METRIC_ADD_MAPPING_FAIL";
  private static final String METRIC_NO_MKCID_IN_AR = "METRIC_NO_MKCID_IN_AR";
  private static final String METRIC_NO_MKCID_IN_IMPRESSION = "METRIC_NO_MKCID_IN_IMPRESSION";
  private static final String METRIC_NO_MKRID_IN_AR = "METRIC_NO_MKRID_IN_AR";
  private static final String METRIC_INCOMING_REQUEST = "METRIC_INCOMING_REQUEST";
  private static final String METRIC_NO_MKRID_IN_IMPRESSION = "METRIC_NO_MKRID_IN_IMPRESSION";
  private static final String METRIC_ERROR_IN_ASYNC_MODEL = "METRIC_ERROR_IN_ASYNC_MODEL";
  private static final String[] ADOBE_PARAMS_LIST = {"id", "ap_visitorId", "ap_category", "ap_deliveryId",
      "ap_oid", "data"};
  private static final int GUID_LIST_MAX_SIZE = 20;

  private static final String CAL_TRANSACTION_TYPE = "mktAdservice";
  private static final String CAL_TRANSACTION_AR = "ar";
  private static final String CAL_TRANSACTION_IMPRESSION = "impression";
  private static final String CAL_TRANSACTION_STATUS_ZERO = "0";

  // get the adobe service info from application.properties in resources dir
  private static Configuration adobeConfig = ConfigurationBuilder.newConfig("adservice.mktAdobeClient");
  private static Client adobeClient = GingerClientBuilder.newClient(adobeConfig);
  private static String adobeEndpoint = (String) adobeClient.getConfiguration().getProperty(EndpointUri.KEY);

  // build ginger client to call mcs
  private static final Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient",
      "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46");
  private static final Client mktClient = GingerClientBuilder.newClient(config);
  private static final String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

  /**
   * Initialize function
   */
  @PostConstruct
  public void postInit() {
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
  }

  /**
   * AR method to collect ar and serve the ad
   *
   * @return response
   */
  @Override
  public Response ar(Integer mkcid, String mkrid, Integer mkevt, String mksid) {
    SpanEventHelper.writeEvent(TYPE_INFO, "request", STATUS_OK,
            request.getPathInfo() + "?" + request.getQueryString());
    if (null == mkcid) {
      MonitorUtil.info(METRIC_NO_MKCID_IN_AR);
    }
    if (null == mkrid) {
      MonitorUtil.info(METRIC_NO_MKRID_IN_AR);
    }
    MonitorUtil.info(METRIC_INCOMING_REQUEST, 1, Field.of("path", "ar"));

    Response res = null;

    GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(request);

    try {
      if (gdprConsentDomain.isAllowedSetCookie()) {
        adserviceCookie.setAdguid(request, response);
      }
      response.addHeader(Headers.ACCEPT_CH, ClientDataEnum.getClientHintHeaders());
      collectionService.collectAr(request, response, gdprConsentDomain);
      if (HttpServletResponse.SC_MOVED_PERMANENTLY == response.getStatus()) {
        Response.ResponseBuilder responseBuilder = Response.status(Response.Status.MOVED_PERMANENTLY);
        for (String headerName : response.getHeaderNames()) {
          responseBuilder.header(headerName, response.getHeader(headerName));
        }
        res = responseBuilder.build();
      } else {
        res = Response.status(Response.Status.OK).build();
      }
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
      try {
        res = Response.status(Response.Status.BAD_REQUEST).build();
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    }
    return res;
  }
  /**
   * Get method to collect impression, viewimp, email open
   *
   * @return response
   */
  @Override
  public Response impression(Integer mkcid, String mkrid, Integer mkevt, String mksid) {
    SpanEventHelper.writeEvent(TYPE_INFO, "request", STATUS_OK,
            request.getPathInfo() + "?" + request.getQueryString());
    if (null == mkcid) {
      MonitorUtil.info(METRIC_NO_MKCID_IN_IMPRESSION);
    }
    if (null == mkrid) {
      MonitorUtil.info(METRIC_NO_MKRID_IN_IMPRESSION);
    }
    MonitorUtil.info(METRIC_INCOMING_REQUEST, 1, Field.of("path", "impression"));
    Response res = null;
    try {
      String adguid = adserviceCookie.setAdguid(request, response);
      response.addHeader(Headers.ACCEPT_CH, ClientDataEnum.getClientHintHeaders());

      res = Response.status(Response.Status.OK).build();

      // get channel
      String channelType = null;
      Map<String, String[]> params = request.getParameterMap();
      if (params.containsKey(Constants.MKCID) && params.get(Constants.MKCID)[0] != null) {
        channelType = ChannelIdEnum.parse(params.get(Constants.MKCID)[0]).getLogicalChannel().getAvro().toString();
      }

      // get partner
      String partner = null;
      if (params.containsKey(Constants.MKPID) && params.get(Constants.MKPID)[0] != null) {
        partner = EmailPartnerIdEnum.parse(params.get(Constants.MKPID)[0]);
      }

      long startTime = startTimerAndLogData(Field.of(Constants.CHANNEL_TYPE, channelType),
          Field.of(Constants.PARTNER, partner));

      // disable Ginger Client to append Ginger agent to the user agent header
      mktClient.property(JaxrsClientBaseConfigBean.SEND_USER_AGENT, false);

      // call mcs
      Builder builder = mktClient.target(endpoint).path("/impression/").request();

      // add all headers
      final Enumeration<String> headers = request.getHeaderNames();
      while (headers.hasMoreElements()) {
        String header = headers.nextElement();
        String values = request.getHeader(header);
        builder = builder.header(header, values);
      }

      // construct X-EBAY-C-TRACKING header
      String guid = adserviceCookie.getGuid(request);
      String guidList = adserviceCookie.getGuidList(request);
      builder = builder.header(Constants.TRACKING_HEADER,
          HttpUtil.constructTrackingHeader(guid, guidList, adguid));

      // add parameters separately to handle special characters
      URIBuilder uri = new URIBuilder(request.getRequestURL().toString());
      Map<String, String[]> parameterMap = request.getParameterMap();
      Iterator<Map.Entry<String, String[]>> iter = parameterMap.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, String[]> entry = iter.next();
        for (String value : entry.getValue()) {
          uri.addParameter(entry.getKey(), value);
        }
      }

      // for email open, call LBS to get buyer access site id
      if (params.containsKey(Constants.MKEVT) && MKEVT.EMAIL_OPEN.getId().equals(params.get(Constants.MKEVT)[0])) {
        int siteId = 0;
        LBSQueryResult lbsQueryResult = LBSClient.getInstance().getLBSInfo(getRemoteIp(request));
        if (lbsQueryResult != null) {
          String country = lbsQueryResult.getIsoCountryCode2();
          siteId = GeoUtils.getSiteIdByISOCountryCode(country);
        }

        // add bs tag into url parameter
        uri.addParameter(Constants.CHOCO_BUYER_ACCESS_SITE_ID, String.valueOf(siteId));
      }

      // add uri and referer to marketing event body
      MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
      mktEvent.setTargetUrl(uri.build().toString());
      mktEvent.setReferrer(request.getHeader("Referer"));

      // call marketing collection service to send ubi event or send kafka async
      builder.async().post(Entity.json(mktEvent), new MCSCallback());
      // send 1x1 pixel
      ImageResponseHandler.sendImageResponse(response);

      // send open events to adobe
//      if (!StringUtils.isEmpty(partner) && EmailPartnerIdEnum.ADOBE.getPartner().equals(partner)) {
//        sendOpenEventToAdobe(params);
//      }

      stopTimerAndLogData(startTime, Field.of(Constants.CHANNEL_TYPE, channelType),
        Field.of(Constants.PARTNER, partner));

    } catch (Exception e) {
      try {
        logger.warn("Impression request process failed, url: {}", request.getRequestURL().append("?")
            .append(request.getQueryString()).toString());
        res = Response.status(Response.Status.BAD_REQUEST).build();
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    }
    return res;
  }

  /**
   * Redirect entrance. Only for CRM
   *
   * @return response
   */
  @Override
  public Response redirect(Integer mkcid, String mkrid, Integer mkevt, String mksid) {
    MonitorUtil.info(METRIC_INCOMING_REQUEST, 1, Field.of("path", "redirect"));
    adserviceCookie.setAdguid(request, response);
    response.addHeader(Headers.ACCEPT_CH, ClientDataEnum.getClientHintHeaders());
    URI redirectUri = null;
    try {
      // assign home page as default redirect url
      URIBuilder uriBuilder = new URIBuilder(ApplicationOptions.getInstance().getRedirectHomepage());
      // add all parameters except landing page parameter to the home page
      MultiValueMap<String, String> parameters = HttpUtil.parse(request.getParameterMap());
      // make sure mkevt=1 to let home page go to mcs
      // no mkevt, then add mkevt=1
      if (!parameters.containsKey(Constants.MKEVT)) {
        parameters.add(Constants.MKEVT, MKEVT.MARKETING_EVENT.getId());
      }
      // invalid mkevt or empty mkevt, then correct it
      if (parameters.getFirst(Constants.MKEVT) == null ||
          !MKEVT.MARKETING_EVENT.getId().equals(parameters.getFirst(Constants.MKEVT))) {
        Map<String, String> mkevtParam = new HashMap<>();
        mkevtParam.put(Constants.MKEVT, MKEVT.MARKETING_EVENT.getId());
        parameters.setAll(mkevtParam);
      }
      Set<String> keySet = parameters.keySet();
      for (String key : keySet) {
        if (Arrays.asList(Constants.getTargetUrlParms()).contains(key)) {
          continue;
        }
        uriBuilder.addParameter(key, parameters.getFirst(key));
      }

      redirectUri = uriBuilder.build();

      // get redirect url
      redirectUri = collectionService.collectRedirect(request, response, mktClient, endpoint);
    } catch (Exception e) {
      // When exception happen, redirect to www.ebay.com
      logger.warn(e.getMessage(), e);
    }
    if (redirectUri != null) {
      return Response.status(Response.Status.MOVED_PERMANENTLY).location(redirectUri).build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  /**
   * Sync command used to map cookie
   *
   * @return response
   */
  @Override
  public Response sync(String guid, String uid) {
    MonitorUtil.info(METRIC_INCOMING_REQUEST, 1, Field.of("path", "sync"));
    String adguid = adserviceCookie.setAdguid(request, response);
    response.addHeader(Headers.ACCEPT_CH, ClientDataEnum.getClientHintHeaders());
    Response res = Response.status(Response.Status.OK).build();
    ImageResponseHandler.sendImageResponse(response);

//    // add all headers except Cookie
//    Builder builder = mktClient.target(endpoint).path("/sync/").request();
//    final Enumeration<String> headers = request.getHeaderNames();
//    while (headers.hasMoreElements()) {
//      String header = headers.nextElement();
//      if ("Cookie".equalsIgnoreCase(header)) {
//        continue;
//      }
//      String values = request.getHeader(header);
//      builder = builder.header(header, values);
//    }
//    // forward to mcs for writing ubi. The adguid in ubi is to help XID team build adguid into the linking system.
//    // construct X-EBAY-C-TRACKING header
//    builder = builder.header(Constants.TRACKING_HEADER,
//        HttpUtil.constructTrackingHeader(guid, adguid));
//
//    // add uri and referer to marketing event body
//    MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
//    // append adguid into the url so that the mcs can parse it
//    mktEvent.setTargetUrl(new ServletServerHttpRequest(request).getURI().toString() + "&adguid=" + adguid);
//    mktEvent.setReferrer(request.getHeader("Referer"));

    // call marketing collection service to send ubi event async
    // Stop writing to Soj per site tracking's ask. It's external.
    // builder.async().post(Entity.json(mktEvent), new MCSCallback());

    // build guid list <=> adguid <=> user id mapping
    try {
      if (StringUtils.isNotEmpty(guid) && guid.length() >= Constants.GUID_LENGTH) {
        guid = guid.substring(0, Constants.GUID_LENGTH);
        String guidList = idMapping.getGuidListByAdguid(adguid);
        String newGuidList;
        if (StringUtils.isNotEmpty(guidList)) {
          List<String> guids = new ArrayList<>(Arrays.asList(guidList.split(StringConstants.AND)));
          if (!guids.contains(guid)) {
            guids.add(guid);
            // limit the number of guid to 20
            if (guids.size() > GUID_LIST_MAX_SIZE) {
              guids = guids.subList(guids.size()-GUID_LIST_MAX_SIZE, guids.size());
            }
            newGuidList = StringUtils.join(guids, StringConstants.AND);
          } else {
            newGuidList = guidList;
          }
        } else {
          newGuidList = guid;
        }

        // If the guid is already existed in the mapping, no need to update mapping
        if (!guidList.equals(newGuidList)) {
          boolean isAddMappingSuccess = idMapping.addMapping(adguid, newGuidList, guid, uid);
          if (isAddMappingSuccess) {
            MonitorUtil.info(METRIC_ADD_MAPPING_SUCCESS);
          } else {
            MonitorUtil.info(METRIC_ADD_MAPPING_FAIL);
          }
        }
      }
    } catch (Exception e) {
      try {
        MonitorUtil.info(METRIC_ADD_MAPPING_FAIL);

      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    }
    return res;
  }

  /**
   * Get GUID from mapping
   *
   * @return guid in string
   */
  @Override
  public Response guid() {
    MonitorUtil.info(METRIC_INCOMING_REQUEST, 1, Field.of("path", "guid"));
    String adguid = adserviceCookie.readAdguid(request);
    String guid = idMapping.getGuidByAdguid(adguid);
    return Response.status(Response.Status.OK).entity(guid).build();
  }

  /**
   * Get user id from mapping
   *
   * @return user id in string
   */
  @Override
  public Response userid() {
    MonitorUtil.info(METRIC_INCOMING_REQUEST, 1, Field.of("path", "userid"));
    String adguid = adserviceCookie.readAdguid(request);
    String encryptedUserid = idMapping.getUidByAdguid(adguid);
    return Response.status(Response.Status.OK).entity(encryptedUserid).build();
  }

  /**
   * Collect Akamai events
   */
  @Override
  public Response akamai(AkamaiEvent body, String xChocoAuth) {
    // call MCS to send akamai events
    String token = "akamai:chocolate_collection";
    String encodedToken = Base64.getEncoder().encodeToString(token.getBytes());
    if (StringUtils.isEmpty(xChocoAuth) || !xChocoAuth.equals(encodedToken)) {
      return Response.status(Response.Status.UNAUTHORIZED).entity("Unauthorized").build();
    } else {
      Builder builder = mktClient.target(endpoint).path("/akamai").request();
      builder.async().post(Entity.json(body), new MCSCallback());

      return Response.status(Response.Status.OK).build();
    }
  }

  /**
   * Get personalization ads for ebay partner network by placement parameters
   * @return response
   */
  @Override
  public Response placement(String st, String cpid, String l, String ft, String tc, String clp, Integer mi,
                            String k, Integer ctids, String mkpid, String ur, String cts, String sf, String pid, String ad_v) {
    Response res = null;

    try {
      adserviceCookie.setAdguid(request, response);
      response.addHeader(Headers.ACCEPT_CH, ClientDataEnum.getClientHintHeaders());
      res = collectionService.collectEpntPlacementRedirect(request, response);
      } catch (Exception e) {
      logger.warn(e.getMessage(), e);
      try {
        res = Response.status(Response.Status.BAD_REQUEST).build();
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    }

    return res;
  }

  private void sendOpenEventToAdobe(Map<String, String[]> params) {
    URIBuilder uriBuilder = null;
    try {
      uriBuilder = generateAdobeUrl(params, adobeEndpoint);

      WebTarget webTarget = adobeClient.target(uriBuilder.build());
      // call Adobe service in async model
      asyncCall(webTarget.request().async());
    } catch (Exception ex) {
      logger.error("Send open event to Adobe exception", ex);
    }
  }

  /**
   * Generate the adobe URI,
   * the adobe server get from raptorio config
   * the paramter get from request's parameters
   */
  public URIBuilder generateAdobeUrl(Map<String, String[]> parameters, String endpoint)
      throws URISyntaxException, UnsupportedEncodingException {
    URIBuilder uriBuilder = new URIBuilder(endpoint);
    // if the url has no adobeParams, we will construct the url with the possible list of params
    if (!parameters.containsKey(Constants.ADOBE_PARAMS) || parameters.get(Constants.ADOBE_PARAMS)[0] == null) {
      logger.warn(Errors.ERROR_OPEN_NO_ADOBE_PARAMS);
      // construct the url with the possible list of params,  only "id" in the the params list is Mandatory
      for (String adobeParam : ADOBE_PARAMS_LIST) {
        if (parameters.containsKey(adobeParam)) {
          uriBuilder.addParameter(adobeParam, parameters.get(adobeParam)[0]);
        }
      }
      return uriBuilder;
    }

    String[] adobeParams = URLDecoder.decode(parameters.get(Constants.ADOBE_PARAMS)[0], "UTF-8").split(",");
    // check the value of adobeParams, it must be a subset of parameters in request
    for (String adobeParam : adobeParams) {
      if (parameters.containsKey(adobeParam)) {
        uriBuilder.addParameter(adobeParam, parameters.get(adobeParam)[0]);
      } else {
        logger.warn("adobeParams field has wrong parameter name: " + adobeParam);
      }
    }
    return uriBuilder;
  }

  /**
   * utility method for callback
   *
   * @param invoker
   * @return
   */
  private CompletableFuture<Response> asyncCall(AsyncInvoker invoker) {
    CompletableFuture<Response> cf = new CompletableFuture<>();
    invoker.get(new InvocationCallback<Response>() {
      @Override
      public void completed(Response s) {
        cf.complete(s);
      }

      @Override
      public void failed(Throwable throwable) {
        // If the session is failed, log a flag in metrics and throw a exception.
        // The exception will be logged by logger
        MonitorUtil.info(METRIC_ERROR_IN_ASYNC_MODEL);
        cf.completeExceptionally(throwable);
      }
    });
    return cf;
  }

  /**
   * Get remote ip
   */
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

  /**
   * Starts the timer and logs some basic info
   *
   * @param additionalFields channelType, partner
   * @return start time
   */
  private long startTimerAndLogData(Field<String, Object>... additionalFields) {
    long startTime = System.currentTimeMillis();
    logger.debug(String.format("StartTime: %d", startTime));
    MonitorUtil.info("ImpressionInput", 1, additionalFields);
    return startTime;
  }

  /**
   * Stops the timer and logs relevant debugging messages
   *
   * @param startTime        the start time, so that latency can be calculated
   * @param additionalFields channelType, partner
   */
  private void stopTimerAndLogData(long startTime, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    MonitorUtil.info("ImpressionSuccess", 1,additionalFields);
    MonitorUtil.latency("ImpressionLatency", endTime - startTime);
  }
}

package com.ebay.app.raptor.chocolate;


import com.ebay.app.raptor.chocolate.adservice.CollectionService;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.Errors;
import com.ebay.app.raptor.chocolate.adservice.util.AdserviceCookie;
import com.ebay.app.raptor.chocolate.adservice.util.ImageResponseHandler;
import com.ebay.app.raptor.chocolate.adservice.util.MarketingTrackingEvent;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.gen.api.*;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContextProvider;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.util.StringUtils;

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
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Resource class
 *
 * @author xiangli4
 */

@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
public class AdserviceResource implements ArApi, ImpressionApi, RedirectApi, GuidApi, UseridApi, SyncApi, EpntApi {
  private static final Logger logger = LoggerFactory.getLogger(AdserviceResource.class);
  @Autowired
  private CollectionService collectionService;

  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

  @Autowired
  private IEndUserContextProvider userCtxProvider;

  @Autowired
  private RaptorSecureContextProvider raptorSecureContextProvider;

  @Context
  private ContainerRequestContext requestContext;

  @Autowired
  private AdserviceCookie adserviceCookie;

  @Autowired
  @Qualifier("cb")
  private IdMapable idMapping;

  private Metrics metrics;

  private static final String METRIC_ADD_MAPPING_SUCCESS = "METRIC_ADD_MAPPING_SUCCESS";
  private static final String METRIC_ADD_MAPPING_FAIL = "METRIC_ADD_MAPPING_FAIL";
  private static final String METRIC_NO_MKCID_IN_AR = "METRIC_NO_MKCID_IN_AR";
  private static final String METRIC_NO_MKCID_IN_IMPRESSION = "METRIC_NO_MKCID_IN_IMPRESSION";
  private static final String METRIC_NO_MKRID_IN_AR = "METRIC_NO_MKRID_IN_AR";
  private static final String METRIC_NO_MKRID_IN_IMPRESSION = "METRIC_NO_MKRID_IN_IMPRESSION";
  private static final String METRIC_NO_CONFIGID_IN_EPNT = "METRIC_NO_CONFIGID_IN_EPNT";
  private static final String METRIC_ERROR_IN_ASYNC_MODEL = "METRIC_ERROR_IN_ASYNC_MODEL";
  private static final String[] ADOBE_PARAMS_LIST = {"id", "ap_visitorId", "ap_category", "ap_deliveryId",
      "ap_oid", "data"};

  // get the adobe service info from application.properties in resources dir
  private static Configuration adobeConfig = ConfigurationBuilder.newConfig("adservice.mktAdobeClient");
  private static Client adobeClient = GingerClientBuilder.newClient(adobeConfig);
  private static String adobeEndpoint = (String) adobeClient.getConfiguration().getProperty(EndpointUri.KEY);

  ExecutorService executorService =
      Executors.newFixedThreadPool(10);

  /**
   * Initialize function
   */
  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  /**
   * AR method to collect ar and serve the ad
   *
   * @return response
   */
  @Override
  public Response ar(Integer mkcid, String mkrid, Integer mkevt, String mksid) {
    if(null == mkcid) {
      metrics.meter(METRIC_NO_MKCID_IN_AR);
    }
    if(null == mkrid) {
      metrics.meter(METRIC_NO_MKRID_IN_AR);
    }
    Response res = null;
    try {
      adserviceCookie.setAdguid(request, response);
      collectionService.collectAr(request, response, requestContext);
      res = Response.status(Response.Status.OK).build();
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
    if(null == mkcid) {
      metrics.meter(METRIC_NO_MKCID_IN_IMPRESSION);
    }
    if(null == mkrid) {
      metrics.meter(METRIC_NO_MKRID_IN_IMPRESSION);
    }
    Response res = null;
    try {
      adserviceCookie.setAdguid(request, response);
      res = Response.status(Response.Status.OK).build();

      Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient",
          "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46");
      Client mktClient = GingerClientBuilder.newClient(config);
      String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

      // add all headers except Cookie
      Builder builder = mktClient.target(endpoint).path("/impression/").request();
      final Enumeration<String> headers = request.getHeaderNames();
      while (headers.hasMoreElements()) {
        String header = headers.nextElement();
        if ("Cookie".equalsIgnoreCase(header)) {
          continue;
        }
        String values = request.getHeader(header);
        builder = builder.header(header, values);
      }

      // get channel for metrics
      String channelType = null;
      Map<String, String[]> params = request.getParameterMap();
      if (params.containsKey(Constants.MKCID) && params.get(Constants.MKCID)[0] != null) {
        channelType = ChannelIdEnum.parse(params.get(Constants.MKCID)[0]).getLogicalChannel().getAvro().toString();
      }

      // construct X-EBAY-C-TRACKING header
      builder = builder.header("X-EBAY-C-TRACKING",
          collectionService.constructTrackingHeader(requestContext, channelType));

      // add uri and referer to marketing event body
      MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
      mktEvent.setTargetUrl(new ServletServerHttpRequest(request).getURI().toString());
      mktEvent.setReferrer(request.getHeader("Referer"));

      // call marketing collection service to send ubi event or send kafka
      Response ress = builder.post(Entity.json(mktEvent));
      ress.close();

      // send 1x1 pixel
      ImageResponseHandler.sendImageResponse(response);
      String partnerId = null;
      if (params.containsKey(Constants.PARTNER_ID)) {
        partnerId = params.get(Constants.PARTNER_ID)[0];
      }
      if (!StringUtils.isEmpty(partnerId) && Constants.ADOBE_PARTNER_ID.equals(partnerId)) {
        sendOpenEventToAdobe(params);
      }

    } catch (Exception e) {
      try {
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
    adserviceCookie.setAdguid(request, response);
    URI redirectUri = null;
    try {
      redirectUri = new URIBuilder(Constants.DEFAULT_REDIRECT_URL).build();
      redirectUri = collectionService.collectRedirect(request, requestContext);
    } catch (Exception e) {
      // When exception happen, redirect to www.ebay.com
      logger.warn(e.getMessage(), e);
    }
    if(redirectUri!=null) {
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

    String adguid = adserviceCookie.setAdguid(request, response);
    Response res = Response.status(Response.Status.OK).build();
    ImageResponseHandler.sendImageResponse(response);

    executorService.submit(() -> {
      try {
        boolean isAddMappingSuccess = idMapping.addMapping(adguid, guid, uid);
        if (isAddMappingSuccess) {
          metrics.meter(METRIC_ADD_MAPPING_SUCCESS);
        } else {
          metrics.meter(METRIC_ADD_MAPPING_FAIL);
        }

      } catch (Exception e) {
        try {
          metrics.meter(METRIC_ADD_MAPPING_FAIL);
        } catch (Exception ex) {
          logger.warn(ex.getMessage(), ex);
        }
      }
    });

    return res;
  }

  /**
   * Get GUID from mapping
   * @return guid in string
   */
  @Override
  public Response guid() {
    String adguid = adserviceCookie.readAdguid(request);
    String guid = idMapping.getGuid(adguid);
    return Response.status(Response.Status.OK).entity(guid).build();
  }

  /**
   * Get user id from mapping
   * @return user id in string
   */
  @Override
  public Response userid() {
    String adguid = adserviceCookie.readAdguid(request);
    String userid = idMapping.getUid(adguid);
    return Response.status(Response.Status.OK).entity(userid).build();
  }

  /**
   * Get user id from mapping
   * @return user id in string
   */
  @Override
  public Response config(String configid) {
    if(null == configid) {
      metrics.meter(METRIC_NO_CONFIGID_IN_EPNT);
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    Response res = null;
    try {
      adserviceCookie.setAdguid(request, response);
      res = Response.status(Response.Status.OK).build();

      Configuration config = ConfigurationBuilder.newConfig("");
      Client mktClient = GingerClientBuilder.newClient(config);
      String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

      // add all headers except Cookie
      Builder builder = mktClient.target(endpoint).path("/impression/").request();
      final Enumeration<String> headers = request.getHeaderNames();
      while (headers.hasMoreElements()) {
        String header = headers.nextElement();
        if ("Cookie".equalsIgnoreCase(header)) {
          continue;
        }
        String values = request.getHeader(header);
        builder = builder.header(header, values);
      }

      // get channel for metrics
      String channelType = null;
      Map<String, String[]> params = request.getParameterMap();
      if (params.containsKey(Constants.MKCID) && params.get(Constants.MKCID)[0] != null) {
        channelType = ChannelIdEnum.parse(params.get(Constants.MKCID)[0]).getLogicalChannel().getAvro().toString();
      }

      // construct X-EBAY-C-TRACKING header
      builder = builder.header("X-EBAY-C-TRACKING",
              collectionService.constructTrackingHeader(requestContext, channelType));

      // add uri and referer to marketing event body
      MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
      mktEvent.setTargetUrl(new ServletServerHttpRequest(request).getURI().toString());
      mktEvent.setReferrer(request.getHeader("Referer"));

      // call marketing collection service to send ubi event or send kafka
      Response ress = builder.post(Entity.json(mktEvent));
      ress.close();

      // send 1x1 pixel
      ImageResponseHandler.sendImageResponse(response);
      String partnerId = null;
      if (params.containsKey(Constants.PARTNER_ID)) {
        partnerId = params.get(Constants.PARTNER_ID)[0];
      }
      if (!StringUtils.isEmpty(partnerId) && Constants.ADOBE_PARTNER_ID.equals(partnerId)) {
        sendOpenEventToAdobe(params);
      }

    } catch (Exception e) {
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
        metrics.meter(METRIC_ERROR_IN_ASYNC_MODEL);
        cf.completeExceptionally(throwable);
      }
    });
    return cf;
  }
}

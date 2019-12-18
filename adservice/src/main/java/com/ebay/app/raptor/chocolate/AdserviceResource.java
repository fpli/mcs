package com.ebay.app.raptor.chocolate;


import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.util.*;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.adservice.CollectionService;
import com.ebay.app.raptor.chocolate.gen.model.SyncEvent;
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

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Map;

/**
 * Resource class
 *
 * @author xiangli4
 */

@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
public class AdserviceResource implements EventsApi {
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
  private CookieReader cookieReader;

  @Autowired
  private AdserviceCookie adserviceCookie;

  @Autowired
  @Qualifier("cb")
  private IdMapable idMapping;

  private Metrics metrics;

  private static final String METRIC_ADD_MAPPING_SUCCESS = "METRIC_ADD_MAPPING_SUCCESS";
  private static final String METRIC_ADD_MAPPING_FAIL = "METRIC_ADD_MAPPING_FAIL";

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  /**
   * Get method to collect impression, viewimp, email open
   *
   * @return response
   */
  @Override
  public Response impression() {
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
      if (params.containsKey(Constants.MKCID) && params.get(Constants.MKCID)[0] != null)
        channelType = ChannelIdEnum.parse(params.get(Constants.MKCID)[0]).getLogicalChannel().getAvro().toString();

      // construct X-EBAY-C-TRACKING header
      builder = builder.header("X-EBAY-C-TRACKING",
          collectionService.constructTrackingHeader(requestContext, cookieReader, channelType));

      // add uri and referer to marketing event body
      MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
      mktEvent.setTargetUrl(new ServletServerHttpRequest(request).getURI().toString());
      mktEvent.setReferrer(request.getHeader("Referer"));

      // call marketing collection service to send ubi event or send kafka
      Response ress = builder.post(Entity.json(mktEvent));
      ress.close();

      // send 1x1 pixel
      ImageResponseHandler.sendImageResponse(response);

    } catch (Exception e) {
      try {
        res = Response.status(Response.Status.BAD_REQUEST).build();
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    } finally {
      return res;
    }
  }

  @Override
  public Response ar() {
    Response res = null;
    try {
      adserviceCookie.setAdguid(request, response);
      collectionService.collectAr(request, response, cookieReader, requestContext);
      res = Response.status(Response.Status.OK).build();
    } catch (Exception e) {
      try {
        res = Response.status(Response.Status.BAD_REQUEST).build();
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    }
    return res;
  }

  @Override
  public Response sync(SyncEvent syncEvent) {
    Response res = null;
    try {
      String adguid = adserviceCookie.setAdguid(request, response);
      boolean isAddMappingSuccess = idMapping.addMapping(adguid, syncEvent.getGuid());
      if (isAddMappingSuccess) {
        metrics.meter(METRIC_ADD_MAPPING_SUCCESS);
        res = Response.status(Response.Status.OK).build();
      } else {
        metrics.meter(METRIC_ADD_MAPPING_FAIL);
        res = Response.status(Response.Status.BAD_REQUEST).build();
      }
    } catch (Exception e) {
      try {
        metrics.meter(METRIC_ADD_MAPPING_FAIL);
        res = Response.status(Response.Status.BAD_REQUEST).build();
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    }
    return res;
  }

  @Override
  public String testGuid() {
    String adguid = adserviceCookie.readAdguid(request);
    String guid = idMapping.getGuid(adguid);
    return guid;
  }

  @Override
  public Response redirect() throws URISyntaxException {
    adserviceCookie.setAdguid(request, response);
    URI redirectUri = new URIBuilder(Constants.DEFAULT_REDIRECT_URL).build();
    try {
      redirectUri = collectionService.collectRedirect(request, requestContext, cookieReader);
    } catch (Exception e) {
      // When exception happen, redirect to www.ebay.com
      logger.warn(e.getMessage(), e);
    }
    return Response.status(Response.Status.MOVED_PERMANENTLY).location(redirectUri).build();
  }

}
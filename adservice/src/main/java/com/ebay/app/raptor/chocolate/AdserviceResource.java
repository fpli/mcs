package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;
import com.ebay.app.raptor.chocolate.adservice.util.ImageResponseHandler;
import com.ebay.app.raptor.chocolate.adservice.util.MarketingTrackingEvent;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.adservice.CollectionService;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContextProvider;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import com.ebay.raptor.cookie.api.RequestCookieData;
import com.ebay.raptor.opentracing.SpanEventHelper;
import com.ebay.raptor.opentracing.Tags;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Enumeration;

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

  /**
   * Get method to collect impression, viewimp, email open
   *
   * @return response
   */
  @Override
  public Response impression() {
    Response res = null;
    try {
      res = Response.status(Response.Status.OK).build();

      Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46");
      Client mktClient = GingerClientBuilder.newClient(config);
      String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

      // add all query parameters
      WebTarget webTarget = mktClient.target(endpoint).path("/impression/");
      Enumeration<String> queryParams = request.getParameterNames();
      while (queryParams.hasMoreElements()) {
        String queryParam = queryParams.nextElement();
        String[] values = request.getParameterValues(queryParam);
        webTarget = webTarget.queryParam(queryParam, values);
      }

      // add all header except Cookie
      Builder builder = webTarget.request();
      final Enumeration<String> headers = request.getHeaderNames();
      while (headers.hasMoreElements()) {
        String header = headers.nextElement();
        if ("Cookie".equalsIgnoreCase(header)) {
          String cguid = cookieReader.getCguid(requestContext).substring(0,32);
          String guid = cookieReader.getGuid(requestContext).substring(0,32);
          builder = builder.header("X-EBAY-C-TRACKING", "guid=" + guid + "," + "cguid=" + cguid);
          continue;
        }
        String values = request.getHeader(header);
        builder = builder.header(header, values);
      }

      // call marketing collection service to send ubi event or send kafka
      Response ress = builder.get();
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

//  @Override
//  public Response ar() {
//    Response res = null;
//    try {
//      collectionService.collectAr(request, response, cookieReader, userCtxProvider.get(), requestContext);
//      res = Response.status(Response.Status.OK).build();
//    } catch (Exception e) {
//      try {
//        res = Response.status(Response.Status.BAD_REQUEST).build();
//      } catch (Exception ex) {
//        logger.warn(ex.getMessage(), ex);
//      }
//    }
//    return res;
//  }
}



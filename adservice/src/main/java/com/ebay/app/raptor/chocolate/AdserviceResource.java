package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;
import com.ebay.app.raptor.chocolate.adservice.util.MarketingTrackingEvent;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.adservice.AdCollectionService;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContextProvider;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import com.ebay.raptor.cookie.api.RequestCookieData;
import com.ebay.raptor.opentracing.SpanEventHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


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
  private AdCollectionService collectionService;

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

  //
  @Inject
  ISecureTokenManager tokenGenerator;

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
      //collectionService.collectImpression(request, response, requestContext);
      res = Response.status(Response.Status.OK).build();

      //marketing event
      MarketingTrackingEvent mktEvent = new MarketingTrackingEvent();
      mktEvent.setTargetUrl("http://www.ebay.com?mkevt=1&mkcid=2&mkrid=222");
      //token
      String token = tokenGenerator.getToken().getAccessToken();
      //cookie
      String cguid = cookieReader.getCguid(requestContext).substring(0,31);
      String guid = cookieReader.getGuid(requestContext).substring(0,31);

      Configuration config = ConfigurationBuilder.newConfig("mktCollectionSvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46");
      Client mktClient = GingerClientBuilder.newClient(config);
      String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

      Response ress = mktClient.target(endpoint).path("/events/").request()
          .header("Authorization", token)
          .header("X-EBAY-C-ENDUSERCTX", "userAgent=ebayUserAgent/eBayIOS;5.28.0;iOS;12.1.2;Apple;iPhone11_2;AT&T;375x812;3.0")
          .header("X-EBAY-C-TRACKING", "guid=" + guid + "," + "cguid=" + cguid)
          .post(Entity.json(mktEvent));
      ress.close();

    } catch (Exception e) {
      // logger.warn(e.getMessage(), e);
      // Tags.STATUS.set(span, e.getMessage());
      // show warning in cal
      SpanEventHelper.writeEvent("Warning", "mktcollectionsvc", "1", e.getMessage());
      try {
        res = Response.status(Response.Status.BAD_REQUEST).build();
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    } finally {
      return res;
    }
  }
}



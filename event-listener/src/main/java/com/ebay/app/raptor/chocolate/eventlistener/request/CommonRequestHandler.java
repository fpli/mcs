/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.request;

import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.traffic.monitoring.ESMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;

/**
 * @author xiangli4
 * Handle common request covering tracking header, end user context etc.
 */
@Component
@DependsOn("EventListenerService")
public class CommonRequestHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommonRequestHandler.class);

  public Map<String, String> getHeaderMaps(HttpServletRequest clientRequest) {
    Map<String, String> headers = new HashMap<>();
    if(clientRequest.getHeaderNames() != null) {
      for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements(); ) {
        String headerName = e.nextElement();
        // skip auth header
        if (headerName.equalsIgnoreCase(AUTH_HEADER)) {
          continue;
        }
        headers.put(headerName, clientRequest.getHeader(headerName));
      }
    }
    return headers;
  }

  /**
   * referer is from post body (mobile) and from header (NodeJs and handler)
   * By internet standard, referer is typo of referrer.
   * From ginger client call, the referer is embedded in enduserctx header, but we also check header for other cases.
   * For local test using postman, do not include enduserctx header, the service will generate enduserctx by
   * cos-user-context-filter.
   * Ginger client call will pass enduserctx in its header.
   * Priority 1. native app from body, as they are the most part 2. enduserctx, ginger client calls 3. referer header
   * 4. For ROI, there is an extra place to get referer, the payload
   * @param event Event body
   * @param requestHeaders request headers
   * @param endUserContext enduserctx header
   * @return referer
   */
  public String getReferer(Event event, Map<String, String> requestHeaders, IEndUserContext endUserContext) {
    String referer = "";
    if (!StringUtils.isEmpty(event.getReferrer())) {
      referer = event.getReferrer();
    }

    if (StringUtils.isEmpty(referer)) {
      referer = endUserContext.getReferer();
    }

    if(StringUtils.isEmpty(referer) && requestHeaders.get(Constants.REFERER_HEADER) != null) {
      referer = requestHeaders.get(Constants.REFERER_HEADER);
    }

    if(StringUtils.isEmpty(referer) && requestHeaders.get(Constants.REFERER_HEADER_UPCASE) != null) {
      referer = requestHeaders.get(Constants.REFERER_HEADER_UPCASE);
    }

    // return 201 for now for the no referer case. Need investigation further.
    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase(STR_NULL) ) {
      LOGGER.warn(Errors.ERROR_NO_REFERER);
      ESMetrics.getInstance().meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }

    // decode referer if necessary. Currently, android is sending rover url encoded.
    if (referer.startsWith(HTTPS_ENCODED) || referer.startsWith(HTTP_ENCODED)) {
      try {
        referer = URLDecoder.decode(referer, StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        LOGGER.warn(e.getMessage());
        LOGGER.warn("Decode referer with utf8 failed");
      }
    }
    return referer;
  }

  /**
   * get user id from auth token if it's user token, else we get from end user ctx
   * @param raptorSecureContext raptor secure context to parse auth token
   * @param endUserContext enduserctx header
   * @return user id
   */
  public String getUserId(RaptorSecureContext raptorSecureContext, IEndUserContext endUserContext) {
    String userId;
    if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
      userId = raptorSecureContext.getSubjectImmutableId();
    } else {
      userId = Long.toString(endUserContext.getOrigUserOracleId());
    }
    return userId;
  }
}

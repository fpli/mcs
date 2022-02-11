package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.constant.ClientDataEnum;
import com.ebay.kernel.util.HeaderMultiValue;
import com.ebay.platform.raptor.cosadaptor.context.impl.COSHeaderMultiValue;
import com.ebay.raptor.kernel.util.RaptorConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Priority;
import javax.inject.Named;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

@Priority(3099)  //need to after 3050 to overrid framework default
@Provider
@Component
@Named("client-hint-usercontext-filter")
public class ClientHintUserContextFilter implements ClientRequestFilter, ClientResponseFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientHintUserContextFilter.class);

  @Override
  public void filter(ClientRequestContext requestContext) {
    MultivaluedMap<String, Object> headers = requestContext.getHeaders();
    String header = requestContext.getHeaderString(RaptorConstants.X_EBAY_C_ENDUSERCTX);
    try {
      if (header != null && MapUtils.isNotEmpty(headers)) {
        HeaderMultiValue userCtxHeader = new HeaderMultiValue(header, RaptorConstants.UTF_8);
        COSHeaderMultiValue.COSHeaderMultiValueBuilder cosBuilder = new COSHeaderMultiValue.COSHeaderMultiValueBuilder();
        // copy all fields from existing header
        userCtxHeader.nameSet().stream()
                .forEach(s -> cosBuilder.put(s, userCtxHeader.get(s)));
        //modify the header through builder
        for (ClientDataEnum clientHint : ClientDataEnum.getClientHint()) {
          String clientHintName = clientHint.getHeaderName();
          if (StringUtils.isNotEmpty((String) cosBuilder.get(clientHintName))) {
            continue;
          }
          if (CollectionUtils.isEmpty(headers.get(clientHintName))) {
            continue;
          }
          String clientHintVal = String.valueOf(headers.get(clientHintName).get(0));
          if (StringUtils.isNotEmpty(clientHintVal)) {
            cosBuilder.put(clientHintName, clientHintVal);
          }
        }
        headers.putSingle(RaptorConstants.X_EBAY_C_ENDUSERCTX, cosBuilder.build().toHeader(RaptorConstants.UTF_8));
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to customize usercontext header", ex);
    }
  }

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) throws IOException {
    responseContext.getHeaders().putSingle(RaptorConstants.X_EBAY_C_ENDUSERCTX, requestContext.getHeaderString(RaptorConstants.X_EBAY_C_ENDUSERCTX));
  }
}

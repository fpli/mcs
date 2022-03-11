package com.ebay.app.raptor.chocolate.eventlistener.component;

import com.ebay.app.raptor.chocolate.eventlistener.model.AdsCollectionSvcRequest;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.com.google.common.base.Joiner;
import com.ebay.com.google.common.base.Splitter;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class AdsCollectionSvcClient {

    @Inject
    RaptorSecureContextProvider raptorSecureContextProvider;
    private static final String ADS_CLICK_URL = "/ads_tracking_collection/v1/ads_click";
    private static final String SERVICE_ID = "adsTrkColsvc.adsTrkRaptorIoHandlerClient";
    private static WebTarget WEB_TARGET = null;
    public static final String ORIG_USER_ID = "origUserId";
    public static final String ORIG_ACCT_ID = "origAcctId%3D";
    public static final String REFERER = "referer";
    public static final String X_EBAY_C_ENDUSERCTX = "X-EBAY-C-ENDUSERCTX";
    public static final String ADS_SVC = "adsSvc";
    private static final Logger logger = LoggerFactory.getLogger(AdsCollectionSvcClient.class);

    public AdsCollectionSvcClient() {
        init();
    }

    public void invokeService(AdsCollectionSvcRequest request, String referrer,
                              MultivaluedMap<String, String> requestHeaders) {
        if (WEB_TARGET != null) {
            MonitorUtil.info(ADS_SVC);
            String endUserContextValue = processEndUserContext(referrer, requestHeaders);
            MultivaluedMap<String, Object> adsColSvcHdrs = generateSvcHeader(requestHeaders, endUserContextValue);
            final Invocation.Builder builder = WEB_TARGET.path(ADS_CLICK_URL).request(MediaType.APPLICATION_JSON);
            builder.headers(adsColSvcHdrs)
                    .async().post(Entity.entity(request, MediaType.APPLICATION_JSON));
        }
    }

    protected MultivaluedMap<String,Object> generateSvcHeader(MultivaluedMap<String, String> inputMap,
                                                              String endUserContextValue){
        MultivaluedMap<String, Object> multivaluedMap = new MultivaluedHashMap<>();
        if (!CollectionUtils.isEmpty(inputMap)) {
            for(Map.Entry<String, List<String>> entry : inputMap.entrySet()){
                if (!StringUtils.equalsIgnoreCase(entry.getKey(), HttpHeaders.AUTHORIZATION) &&
                        !CollectionUtils.isEmpty(entry.getValue())) {
                    multivaluedMap.put(entry.getKey(), convertStr(entry.getValue()));
                }
            }
        }
        if (StringUtils.isNotBlank(endUserContextValue)) {
            multivaluedMap.putSingle(X_EBAY_C_ENDUSERCTX, endUserContextValue);
        }
        return multivaluedMap;
    }

    private List<Object> convertStr(List<String> strings){
        return strings.stream().filter(value-> !value.isEmpty()).collect(Collectors.toList());
    }

    protected  String processEndUserContext(String referrer, MultivaluedMap<String, String> headers) {
        try {
            String userId = getOracleUserIdFromSecureContext();
            return addAdditionalValuesToEndUserCtxHeader(headers, userId, referrer);
        } catch(Exception e) {
            logger.warn(e.getMessage(), e);
        }
        return null;
    }

    protected String addAdditionalValuesToEndUserCtxHeader(MultivaluedMap<String, String> headers, String userId, String referer) {
        String endUserContextValue = null;
        if (headers != null && !headers.isEmpty() && (StringUtils.isNotBlank(userId) || StringUtils.isNotBlank(referer))) {
            List<String> xEbayCEndUserContextHeader = null;
            for(Map.Entry<String, List<String>> entry: headers.entrySet()){
                if (StringUtils.isNotEmpty(entry.getKey()) && X_EBAY_C_ENDUSERCTX.equalsIgnoreCase(entry.getKey().trim())){
                    xEbayCEndUserContextHeader = entry.getValue();
                    break;
                }
            }

            if (!CollectionUtils.isEmpty(xEbayCEndUserContextHeader)){
                endUserContextValue = xEbayCEndUserContextHeader.get(0);
            }

            if (StringUtils.isNotBlank(userId)) {
                endUserContextValue = buildEndUserContextHeader(endUserContextValue, ORIG_USER_ID, ORIG_ACCT_ID+userId);
            }

            if (StringUtils.isNotBlank(referer)) {
                try {
                    String urlEncodedReferer = URLEncoder.encode(referer, StandardCharsets.UTF_8.toString());
                    endUserContextValue = buildEndUserContextHeader(endUserContextValue, REFERER, urlEncodedReferer);
                } catch (Exception e) {
                    logger.warn("Url encodding failed", e);
                }
            }
        }
        return endUserContextValue;
    }

    private String buildEndUserContextHeader(final String headerValue, String headerName, String value) {
        //check the existing header for user and augment or replace
        if (StringUtils.isNotBlank(headerValue)) {
            if (headerValue.contains(headerName)) {
                List<String> values = new ArrayList<>();
                for (String keyValueString : Splitter.on(",").split(headerValue)) {
                    if (!keyValueString.contains(headerName)) {
                        values.add(keyValueString);
                    }
                }
                values.add(String.format("%s=%s", headerName, value));
                return Joiner.on(",").join(values);
            } else {
                //The header does not contain the key so we can just append it
                return String.format("%s,%s=%s", headerValue, headerName, value);
            }
        }
        //the header does not exist so we're free to return just the userId.
        return String.format("%s=%s", headerName, value);
    }

    private String getOracleUserIdFromSecureContext() {
        if (raptorSecureContextProvider == null || raptorSecureContextProvider.get() == null) {
            return null;
        }
        return raptorSecureContextProvider.get().getSubjectImmutableId();
    }

    private void init() {
        try {
            final Configuration config = ConfigurationBuilder.newConfig(SERVICE_ID);
            Client client = GingerClientBuilder.newClient(config);
            final String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
            WEB_TARGET = client.target(endpoint);
        } catch (Throwable e){
            logger.warn(e.getMessage(), e);
        }
    }
}

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.eventlistener.component.AdsCollectionSvcClient;
import com.ebay.app.raptor.chocolate.eventlistener.model.AdsCollectionSvcRequest;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import javax.ws.rs.core.MultivaluedMap;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@Component
@DependsOn("EventListenerService")
public class AdsClickCollector {

    @Autowired
    protected AdsCollectionSvcClient adsCollectionSvcClient;
    private static final String EBAY_USER_AGENT = "ebayuseragent";
    private static final String ANDROID = "ebayandroid";
    private static final String IPHONE = "ebayiphone";
    private static final String AMDATA = "amdata";
    private static final String CLICK_TIME = "|tsp:";
    private static final Logger LOGGER = LoggerFactory.getLogger(AdsClickCollector.class);

    public void processPromotedListingClick(IEndUserContext endUserContext, Event event,
                                            MultivaluedMap<String, String> requestHeaders) {
        try {
            String amdata = getAmdata(event);
            if (isInvokeAdsSvc(endUserContext, amdata)) {
                AdsCollectionSvcRequest adsCollectionSvcRequest = createAdsRequest(event, amdata);
                adsCollectionSvcClient.invokeService(adsCollectionSvcRequest, event.getReferrer(), requestHeaders);
            }
        } catch(Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    protected AdsCollectionSvcRequest createAdsRequest(Event event, String amData) {
        AdsCollectionSvcRequest request = new AdsCollectionSvcRequest();
        StringBuilder amDataSB = new StringBuilder();
        StringBuilder clickTime = new StringBuilder();
        clickTime.append(CLICK_TIME).append(System.currentTimeMillis());
        try {
            amDataSB.append(amData).append(URLEncoder.encode(clickTime.toString(), StandardCharsets.UTF_8.toString()));
            request.setAmdata(amDataSB.toString());
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Error while encoding amdata", e);
        }
        request.setReferrer(event.getReferrer());
        request.setRequestUrl(event.getTargetUrl());
        return request;
    }

    protected String getAmdata(Event body) {
        if (body == null) {
            return null;
        }
        MultiValueMap<String, String> parameters =
                UriComponentsBuilder.fromUriString(body.getTargetUrl()).build().getQueryParams();
        String value = null;
        if (!CollectionUtils.isEmpty(parameters) && !CollectionUtils.isEmpty(parameters.get(AMDATA))) {
            value = parameters.get(AMDATA).get(0);
        }
        return value;
    }

    protected boolean isInvokeAdsSvc(IEndUserContext endUserContext, String amdata) {
        if (plClickPayloadMissing(amdata) || !isNative(endUserContext)) {
            return false;
        }
        return true;
    }

    private boolean plClickPayloadMissing(String amdata) {
        return StringUtils.isBlank(amdata);
    }

    private boolean isNative(IEndUserContext endUserContext){
        if (endUserContext != null && StringUtils.isNotBlank(endUserContext.getUserAgent())){
            String userAgentNormalized = endUserContext.getUserAgent().trim().toLowerCase();
            if (userAgentNormalized.startsWith(EBAY_USER_AGENT) || userAgentNormalized.contains(ANDROID)
                    || userAgentNormalized.contains(IPHONE)){
                return true;
            }
        }
        return false;
    }
}

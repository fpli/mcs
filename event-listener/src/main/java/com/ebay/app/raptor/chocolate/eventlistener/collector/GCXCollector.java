package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageIdEnum;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.container.ContainerRequestContext;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.deeplinksites;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ebaySitesIncludeULK;

@Component
@DependsOn("EventListenerService")
public class GCXCollector {

    private void addTagFromUrlQuery(MultiValueMap<String, String> parameters, IRequestScopeTracker requestTracker,
                                    String urlParam, String tag, Class tagType) {
        if (parameters.containsKey(urlParam) && parameters.get(urlParam).get(0) != null) {
            requestTracker.addTag(tag, parameters.get(urlParam).get(0), tagType);
        }
    }

    public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent) {
        // send click event to ubi
        // Third party clicks should not be tracked into ubi
        if (ChannelActionEnum.CLICK.equals(baseEvent.getActionType()) && (ebaySitesIncludeULK.matcher(baseEvent.getUrl().toLowerCase()).find()
                || deeplinksites.matcher(baseEvent.getUrl().toLowerCase()).find())) {
            MultiValueMap<String, String> parameters = baseEvent.getUrlParameters();
            IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);
            // Ubi tracking
            requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.CLICK.getId(), Integer.class);
            // channel id
            addTagFromUrlQuery(parameters, requestTracker, MKCID, TAG_CHANNEL, String.class);
            //bu
            addTagFromUrlQuery(parameters, requestTracker, BEST_GUESS_USER, TAG_BU, String.class);
            //trkId
            addTagFromUrlQuery(parameters, requestTracker, TAG_TRACK_ID, TAG_TRACK_ID, String.class);
            // event family
            requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, EVENT_FAMILY_CRM, String.class);
            // event action
            requestTracker.addTag(TrackerTagValueUtil.EventActionTag, EVENT_ACTION, String.class);
            // target url
            if (StringUtils.isNotBlank(baseEvent.getUrl())) {
                requestTracker.addTag(SOJ_MPRE_TAG, baseEvent.getUrl(), String.class);
            }
            // referer
            if (StringUtils.isNotBlank(baseEvent.getReferer())) {
                requestTracker.addTag(TAG_REF, baseEvent.getReferer(), String.class);
            }
            // utp event id
            if (StringUtils.isNotBlank(baseEvent.getUuid())) {
                requestTracker.addTag(TAG_UTP_ID, baseEvent.getUuid(), String.class);
            }
            //UFES Tag
            String ufesEdgTrkSvcHeader = baseEvent.getRequestHeaders().get(UFES_EDGTRKSVC_HDR);
            if (StringUtils.isNotBlank(ufesEdgTrkSvcHeader)) {
                requestTracker.addTag(UFES_EDGTRKSVC_HDR, ufesEdgTrkSvcHeader, String.class);
            }
            // add isUFESRedirect if the click traffic is converted from Rover to Chocolate by UFES
            if (parameters.containsKey(UFES_REDIRECT)
                    && Boolean.TRUE.toString().equalsIgnoreCase(parameters.getFirst(UFES_REDIRECT))) {
                requestTracker.addTag(TAG_IS_UFES_REDIRECT, true, Boolean.class);
            }
            // is from ufes
            requestTracker.addTag(TAG_IS_UFES,
                    StringUtils.isNotBlank(baseEvent.getRequestHeaders().get(IS_FROM_UFES_HEADER)), Boolean.class);
            // status code
            String statusCode = baseEvent.getRequestHeaders().get(NODE_REDIRECTION_HEADER_NAME);
            if (StringUtils.isNotBlank(statusCode)) {
                requestTracker.addTag(TAG_STATUS_CODE, statusCode, String.class);
            }
            // populate device info
            CollectionServiceUtil.populateDeviceDetectionParams(baseEvent.getUserAgentInfo(), requestTracker);
        }
    }
}

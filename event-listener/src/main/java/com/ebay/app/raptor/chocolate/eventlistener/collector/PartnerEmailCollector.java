package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.tracking.api.IRequestScopeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import javax.ws.rs.container.ContainerRequestContext;

import static com.ebay.app.raptor.chocolate.constant.Constants.partnerEmailParamTags;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.deeplinksites;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.ebaySitesIncludeULK;

@Component
@DependsOn("EventListenerService")
public class PartnerEmailCollector extends CustomerMarketingCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartnerEmailCollector.class);

    @PostConstruct
    @Override
    public void postInit() {
        super.postInit();
    }

    /**
     * Track ubi
     *
     * @param requestContext request context
     * @param baseEvent      base event
     */
    @Override
    public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent) {
        // send click event to ubi
        // Third party clicks should not be tracked into ubi
        if (ChannelActionEnum.CLICK.equals(baseEvent.getActionType())
                && (ebaySitesIncludeULK.matcher(baseEvent.getUrl().toLowerCase()).find() || deeplinksites.matcher(baseEvent.getUrl().toLowerCase()).find())
                && !baseEvent.isThirdParty()) {
            // Ubi tracking
            IRequestScopeTracker requestTracker =
                    (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);
            // common tags and soj tags
            super.trackUbi(requestContext, baseEvent);

            addUbiTag(partnerEmailParamTags, baseEvent, requestTracker);
        }
    }
}


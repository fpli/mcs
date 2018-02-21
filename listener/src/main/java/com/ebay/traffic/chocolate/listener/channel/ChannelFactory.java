package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.init.KafkaProducers;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;

/**
 * A simple factory method to assign different channel for different incoming
 * servletRequest based on pattern match
 */

public class ChannelFactory {
    private static final Logger logger = Logger.getLogger(ChannelFactory.class);
    
    
    /** @return a query message derived from the given string. */
    static StringBuffer deriveWarningMessage(StringBuffer sb,
            HttpServletRequest servletRequest) {
        sb.append(" URL=").append(servletRequest.getRequestURL().toString())
          .append(" queryStr=").append(servletRequest.getQueryString());
        return sb;
    }
    
    /**
     * Assign the action to specific channel handler based on input parameters.
     * For Rover format URI only. Parse parameters from URI using @Path(
     * "/{action}/{trackingProviderId}/{rotationString}/{channel:[0-9]*}{subResources:.*}"
     * )
     * 
     * @param channelId a number identifying the channel
     * @param action the Rover action being tracked
     * @param trackingProviderId unsure if this every changes from '1'
     * @param rotationString A way to group actions
     * @param servletRequest The actual HTTP request
     * @return Channel
     */
    public static Channel getChannel(String channelId, String action,
            String trackingProviderId, String rotationString,
            HttpServletRequest servletRequest) {
        
        ChannelIdEnum channel = ChannelIdEnum.parse(channelId);
        
        // Un-parseable channel - return tracking action
        if (channel == null) {
            StringBuffer sb = new StringBuffer();
            sb.append("No pattern matched;");
            sb = deriveWarningMessage(sb, servletRequest);
            logger.warn(sb.toString());
            return new DefaultChannel(MetricsClient.getInstance());
        }
        
        // Invalid action - return tracking action
        ChannelActionEnum channelAction = ChannelActionEnum.parse(channel, action);
        if (!channel.getLogicalChannel().isValidRoverAction(channelAction)) {
            // For rest of the channels and actions, return tracking channel
            StringBuffer sb = new StringBuffer();
            sb.append("Invalid tracking action given a channel;");
            sb = deriveWarningMessage(sb, servletRequest);
            logger.warn(sb.toString());
            return new DefaultChannel(MetricsClient.getInstance());
        }
        
        // If we're in mock context, then log the message we're getting
        if (channel.isTestChannel()) {
            StringBuffer sb = new StringBuffer();
            sb.append("Received test URL;");
            sb = deriveWarningMessage(sb, servletRequest);
            KafkaProducers.getInstance().getChannelProducerMap().put(ChannelIdEnum.NINE,
                    KafkaProducers.getInstance().getKafkaProducer(ChannelIdEnum.EPN));
            logger.info(sb.toString());
        }
        
        switch (channel.getLogicalChannel()) {
        case EPN:
            return new EpnChannel(MessageObjectParser.getInstance(), MetricsClient.getInstance(),
                    KafkaProducers.getInstance().getKafkaProducer(channel), channelAction, channel.getLogicalChannel());
        case DISPLAY:
            return new DisplayChannel(MessageObjectParser.getInstance(),MetricsClient.getInstance(),
                    KafkaProducers.getInstance().getKafkaProducer(channel), channelAction, channel.getLogicalChannel());
        default:
            return defaultChannelResult(servletRequest);
        }
    }

    public static Channel getChannel(HttpServletRequest request) {
        String[] result = request.getRequestURI().split("/");
        //Validate.isTrue(result.length == 5); // 0th entry will be blank
        if(result.length == 5){
            return getChannel(result[4], result[1], result[2], result[3], request);
        }
        else return defaultChannelResult(request);
    }

    private static DefaultChannel defaultChannelResult(HttpServletRequest request) {
        StringBuffer sb = new StringBuffer();
        sb.append("Received URL we were not supposed to get; returning default handler");
        sb = deriveWarningMessage(sb, request);
        logger.error(sb.toString());
        return new DefaultChannel(MetricsClient.getInstance());
    }

}

package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.*;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.utp.common.model.Message;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Receive performance marketing events from utp topic, aggregate metrics of different kinds of messages to sherlock
 *
 * @author yuhxiao
 * @since 2021/11/10
 */
public class UtpMonitor extends RichMapFunction<UnifiedTrackingRheosMessage, UnifiedTrackingRheosMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UtpMonitor.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    private static Pattern pattern = Pattern.compile("(m|www)\\.(befr|benl|cafr|ebay)\\.((com|ca|de|at|ch|es|fr|ebay|in|it|pl|ph|nl|co|ie)|(com|ebay|co)\\.(au|be|ca|hk|sg|my|uk))$", Pattern.CASE_INSENSITIVE);
    private static List<String> topPageList = Arrays.asList("i","itm","sch","b","e","vod","ulk","ws","p","cnt","sl","signin","fdbk","rtn");

    private SherlockioMetrics sherlockioMetrics;
    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = PropertyMgr.getInstance()
                .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
        SherlockioMetrics.init(properties.getProperty(PropertyConstants.MONITOR_SHERLOCKIO_NAMESPACE),
                properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT), properties.getProperty(PropertyConstants.SHERLOCKIO_USER));
        sherlockioMetrics = SherlockioMetrics.getInstance();
    }

    @Override
    public UnifiedTrackingRheosMessage map(UnifiedTrackingRheosMessage message) {
        String channelType = nullVerifier(message.getChannelType());
        String actionType = nullVerifier(message.getActionType());
        String producer = nullVerifier(message.getService());
        String isBot = nullVerifier(String.valueOf(message.getIsBot()));
        String isUep = nullVerifier(getUEP(message.getPayload()));
        String platform = nullVerifier(getPlatform(message));
        String site = nullVerifier(String.valueOf(message.getSiteId()));
        String uxe = nullVerifier(getUxe(message.getPayload()));
        String uxt = nullVerifier(getUxt(message.getPayload()));
        try {
            sherlockioMetrics.meterByGauge("unified_tracking_incoming_total", 1,
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer),
                    Field.of("isBot", isBot),
                    Field.of("isUEP", isUep),
                    Field.of("platform", platform),
                    Field.of("site", site)
            );

            sherlockioMetrics.meterByGauge("unified_tracking_latency_total",
                    message.getEventTs() - message.getProducerEventTs(),
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer)
            );

            String url = nullVerifier(message.getUrl());
            String mkcid = getDuplicateValue(url, "mkcid");
            String mkrid = getDuplicateValue(url, "mkrid");
            String mkpid = getDuplicateValue(url, "mkpid");
            String mksid = getDuplicateValue(url, "mksid");
            sherlockioMetrics.meterByGauge("unified_tracking_duplicate_incoming_total", 1,
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer),
                    Field.of("isBot", isBot),
                    Field.of("mkcid", mkcid),
                    Field.of("mkrid", mkrid),
                    Field.of("mkpid", mkpid),
                    Field.of("mksid", mksid)
            );

            if ("true".equals(isUep.toLowerCase())) {
                try {
                    List<String> messageId = getMessageId(message.getPayload());
                    String cnvId = nullVerifier(getCnvId(message.getPayload()));
                    for (int i = 0; i < messageId.size(); i++) {
                        sherlockioMetrics.meterByGauge("unified_tracking_payload_total", 1,
                                Field.of("channel", channelType),
                                Field.of("action", actionType),
                                Field.of("producer", producer),
                                Field.of("isBot", isBot),
                                Field.of("isUEP", isUep),
                                Field.of("platform", platform),
                                Field.of("messageId", messageId.get(i)),
                                Field.of("cnvId", cnvId),
                                Field.of("site", site),
                                Field.of("is1stMId", i == 0 ? "true" : "false"),
                                Field.of("uxe", uxe),
                                Field.of("uxt", uxt)
                        );
                    }
                } catch (Exception e) {
                    sherlockioMetrics.meterByGauge("unified_tracking_payload_parsing_error_total", 1,
                            Field.of("channel", channelType),
                            Field.of("action", actionType),
                            Field.of("producer", producer)
                    );
                }
            }

            //UFES metrics
            String isNative = "false";
            if(message.getUserAgent() != null){
                if(message.getUserAgent().toLowerCase().contains("ebayandroid") ||
                        message.getUserAgent().toLowerCase().contains("ebayios")) {
                    isNative = "true";
                }
            }
            if(producer.equalsIgnoreCase("chocolate") &&
                    isNative.equalsIgnoreCase("false") &&
                    actionType.equalsIgnoreCase("click")){
                String isUFES = nullVerifier(getUFESSignal(message.getPayload()));
                //domain
                String domain = getDomainFromUrl(message.getUrl());
                //pagetype: /i/, /itm/, /sch/,/b/，/vod/,/ulk/messages/,/ulk/usr/,/ws/,/p/, home page
                String pageType = getPageType(message.getUrl());

                sherlockioMetrics.meterByGauge("chocolate_web_incoming_traffic", 1,
                        Field.of("isUFES", isUFES),
                        Field.of("domain",domain),
                        Field.of("pagetype", pageType),
                        Field.of("isBot", isBot));
            }


        } catch (Exception e) {
            sherlockioMetrics.meterByGauge("unified_tracking_metrics_error_total", 1,
                    Field.of("channel", channelType),
                    Field.of("action", actionType),
                    Field.of("producer", producer)
            );
            LOGGER.error("error fields of message " + message.getUrl() + " and error is" + e.toString());
        }
        return message;
    }

    @Override
    public void close() {
        SherlockioMetrics.getInstance().close();
    }

    private static List<String> getMessageId(Map<String, String> payload) throws Exception {
        List<String> nullMessageList = new ArrayList<>();
        nullMessageList.add("NULL");
        String messageListString = payload.getOrDefault("annotation.mesg.list", "[]");
        List<Message> messageList = mapper.readValue(messageListString, new TypeReference<List<Message>>() {
        });
        if (messageList.size() == 0) {
            return nullMessageList;
        }
        return messageList
                .stream()
                .map(e -> {
                    if (e.mesgId == null) {
                        return "NULL";
                    }
                    return e.mesgId;
                })
                .collect(Collectors.toList());
    }

    /**
     * Check the payload, see if there is a exist is getCnvId
     *
     * @param payload the payload to be read
     * @return the corresponding value if so, otherwise return "NULL"
     */
    private static String getCnvId(Map<String, String> payload) {
        return payload.getOrDefault("annotation.cnv.id", "NULL");
    }

    /**
     * Check if the input is null
     *
     * @param dimension the dimension that needs to be checked
     * @return return itself if it's not null, otherwise return "NULL"
     */
    private static String nullVerifier(String dimension) {
        if (dimension != null && StringUtils.isNotEmpty(dimension.trim())) {
            return dimension;
        }
        return "NULL";
    }

    /**
     * Check the payload, see if there is a exist is UEP key
     *
     * @param payload the payload to be read
     * @return the corresponding value if so, otherwise return "NULL"
     */
    private static String getUEP(Map<String, String> payload) {
        return payload.getOrDefault("isUEP", "NULL");
    }

    /**
     * Check the payload to see if it is handled by UFES
     *
     * @param payload the payload to be read
     * @return the corresponding value if so, otherwise return "NULL"
     */
    public static String getUFESSignal(Map<String, String> payload) {
        return payload.getOrDefault("isUfes", "NULL");
    }

    /**
     * get domain from url
     *
     * @param url is the url to be read
     * @return the corresponding value if so, otherwise return "NULL"
     */
    public static String getDomainFromUrl(final String url) {
        if (url == null || url.length() == 0) {
            return null;
        }

        // Parse the url link into a URL
        URL landingPage;
        try {
            landingPage = new URL(url);
        } catch (Exception e) {
            LOGGER.warn("Error in parsing incoming link into url: ", e);
            return null;
        }

        // Strip off domain and validate that it is not empty or null
        return landingPage.getHost().toLowerCase().trim();
    }

    /**
     * get pagetype from url
     *
     * @param url is the url to be read
     * @return the corresponding value if so, otherwise return "NULL"
     */
    public static String getPageType(final String url) {

        if (url == null || url.length() == 0) {
            return null;
        }

        URL landingPage;
        String pageType = null;
        try {
            landingPage = new URL(url);
            String path = landingPage.getPath();
            if(path == null || path.length() == 0 || path.equalsIgnoreCase("/")){
                String domain = getDomainFromUrl(url);
                Matcher matcher = pattern.matcher(domain);
                boolean matchFound = matcher.find();
                if(matchFound) {
                    pageType = "homepage";
                } else {
                    pageType = "others";
                }
            } else{
                pageType = path.split("/")[1];
                if(!topPageList.contains(pageType)){
                    pageType = "others";
                }
            }
            return pageType;
        } catch (Exception e) {
            LOGGER.warn("Error in parsing incoming link into url: ", e);
            LOGGER.warn("Error url: ", url);
            return null;
        }
    }

    /**
     * for test domain pattern
     *
     * @param domain the domain string to be tested
     * @return if the domain is ebay hoempage domain
     */
    public static boolean isHomepageDomain(String domain){
        Matcher matcher = pattern.matcher(domain);
        boolean matchFound = matcher.find();
        return matchFound?true:false;
    }


    /**
     * Check the message, see what platform generated the message
     *
     * @param message the message to be read
     * @return the String platform
     */
    private static String getPlatform(UnifiedTrackingRheosMessage message) {
        String appId = message.getAppId();
        String userAgent = message.getUserAgent();
        if (appId == null) {
            if (userAgent != null) {
                return "DESKTOP";
            } else {
                return "NULL";
            }
        } else {
            switch (appId) {
                case "3564":
                    return "MOBILE_PHONE_WEB";
                case "1115":
                    return "MOBILE_TABLET_WEB";
                case "1462":
                    return "IPHONE";
                case "2878":
                    return "IPAD";
                case "2571":
                    return "ANDROID";
                default:
                    return "NULL";
            }
        }
    }

    public static String getDuplicateValue(String url, String duplicateItemName) {
        String parameterPattern = "\\d+\\-\\d+\\-\\d+\\-\\d+|\\d+";
        try {
            String decodeUrl = url;
            for (int i = 0; i < 3; i++) {
                decodeUrl = URLDecoder.decode(decodeUrl, "UTF-8");
            }
            UriComponents uriComponents = UriComponentsBuilder.fromUriString(decodeUrl).build();
            MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
            if (parameters.containsKey(duplicateItemName)) {
                List<String> items = parameters
                        .get(duplicateItemName)
                        .stream()
                        .map(String::trim)
                        .distinct()
                        .map(e -> {
                            if (!e.matches(parameterPattern)) {
                                LOGGER.info("wrong url format " + url);
                                return "ERROR";
                            }
                            if (e.length() == 0) {
                                return "EMPTY";
                            }
                            if (e.contains(";")) {
                                e = e.replaceAll(";", "");
                            }
                            if (e.contains("|")) {
                                e = e.replaceAll("\\|", "");
                            }
                            if (e.contains("=")) {
                                e = e.replaceAll("=", "");
                            }
                            return e;
                        })
                        .sorted(StringUtils::compare)
                        .collect(Collectors.toList());

                boolean duplicateOrNonExist = (items.size() > 1) || (items.size() == 1 && "EMPTY".equals(items.get(0)));
                if (duplicateOrNonExist) {
                    return StringUtils.join(items, '+');
                } else {
                    return "DEFAULT";
                }
            } else {
                return "NULL";
            }
        } catch (Exception e) {
            return "EXCEPTION";
        }
    }
    
    public static String getUxe(Map<String, String> payload) {
        return payload.getOrDefault("!uxe", "NULL");
    }
    
    public static String getUxt(Map<String, String> payload) {
        return payload.getOrDefault("!uxt", "NULL");
    }
    
}

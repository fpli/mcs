package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.kernel.util.DomainIpChecker;
import com.ebay.kernel.util.RequestUtil;
import com.ebay.kernel.util.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing the POJOs
 * 
 * @author kanliu
 */
public class MessageObjectParser {

    /** Logging instance */
    private static final Logger logger = Logger.getLogger(MessageObjectParser.class);
    private static MessageObjectParser INSTANCE;
    private static final long DEFAULT_PUBLISHER_ID = -1L;
    public static final String REDIRECT_HEADER = "Location";
    /* singleton class */
    private MessageObjectParser() {
    }
    
    /**
     * Convert a HTTP request to a listener message for Kafka.
     * 
     * @param clientRequest
     *            to use in parsing uri and timestamp
     * @param proxyResponse
     *            to use in parsing response headers
     * @param startTime
     *            as start time of the request
     * @return ListenerMessage  as the parse result.
     */
    public ListenerMessage parseHeader(
        final HttpServletRequest clientRequest,
        final HttpServletResponse proxyResponse, Long startTime, Long campaignId,
        final ChannelType channelType, final ChannelActionEnum action, String snid, String requestUrl) {

        ListenerMessage record = new ListenerMessage();
        if (StringUtils.isEmpty(requestUrl)) {
            requestUrl = new ServletServerHttpRequest(clientRequest).getURI().toString();
        }
        record.setUri(requestUrl);

        // user id
        record.setUserId(-1L);

        // cguid, guid
        String cookieRequestHeader = clientRequest.getHeader("Cookie");
        String cookieResponseHeader = proxyResponse.getHeader("Set-Cookie");
        String location = proxyResponse.getHeader("Location");
        record.setCguid(getGuid(cookieRequestHeader, cookieResponseHeader, location, "cguid"));
        record.setGuid(getGuid(cookieRequestHeader, cookieResponseHeader, null, "tguid"));

        // client remote IP
        record.setRemoteIp(getRemoteIp(clientRequest));

        // language code
        record.setLangCd("");

        // user agent
        record.setUserAgent(getUserAgent(clientRequest));

        // geography identifier
        record.setGeoId(-1L);

        // udid
        record.setUdid("");

        // referer
        record.setReferer(getReferer(clientRequest));

        // site id
        record.setSiteId(-1L);

        // landing page url
        record.setLandingPageUrl("");

        // source and destination rotation id
        record.setSrcRotationId(-1L);
        record.setDstRotationId(-1L);

        // Set the channel type + HTTP headers + channel action
        record.setChannelType(channelType);
        record.setHttpMethod(this.getMethod(clientRequest).getAvro());
        record.setChannelAction(action.getAvro());
        // Format record
        record.setRequestHeaders(serializeRequestHeaders(clientRequest));
        record.setResponseHeaders(serializeResponseHeaders(proxyResponse));
        record.setTimestamp(startTime);

        // Get snapshotId and short snapshotId from request
        Long snapshotId = SnapshotId.getNext(ListenerOptions.getInstance().getDriverId(), startTime).getRepresentation();
        record.setSnapshotId(snapshotId);
        ShortSnapshotId shortSnapshotId = new ShortSnapshotId(record.getSnapshotId().longValue());
        record.setShortSnapshotId(shortSnapshotId.getRepresentation());

        record.setCampaignId(campaignId);
        record.setPublisherId(DEFAULT_PUBLISHER_ID);
        record.setSnid((snid != null) ? snid : "");
        record.setIsTracked(false);     //TODO No messages are Durability-tracked for now

        return record;
    }

    /**
     * Get User Agent
     */
    private String getUserAgent(HttpServletRequest request) {
        String userAgent = request.getHeader("User-Agent");
        return userAgent == null ? "" : userAgent;
    }

    /**
     * Get Referer header
     */
    private String getReferer(HttpServletRequest request) {
        String referer = request.getHeader("Referer");
        return referer == null ? "" : referer;
    }

    private boolean isInternalIp(String ip) {
        if (ip == null || ip.isEmpty()) {
            return false;
        }

        DomainIpChecker ipChecker = DomainIpChecker.getInstance();
        return ipChecker.isHostInNetwork(ip) || ip.startsWith("10.") || ip.startsWith("192.168.");
    }

    /**
     * Get client remote Ip
     */
    private String getRemoteIp(HttpServletRequest request) {
//        String remoteIp = request.getHeader("X-eBay-Client-IP");
//
//        if (remoteIp == null) {
//            String xForwardFor = request.getHeader("X-Forwarded-For");
//            if (xForwardFor != null && !xForwardFor.isEmpty()) {
//                remoteIp = xForwardFor.split(",")[0];
//            }
//        }
//        return remoteIp == null ? "" : remoteIp;
        String remoteIp = null;
        String xForwardFor = request.getHeader("X-Forwarded-For");
        if (xForwardFor != null && !xForwardFor.isEmpty()) {
            remoteIp = xForwardFor.split(",")[0];
        }

        if (remoteIp == null || remoteIp.isEmpty()) {
            remoteIp = RequestUtil.getRemoteAddr(request);
        }

        return remoteIp == null ? "" : remoteIp;
    }

    //parse cguid and tguid from response headers; if null, parse from request headers; if cguid is still null; parse from location
    public String getGuid(String cookieRequestHeader, String cookieResponseHeader, String location, String guid) {
        String result = null;
        if (cookieResponseHeader != null && !cookieResponseHeader.isEmpty()) {
            String[] splits = cookieResponseHeader.split(guid + "/");
            if (splits.length > 1) {
                result = splits[1].substring(0, 32);
            }
        }
        if (result == null && cookieRequestHeader != null && !cookieRequestHeader.isEmpty()) {
            String[] splits = cookieRequestHeader.split(guid + "/");
            if (splits.length > 1) {
                result = splits[1].substring(0, 32);
            }
        }
        if (result == null && location != null && !location.isEmpty() && guid == "cguid") {
            String[] splits = location.split(guid + "=");
            if (splits.length > 1) {
                result = splits[1].substring(0, 32);
            }
        }
        return result == null ? "" : result;
    }

    /**
     * Get request method and validate from request
     * @param clientRequest Request
     * @return HttpMethodEnum
     */
    public HttpMethodEnum getMethod(HttpServletRequest clientRequest) {
        HttpMethodEnum httpMethod = HttpMethodEnum.parse(clientRequest.getMethod());
        Validate.notNull(httpMethod, "Could not parse HTTP method from HTTP request=" + clientRequest.getMethod());
        return httpMethod;
    }

    private static final String CHOCO_TAG = "dashenId";
    private static final String REDIRECTION_CNT_TAG = "dashenCnt";

    public String appendURLWithChocolateTag(String urlStr) {
        URL url = null;
        try {
            url = new URL(urlStr);
        } catch (MalformedURLException e) {
        }
        String query = url.getQuery();
        // append snapshotId into URL
        if(!urlStr.contains(CHOCO_TAG)){
            if(query != null && ! query.isEmpty()) {
                urlStr += "&";
            }else{
                urlStr += "?";
            }
            urlStr += getChocoTag(urlStr);
        }
        // append redirection count into URL
        if(urlStr.contains(REDIRECTION_CNT_TAG)){
            Pattern p = Pattern.compile(REDIRECTION_CNT_TAG + "(=|%3D)[0-9]");
            Matcher m = p.matcher(urlStr);
            if (m.find()) {
                String urlWithCnt = m.group();
                int cnt = Integer.valueOf(urlWithCnt.substring(urlWithCnt.length()-1)) + 1;
                urlWithCnt = REDIRECTION_CNT_TAG + "=" + cnt;
                urlStr = urlStr.replaceAll(p.pattern(), urlWithCnt);
            }
        }else{
            urlStr += "&" + REDIRECTION_CNT_TAG + "=" + 0;
        }
        return urlStr;
    }

    public String getChocoTag(String requestUrl){
        String chocoTag = null;
        // append snapshotId into URL
        if(requestUrl.contains(CHOCO_TAG)){
            Pattern p = Pattern.compile(CHOCO_TAG + "(=|%3D)\\d+");
            Matcher m = p.matcher(requestUrl);
            if (m.find()) {
                chocoTag = m.group();
            }
        }else{
            Long snapshotId = SnapshotId.getNext(ListenerOptions.getInstance().getDriverId(), System.currentTimeMillis()).getRepresentation();
            chocoTag = CHOCO_TAG + "=" + snapshotId;
        }
        return chocoTag;
    }

    public String getRedirectionCount(String requestUrl){
        // append redirection count into URL
        String urlWithCnt = null;
        if(requestUrl.contains(REDIRECTION_CNT_TAG)){
            Pattern p = Pattern.compile(REDIRECTION_CNT_TAG + "(=|%3D)[0-9]");
            Matcher m = p.matcher(requestUrl);
            if (m.find()) {
                urlWithCnt = m.group();
            }
        }else{
            urlWithCnt = REDIRECTION_CNT_TAG + "=" + 0;
        }
        return urlWithCnt;
    }

    /**
     * Return if it is core site url
     * @param clientRequest http servlet request
     * @return is core site
     */
    public boolean isCoreSite(HttpServletRequest clientRequest) {
        String serverName = clientRequest.getServerName();
        if(ListenerOptions.getInstance().getRoverCoreSites().contains(serverName)) {
            return true;
        }
        return false;
    }

    /**
     * Append dashen tags on Response Location header if it is necessary
     * @param clientRequest Request
     * @param proxyResponse Response, for first redirect to Rover, original request will
     *                      be attached to Location header as part of the URL query
     */
    public boolean appendTagWhenRedirect(HttpServletRequest clientRequest, HttpServletResponse proxyResponse, String requestUrl)
      throws MalformedURLException, UnsupportedEncodingException {
        // no action if no redirect
        if (proxyResponse.getStatus() != 301 || proxyResponse.getHeader(REDIRECT_HEADER) == null) {
            return false;
        }

        // assume Rover always return valid URL
        URL redirectUrl = new URL(proxyResponse.getHeader(REDIRECT_HEADER));

        // redirect to other domain
        if (!redirectUrl.getHost().startsWith("rover")) {
            return false;
        }

        boolean withQuery = (redirectUrl.getQuery() != null);
        //SET chocolate tags in location
        String chocoTag = getChocoTag(requestUrl);
        String redirectCnt = getRedirectionCount(requestUrl);
        String location = proxyResponse.getHeader(REDIRECT_HEADER);
        // append snapshotId into URL if not exist
        if (!location.contains(CHOCO_TAG)) {
            location = withQuery ? location + "&" : location + "?";
            location += chocoTag;
        }
        // append redirection count into URL if not exist
        if (location.contains(REDIRECTION_CNT_TAG)) {
            Pattern p = Pattern.compile(REDIRECTION_CNT_TAG + "(=|%3D)[0-9]");
            location = location.replaceAll(p.pattern(), redirectCnt);
        } else {
            location += "&" + redirectCnt;
        }
        proxyResponse.setHeader(REDIRECT_HEADER, location);
        return true;
    }

    public String serializeRequestHeaders(HttpServletRequest clientRequest) {
        StringBuilder requestHeaders = new StringBuilder();
        for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements();) {
            String headerName = e.nextElement();
            requestHeaders.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
        }
        if(!StringUtils.isEmpty(requestHeaders.toString())) requestHeaders.deleteCharAt(0);
        return requestHeaders.toString();
    }

    private String serializeResponseHeaders(HttpServletResponse response) {
        StringBuilder requestHeaders = new StringBuilder();
        for (String headerName: response.getHeaderNames()) {
            requestHeaders.append("|").append(headerName).append(": ").append(response.getHeader(headerName));
        }
        requestHeaders.deleteCharAt(0);
        return requestHeaders.toString();
    }

    /** returns the singleton instance */
    public static MessageObjectParser getInstance() {
        return INSTANCE;
    }

    /**
     * Initialize singleton instance
     */
    public static synchronized void init() {
        if (INSTANCE == null) {
            INSTANCE = new MessageObjectParser();
        }
    }
}
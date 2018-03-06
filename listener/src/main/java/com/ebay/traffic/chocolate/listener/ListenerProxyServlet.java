package com.ebay.traffic.chocolate.listener;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.listener.channel.Channel;
import com.ebay.traffic.chocolate.listener.channel.ChannelFactory;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.proxy.AsyncProxyServlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class ListenerProxyServlet extends AsyncProxyServlet.Transparent {
    /** All serializable classes need a UID. */
    private static final long serialVersionUID = 8041506560324325858L;
    private static final String PROXY_FAILURE = "proxyFailure";
    private static final String CLIENT_FAILURE = "clientFailure";
    private static final String MALFORMED_URL = "malformedURL";

    private static final int REQUEST_BUFFER_SIZE = 1024 * 12;
    private static final Logger logger = Logger.getLogger(ListenerProxyServlet.class);
    private static final String UTF_8 = "UTF-8";
    /** According to http://www.ietf.org/rfc/rfc2396.txt
     *  Below characters should not be encoded in the URL
     */
    private static final String[] RESERVED = {";" , "/" , "?" , ":" , "@" , "&" , "=" , "+" , "$" , ","};


    private static String outputHttpPort;
    private static String outputHttpsPort;
    private static int inputHttpPort;
    private static int inputHttpsPort;
    private MetricsClient metrics;


    @Override
    public void init()throws ServletException {
        ServletConfig config = getServletConfig();

        outputHttpPort = config.getInitParameter(ListenerOptions.OUTPUT_HTTP_PORT);
        outputHttpsPort = config.getInitParameter(ListenerOptions.OUTPUT_HTTPS_PORT);
        inputHttpPort = Integer.parseInt(config.getInitParameter(ListenerOptions.INPUT_HTTP_PORT));
        inputHttpsPort = Integer.parseInt(config.getInitParameter(ListenerOptions.INPUT_HTTPS_PORT));
        metrics = MetricsClient.getInstance();
        metrics.meter(PROXY_FAILURE, 0);
        metrics.meter(CLIENT_FAILURE, 0);
        metrics.meter(MALFORMED_URL, 0);

        super.init();

    }

    /**
     * Proxy the response to the client, then perform channel-specific post-processing
     */
    @Override
    protected void onProxyResponseSuccess(HttpServletRequest clientRequest, HttpServletResponse proxyResponse, Response serverResponse)
    {
        Channel channel = ChannelFactory.getChannel(clientRequest);
        channel.process(clientRequest, proxyResponse);
        super.onProxyResponseSuccess(clientRequest, proxyResponse, serverResponse);
    }

    /**
     * Do not add Proxy related headers
     */
    @Override
    protected void addProxyHeaders(HttpServletRequest clientRequest, Request proxyRequest) {
    }

        /**
         * Add chocolate special header for tracking
         */
    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response){
        try {
            this.getHttpClient().setRequestBufferSize(REQUEST_BUFFER_SIZE);
            super.service(request, response);
            response.addHeader("X-EBAY-CHOCOLATE", "true");
        } catch (ServletException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle and track response, onProxyResponseFailure handle exception from server side
     * and return status 502/504 to client
     */
    @Override
    protected void onProxyResponseFailure(HttpServletRequest clientRequest, HttpServletResponse proxyResponse, Response serverResponse, Throwable failure){
        metrics.meter(PROXY_FAILURE);
        logger.warn(failure);
        super.onProxyResponseFailure(clientRequest, proxyResponse, serverResponse, failure);
    }

    /**
     * Track response, onClientRequestFailure handle exception from client side
     * and return status 408/500 to client
     */
    @Override
    protected void onClientRequestFailure(HttpServletRequest clientRequest, org.eclipse.jetty.client.api.Request proxyRequest, HttpServletResponse proxyResponse, Throwable failure){
        metrics.meter(CLIENT_FAILURE);
        logger.warn(failure);
        super.onClientRequestFailure(clientRequest, proxyRequest, proxyResponse, failure);
    }

    /**
     * Point the request to LB, set the port based on input request
     * @param clientRequest incoming Http request
     * @return request full URL string to the destination
     */
    @Override
    protected String rewriteTarget(HttpServletRequest clientRequest) {
        try {
            URI rewrittenURI = URI.create(super.rewriteTarget(clientRequest));
            return setPort(rewrittenURI, portMapping(clientRequest.getLocalPort()));
        }
        catch (IllegalArgumentException e) {
            metrics.meter(MALFORMED_URL);
            reencodeQuery(clientRequest);
            URI rewrittenURI = URI.create(super.rewriteTarget(clientRequest));
            return setPort(rewrittenURI, portMapping(clientRequest.getLocalPort()));
        }
    }

    void reencodeQuery(HttpServletRequest clientRequest) {
        String query = clientRequest.getQueryString();
        try {
            query = URLDecoder.decode(query, UTF_8);
            query = URLEncoder.encode(query, UTF_8); // this encodes reserved characters and ' ' wrongly for query strings
            query = query.replaceAll("\\+", "%20");
            // convert reserved characters back
            for(String pattern: RESERVED){
                query = query.replace( URLEncoder.encode(pattern, UTF_8), pattern);
            }
        } catch (UnsupportedEncodingException unused) {
            // do nothing - should never be thrown, because encoding is hard-coded to UTF-8
        }
        org.eclipse.jetty.server.Request request = (org.eclipse.jetty.server.Request) clientRequest;
        request.setQueryString(query);
    }

    /**
     * Mapping input port to output port
     * in prod:    8080 -> 8080, 8082 -> 8082
     * in staging: 8080 -> 80, 8082 -> 443
     * @param port port of the incoming request
     * @return port for the destination(LB)
     */
    private static int portMapping(int port){
        if (port == inputHttpPort)
            return Integer.parseInt(outputHttpPort);
        else
            return Integer.parseInt(outputHttpsPort);
    }

    /**
     * Set port for the rewrittenURI
     * @param uri the origin rewritten URI
     * @param port the destination port to set
     * @return final URL in String format
     */
    private static String setPort(URI uri, int port){
        String url = null;
        try {
            url = new URIBuilder(uri).setPort(port).build().toString();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return url;
    }
}
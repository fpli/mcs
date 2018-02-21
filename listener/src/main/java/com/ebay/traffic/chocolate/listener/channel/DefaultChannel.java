package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DefaultChannel implements Channel {
    private static final Logger logger = Logger.getLogger(DefaultChannel.class);
    private final MetricsClient metrics;

    DefaultChannel(MetricsClient metrics) {
        this.metrics = metrics;
    }

    /**
     * Default channel handler proxies the request and logs a warning
     */
    @Override
    public void process(HttpServletRequest servletRequest, HttpServletResponse proxyResponse) {
        logger.warn("Un-managed channel request: " + servletRequest.getRequestURL());
        metrics.meter("un-managed");
    }

    @Override
    public long getPartitionKey(final HttpServletRequest servletRequest) {
        return servletRequest.hashCode();
    }
}
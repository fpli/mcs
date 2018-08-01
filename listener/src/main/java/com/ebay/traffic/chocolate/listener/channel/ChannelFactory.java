package com.ebay.traffic.chocolate.listener.channel;

/**
 * A simple factory method to assign different channel for different incoming
 * servletRequest based on pattern match
 */

public class ChannelFactory {
    public static Channel createChannel() {
        return new DefaultChannel();
    }
}

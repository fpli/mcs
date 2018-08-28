package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;

/**
 * Created by spugach on 11/14/16.
 */
public interface FilterRule {
    /**
     * Check if the rule is applied to a specific event type
     * @param action ChannelAction to check
     * @return true if
     */
    boolean isChannelActionApplicable(ChannelAction action);

    /**
     * Test the event. Returns a weight value of [0.0, 1.0]. A failed rule adds the weight to an accumulator.
     * As soon as the accumulator hits 1.0, the event is failed.
     * For example, a return value of 1.0 (or greater) is an outright fail.
     * @param event event (impression/click) to test
     * @return a value showing the "invalidity" of the event according to the rule
     */
    int test(FilterRequest event);
}

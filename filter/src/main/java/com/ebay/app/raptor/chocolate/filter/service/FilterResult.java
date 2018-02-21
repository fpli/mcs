package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;

/**
 * Result of the filtering test:
 * Verdict - is the event (click/impression) valid?
 * FailedRule - type of rule that the event failed, or RuleType.NONE
 *
 * Created by spugach on 11/29/16.
 */
public class FilterResult {
    private boolean isValid;
    private FilterRuleType failedRule;

    /**
     * Create a FilterResult with all its attributes
     * @param isValid true = passed (good), false = failed (bad)
     * @param failedRule rule that the event failed (or RuleType.NONE if passed)
     */
    public FilterResult(boolean isValid, FilterRuleType failedRule) {
        this.isValid = isValid;
        this.failedRule = failedRule;
    }

    public boolean isEventValid() {
        return this.isValid;
    }

    public FilterRuleType getFailedRule() {
        return this.failedRule;
    }
}
package com.ebay.app.raptor.chocolate.adservice.util;

import java.security.InvalidParameterException;

/**
 * IAB whitelist entry
 * Matches for presence of a pattern in the (user agent) string, with options.
 * Currently the two options checked are
 * - is the pattern active
 * - is it checked at the start, or anywhere
 *
 * Created by spugach on 11/16/16.
 */
public class TwoParamsListEntry {
    private boolean isActive = false;
    private boolean isStart = false;
    private String searchStr;

    /**
     * Create a whitelist entry from IAB list entry (|-separated)
     * @param botListEntry IAB whitelist entry
     */
    public TwoParamsListEntry(String botListEntry) {
        if (!botListEntry.matches("^[^\\|]+\\|[01]\\|[01]")) {
            throw new InvalidParameterException();
        }
        String[] params = botListEntry.split("\\|");
        this.searchStr = params[0].toLowerCase();
        this.isActive = (params[1].equals("1"));
        this.isStart = (params[2].equals("1"));
    }

    /**
     * Check a string against the whitelist pattern
     * @param uaString string to check
     * @return is there a match (given options)?
     */
    public boolean match(String uaString) {
        if (!this.isActive || uaString == null || uaString.isEmpty()) {
            return false;
        }

        if (this.isStart) {
            return uaString.toLowerCase().startsWith(this.searchStr);
        } else {
            return uaString.toLowerCase().contains(this.searchStr);
        }
    }
}

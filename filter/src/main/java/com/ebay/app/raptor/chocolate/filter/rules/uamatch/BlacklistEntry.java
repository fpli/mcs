package com.ebay.app.raptor.chocolate.filter.rules.uamatch;

import java.security.InvalidParameterException;

/**
 * IAB blacklist entry
 * Matches for presence of a pattern in the (user agent) string, with options.
 * Currently the three options checked are
 * - is the pattern active
 * - is it checked at the start, or anywhere
 * - exceptions
 *
 * Created by spugach on 11/16/16.
 */
public class BlacklistEntry {
    private boolean isActive = false;
    private boolean isStart = false;
    private String searchStr;
    private String[] exceptions;

    /**
     * Create a blacklist entry from IAB list entry (|-separated)
     * @param botListEntry IAB blacklist entry
     */
    public BlacklistEntry(String botListEntry) {
        if (!botListEntry.matches("^[^\\|]+\\|[01]\\|[^\\|]*\\|[01]\\|[012]\\|[01]")) {
            throw new InvalidParameterException();
        }

        String[] params = botListEntry.split("\\|");
        this.searchStr = params[0].toLowerCase();
        this.isActive = (params[1].equals("1"));
        this.isStart = (params[5].equals("1"));
        this.exceptions = params[2].split(",");
    }

    /**
     * Check a string against the blacklist pattern
     * @param uaString string to check
     * @return is there a match (given options)?
     */
    public boolean match(String uaString) {
        if (!this.isActive || uaString == null || uaString.isEmpty()) {
            return false;
        }

        String uaLower = uaString.toLowerCase();

        boolean matched;
        if (this.isStart) {
            matched = uaLower.startsWith(this.searchStr);
        } else {
            matched = uaLower.contains(this.searchStr);
        }

        if (matched) {
            for (String ex : this.exceptions) {
                if (!ex.isEmpty() && uaLower.contains(ex.trim().toLowerCase())) {
                    matched = false;
                    break;
                }
            }
        }

        return matched;
    }
}

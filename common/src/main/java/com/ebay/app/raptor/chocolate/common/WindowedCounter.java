package com.ebay.app.raptor.chocolate.common;

import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Maintains a counter according to a total and also a windowed time interval. 
 * 
 * For example, suppose we wanted to maintain a count over the last minute. Obviously it'd 
 * be intractable to keep a sequence of timestamps, one per count, and check them all to expire the old 
 * ones out. So, instead, we keep a counter for each "window" representing a counter for a particular 
 * time interval and a maximum number of windows to track. 
 * 
 * Smaller windows mean more granularity, but more times the task will run to expire windows. 
 * 
 * The total window interval is represented as numWindows * windowMillis (for the individual frame length).
 * 
 * E.g., a good setting for a 1-minute counter might be 5-second windows & 12 windows total. 
 * 
 * I also include a total counter which is always incremented because almost all metrics using this will 
 * also incorporate the total, so it make sense to bundle this as well. 
 * 
 * @author jepounds
 */
public class WindowedCounter {

    /** Logging instance */
    private static final Logger logger = Logger.getLogger(WindowedCounter.class);
    
    /** The name of this particular counter. */
    final String name; 
    
    /** Max windows this counter represents. */
    final int maxWindows;
    
    /** 
     * Deque representing the windows. Must always be accessed by its own lock. 
     * why aren't we using a concurrent queue? Simple, because the concurrent 
     * idioms aren't strong enough, so we have to use the inherent lock anyway. 
     */
    final LinkedList<Long> windows;
    
    /** Total counter tracker. */
    final AtomicLong total;
    
    /**
     * Ctor. The total time window represented, in milliseconds, 
     * is the max windows times the frame length in WindowedCounterTimer. 
     * So if the window length is 5000 milliseconds and there are 12 windows, then the object would span 
     * counters for 1-minute windows. 
     * 
     * @pre at least 1 window, name can't be blank
     * @param name            Name for this particular counter. 
     * @param maxWindows      Total number of extant windows to track
     */
    public WindowedCounter(final String name, final int maxWindows) {
        Validate.isTrue(StringUtils.isNotBlank(name), "Counter name can't be blank");
        Validate.isTrue(maxWindows >=1, "At least 1 window must be present");
        this.name = name;
        this.maxWindows = maxWindows;
        this.windows = new LinkedList<Long>();
        // At least one window should be ready
        this.windows.add(0l);
        
        total =  new AtomicLong(0l);
    }
    
    /** Enqueue the newest window and expire the oldest one if needed */
    void createWindow() {
        synchronized (windows) {
            Validate.isTrue(windows.size() <= maxWindows, "Invalid window length");
            logger.trace("createWindow called; windows=" + windows.size());
            
            // If windows size is greater than the max, then drop the oldest one. 
            if (windows.size() == maxWindows) {
                windows.remove(); // drop the head. 
            }
            
            // Now add in a new window at the end of the list. 
            windows.add(0l);
        }
    }
    
    /** 
     * Increment the totals represented by one.
     */
    public void increment() {
        accumulate(1);
    }
    
    /** 
     * Accumulate the totals represented by the specified number. 
     * I don't care about synchronizing the total with the window count, 
     * so take your deadlock empire strategy away. 
     * 
     * @param unit    unit to increment by, should be equal to or greater than 1
     */
    public void accumulate(final long unit) {
        Validate.isTrue(unit > 0, "unit should be greater than 1");
        total.addAndGet(unit);
        
        // Increment the latest window at hand. 
        synchronized (windows) {
            Validate.isTrue(!windows.isEmpty(), "windows shouldn't be empty");
            
            // Now accumulate the tail frame
            windows.set(windows.size()-1, windows.getLast().longValue() + unit); 
        }
    }
    
    /** @return name of this counter */
    public String getName() { return this.name; }
    
    /** @return the total represented in this count */
    public long getTotalCount() { return total.get(); }
    
    /** @return the window time represented in this count. */
    public long getWindowCount() { 
        long count = 0;
        synchronized (windows) {
            for (Long window : windows) count += window.longValue();
        }
        return count;
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("name=").append(name).append(" maxWindows=").append(maxWindows)
          .append(" total_count=").append(getTotalCount())
          .append(" window_count=").append(getWindowCount());
        return sb.toString();
    }
}

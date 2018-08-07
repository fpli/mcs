package com.ebay.app.raptor.chocolate.common;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;

/**
 * Batch implementation for the timer task inherent within windowed counter. 
 * 
 * @author jepounds
 */
public class WindowedCounterTimer {

    /** Logging information */
    private static final Logger logger = Logger.getLogger(WindowedCounterTimer.class);
    
    /** The number of milliseconds each window represents. */ 
    final int windowMillis;
 
    /** The timer to run. */
    final Timer timer;
    
    /** Whether we've called start or not. */
    volatile boolean started = false;
    
    /** The list of windowed counters to update. */
    final List<WindowedCounter> counters = Collections.synchronizedList(new LinkedList<WindowedCounter>());

    /**
     * Ctor. The total time window represented, in milliseconds, 
     * is the max windows times the frame length in WindowedCounterTimer. 
     * So if the window length is 5000 milliseconds and there are 12 windows, then the object would span 
     * counters for 1-minute windows. 
     * 
     * @pre at least 50 milliseconds for windows and at least 1 window
     * @param windowMillis      Scheduled window frame time for updates
     */
    public WindowedCounterTimer(final int windowMillis) {
        Validate.isTrue(windowMillis >= 50, "Window length must be at least 50ms");
        timer = new Timer(true); // true means schedule as a daemon thread as to not to block JVM on shutdown
        this.windowMillis = windowMillis;
    }
    
    /** Start the timer. Can only do this once. */
    public synchronized void start() {
        Validate.isTrue(!started, "Can call start only once.");
        started = true;
        
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                createWindows();
            }
        }, windowMillis, windowMillis);
    }
    
    /** Cancel the timer */ 
    public synchronized void stop() { 
        if (!started) return; 
        timer.cancel(); 
    }
    
    /** 
     * Register a counter 
     * 
     * @pre no null parameters
     * @param counter to register
     */
    public void register(WindowedCounter counter) {
        Validate.notNull(counter, "Counter can't be null");
        this.counters.add(counter);
        logger.info("Registered windowed counter:" + counter.getName());
    }
    
    /**
     * Runs the counters together
     */
    void createWindows() {
        synchronized (counters) {
            for (WindowedCounter counter : counters)
                counter.createWindow();
        }
    }
}

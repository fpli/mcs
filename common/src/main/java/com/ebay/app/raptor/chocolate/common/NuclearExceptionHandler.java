package com.ebay.app.raptor.chocolate.common;

import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * So, the ingester spawns off a bunch of child threads for Kafka consumption, etc. 
 * 
 * Because we are not acquainted with the right model of exception handling for 
 * Kafka, Cassandra, etc., we decided to simply kill off the entire process on shutdown. 
 * 
 * But we don't want to simply shut down the whole process. 
 * 1. We set an error state with the REST server. 
 * 2. We get passed in a shutdown flag which is set on exception, to allow for graceful stop
 * 
 * @author jepounds
 */
public class NuclearExceptionHandler {
    
    /** Singleton instance. */
    static volatile NuclearExceptionHandler INSTANCE = null;
    
    /** Singleton exception handler instance. */
    static volatile UncaughtExceptionHandler THREAD_EXCEPTION_HANDLER = null;
    
    /** Logging instance */
    private static final Logger logger = Logger.getLogger(NuclearExceptionHandler.class);
    
    /** Shutdown flag that gets set to true on catching an exception */
    private final AtomicBoolean shutdownFlag;
    
    /**
     * Ctor - do-nothing
     * 
     * @param shutdownFlag set this to true when we catch an exception. 
     */
    public NuclearExceptionHandler(AtomicBoolean shutdownFlag) {
        Validate.notNull(shutdownFlag, "Shutdown flag cannot be null");
        this.shutdownFlag = shutdownFlag;
    }
 
    /**
     * Process uncaught exception. 
     * 
     * @param context  the context of this exception. Usually it'd be something like 
     *                 a thread ID. 
     * @param e the exception we're catching. 
     */
    public void callback(String context, Throwable e) {
        // Log what we've observed.
        logger.fatal("Caught unhandled exception in context=" + context + " terminating system", e);

        // Shut down the system. 
        shutdownFlag.set(true);
    }
    
    /**
     * In theory, we could do without synchronized as only the main thread should be setting this. 
     * 
     * In practice, I write dumb code. 
     * 
     * @pre global instance must be null, and params can't be null
     * @param e to set - can't be null
     */
    public static synchronized void setGlobalHandler(NuclearExceptionHandler e) {
        Validate.isTrue(INSTANCE == null, "Exception handler can't be set");
        Validate.isTrue(THREAD_EXCEPTION_HANDLER == null, "Threaded exception handler can't be set");
        Validate.notNull(e, "Exception handler must be present");
        INSTANCE = e;
        THREAD_EXCEPTION_HANDLER = new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                String context = "Caught exception in thread id=" + Long.toString(t.getId());
                INSTANCE.callback(context, e);
            }
        };
    }

    /** @return the global handler. */
    public static NuclearExceptionHandler getGlobalHandler() {
        Validate.notNull(INSTANCE, "Instance should be set");
        return INSTANCE;
    }
    
    /** @return the global thread-specific handler. */
    public static UncaughtExceptionHandler getGlobalThreadExceptionHandler() {
        Validate.notNull(THREAD_EXCEPTION_HANDLER, "Thread exception handler cannot be null at this point");
        return THREAD_EXCEPTION_HANDLER;
    }
}

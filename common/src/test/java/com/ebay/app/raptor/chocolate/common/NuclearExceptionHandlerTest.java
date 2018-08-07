package com.ebay.app.raptor.chocolate.common;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jepounds
 */
@SuppressWarnings("javadoc")
public class NuclearExceptionHandlerTest {

    @Test
    public void testCallback() {
        AtomicBoolean b = new AtomicBoolean(false);
        NuclearExceptionHandler handler = new NuclearExceptionHandler(b);
        // We simply want to make sure that the handler is capable of handling
        // nulls, emptys, etc., all the while setting the shutdown flag.
        // valid parameters
        handler.callback("ctx", new Exception("Test exception"));
        assertTrue(b.get());

        // empty string
        b.set(false);
        handler.callback("", new Exception("Test exception"));
        assertTrue(b.get());

        // null string
        b.set(false);
        handler.callback(null, new Exception("Test exception"));
        assertTrue(b.get());

        // null exception
        b.set(false);
        handler.callback("ctx", null);
        assertTrue(b.get());
    }

    @Test(expected = NullPointerException.class)
    public void testNullInput() {
        // we must pass in a stop flag no matter what.
        new NuclearExceptionHandler(null);
    }

    @Test
    public void testGracefulExit() throws InterruptedException {
        final AtomicBoolean stopFlag = new AtomicBoolean(false);
        
        // Create a thread that we aim to throw an exception within. 
        // This would simulate the child thread that throws an exception. 
        final Thread childThread = new Thread() {
            @Override
            public void run() {
                try {
                    // Give it 5 seconds. 
                    System.out.println("Child thread sleeping 3 seconds...");
                    Thread.sleep(3000);
                    System.out.println("Now throwing runtime exception from child thread");
                    throw new RuntimeException("I barfed");
                }
                catch (InterruptedException e) {
                    fail();
                }
            }
        };

        // Create a thread that loops on a stop flag. This would simulate the 
        // "main" program we wish to exit gracefully. 
        Thread mainService = new Thread() {
            @Override
            public void run() {
                System.out.println("Service thread starting child thread...");
                childThread.start();
                
                int loops = 0;
                while (!stopFlag.get()) {
                    try {
                        System.out.println("Test thread iteration=" + (++loops));
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        System.out.println("Thread aborted...");
                        continue;
                    }
                }
                System.out.println("Stop flag detected from main thread; joining child thread.");
                try {
                    childThread.join();
                }
                catch (InterruptedException e) {
                    fail();
                }
                System.out.println("Main thread clean shutdown complete.");
            }
        };
                
        NuclearExceptionHandler.INSTANCE = null;
        NuclearExceptionHandler.THREAD_EXCEPTION_HANDLER = null;
        NuclearExceptionHandler.setGlobalHandler(new NuclearExceptionHandler(stopFlag));
        
        // Now set the handler on the child thread. 
        childThread.setUncaughtExceptionHandler(NuclearExceptionHandler.THREAD_EXCEPTION_HANDLER);
        
        // Run the service thread. 
        mainService.start();
        mainService.join();
    }

    @Test(expected = NullPointerException.class)
    public void testUnsetHandler() {
        NuclearExceptionHandler.INSTANCE = null;
        NuclearExceptionHandler.THREAD_EXCEPTION_HANDLER = null;
        NuclearExceptionHandler.getGlobalHandler();
    }

    @Test(expected = NullPointerException.class)
    public void testUnsetThreadHandler() {
        NuclearExceptionHandler.INSTANCE = null;
        NuclearExceptionHandler.THREAD_EXCEPTION_HANDLER = null;
        NuclearExceptionHandler.getGlobalThreadExceptionHandler();
    }
}

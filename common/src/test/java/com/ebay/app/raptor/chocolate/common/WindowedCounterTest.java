package com.ebay.app.raptor.chocolate.common;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author jepounds
 */
@SuppressWarnings("javadoc")
public class WindowedCounterTest {

    @Test(expected=IllegalArgumentException.class)
    public void testCtorNullName() {
        new WindowedCounter(null, 100);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCtorEmptyName() {
        new WindowedCounter("", 100);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCtorBlankName() {
        new WindowedCounter("  ", 100);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCtorLowMillis() {
        new WindowedCounterTimer(49);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCtorLowWindows() {
        new WindowedCounter("tooshort", 0);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testNullRegister() {
        WindowedCounterTimer timer = new WindowedCounterTimer(5000);
        timer.register(null);
    }
    
    @Test 
    public void testStartTwice() {
        WindowedCounterTimer timer = new WindowedCounterTimer(5000);
        boolean caught = false;
        timer.start();
        try {
            timer.start();
        } catch (IllegalArgumentException e) {
            caught = true;
        }
        timer.stop();
        assertTrue(caught);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidAccumulate() {
        WindowedCounter counter = new WindowedCounter("myname", 12);
        counter.accumulate(0l);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidWindowing() {
        WindowedCounter counter = new WindowedCounter("myname", 12);
        counter.windows.remove();
        counter.accumulate(1l);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInvalidWindowing2() {
        WindowedCounter counter = new WindowedCounter("myname", 1);
        counter.windows.add(1l);
        counter.createWindow();
    }
    
    @Test
    public void testIncrement() {
        WindowedCounter counter = new WindowedCounter("myname", 12);
        assertEquals("myname", counter.name);
        assertEquals("myname", counter.getName());
        assertEquals(12, counter.maxWindows);
        assertEquals(1, counter.windows.size());
        assertEquals(0l, counter.windows.get(0).longValue());
        assertEquals(0l, counter.getTotalCount());
        assertEquals(0l, counter.getWindowCount());
        
        // Test the windowing functionality and the total functionality
        long runningTotal = 0; 
        for (int i = 0; i < 20; ++i) {
            for (int j = 0; j < i; ++j) {
                counter.increment();
                ++runningTotal;
                assertEquals(runningTotal, counter.getTotalCount());
                assertEquals(j+1, counter.windows.get(counter.windows.size()-1).intValue());
            }
            counter.createWindow();

            // Add up all the frames we expect so far. basically, we expect the frames to 
            // increment like this... 
            // 0, 1, 2, 3, 4, 5 ... 
            // and we expect the first frame to be 0 (because we *just* called createWindow)
            assertEquals(Math.min(counter.maxWindows, i+2), counter.windows.size());
            long expectedWindowCount = 0;
            long expectedFrame = i+1;
            for (int j = counter.windows.size()-1; --j >= 0;) {
                expectedFrame = Math.max(0l, expectedFrame-1);
                assertEquals(expectedFrame, counter.windows.get(j).longValue());
                expectedWindowCount += expectedFrame;
            }
            assertEquals(expectedWindowCount, counter.getWindowCount());
        }
    }
    
    @Test
    public void testAccumulate() {
        WindowedCounter counter = new WindowedCounter("myname", 5);
        assertEquals("myname", counter.name);
        assertEquals(5, counter.maxWindows);
        assertEquals(1, counter.windows.size());
        assertEquals(0l, counter.windows.get(0).longValue());
        assertEquals(0l, counter.getTotalCount());
        assertEquals(0l, counter.getWindowCount());
        
        // Test the windowing functionality and the total functionality
        long runningTotal = 0; 
        for (int i = 0; i < 20; ++i) {
            counter.accumulate(i+1);
            counter.createWindow();

            // Add up all the frames we expect so far. basically, we expect the frames to 
            // increment like this... 
            // 1, 2, 3, 4, 5...
            // and we expect the first frame to be 0 (because we *just* called createWindow)
            assertEquals(Math.min(counter.maxWindows, i+2), counter.windows.size());
            long expectedWindowCount = 0;
            long expectedFrame = i+2;
            for (int j = counter.windows.size()-1; --j >= 0;) {
                expectedFrame = Math.max(1l, expectedFrame-1);
                assertEquals(expectedFrame, counter.windows.get(j).longValue());
                expectedWindowCount += expectedFrame;
            }
            assertEquals(expectedWindowCount, counter.getWindowCount());
        }
        
    }
    
    @Test
    public void testCycleSimple() throws InterruptedException {
        // Create a windowed count with 100 frames (more than we need) and 200 msec
        final WindowedCounter counter = new WindowedCounter("timer test", 100);
        final WindowedCounterTimer timer = new WindowedCounterTimer(200);
        
        // On another thread, alternate between incrementing / accumulating and then exit. 
        final Thread thread = new Thread() {
            @Override
            public void run() {
                // do this inside the span of ~500 msec, more than long enough for a window.
                for (int i = 0; i < 5; ++i) {
                    counter.increment();
                    counter.accumulate(2l);
                    counter.increment();
                    counter.accumulate(5l);
                    counter.increment();
                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException e) {
                        fail();
                    }
                }
            }
        };
        
        timer.register(counter);
        timer.start();
        thread.start();
        thread.join();

        // Should be equal to 50, which is 5 times cycle of 10
        assertEquals(50l, counter.getWindowCount());
        assertEquals(50l, counter.getTotalCount());
        timer.stop();
    }
    
    @Test
    public void testCycleWithWindowing() throws InterruptedException {
        // Create a windowed count with 2 frame and 100 msec, 
        // which is pretty aggressive
        final WindowedCounter counter = new WindowedCounter("timer test", 2);
        final WindowedCounterTimer timer = new WindowedCounterTimer(100);
        timer.register(counter);
        
        // On another thread, alternate between incrementing / accumulating and then exit. 
        final Thread thread = new Thread() {
            @Override
            public void run() {
                // do this inside the span of ~500 msec, more than long enough for a window.
                for (int i = 0; i < 5; ++i) {
                    counter.increment();
                    counter.accumulate(2l);
                    counter.increment();
                    counter.accumulate(5l);
                    counter.increment();
                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException e) {
                        fail();
                    }
                }
            }
        };
        
        timer.start();
        thread.start();
        thread.join();

        // Total is total and doesn't change depending on window
        assertEquals(50l, counter.getTotalCount());
        // Timer should definitely have ran in the interval we're testing. Maybe not on stupid Altus boxes though. 
        assertTrue(counter.getWindowCount() < counter.getTotalCount());
        System.out.println("After windowing, counter is=" + counter.toString());
        timer.stop();
    }
}

package com.ebay.traffic.chocolate.mkttracksvc.util;


import jersey.repackaged.com.google.common.collect.Iterables;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

@SuppressWarnings("javadoc")
public class RotationId18Test {

    @Test
    public void testGenerationForTesting() {
        final long time = System.currentTimeMillis();
        final int driver = 1;

        // Equality check. There's no way to guarantee a particular
        // sequence value under current design in the "normal" API, deliberately
        {
            RotationId18 test = RotationId18.generateForUnitTests(time, 0, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), time);
            assertEquals(test.getSequenceId(), 0l);
        }

        // Sequence check.
        final int sequenceId = 1023;
        {
            RotationId18 test = RotationId18.generateForUnitTests(time, sequenceId, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), time);
            assertEquals(test.getSequenceId(), sequenceId);
        }

        // Sequence check.
        final int sequenceId2 = 1024;
        {
            RotationId18 test = RotationId18.generateForUnitTests(time, sequenceId2, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), time);
            assertEquals(test.getSequenceId(), 0);
        }

        // Time check.
        {
            long past = time - 100000l;
            RotationId18 test = RotationId18.generateForUnitTests(past, sequenceId, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), past);
            assertEquals(test.getSequenceId(), sequenceId);
        }

        // Check for time.
        {
            final long past = System.currentTimeMillis() - 300000;
            RotationId18 snid = RotationId18.getNext(driver, past);
            assertEquals(driver, snid.getDriverId());
        }
    }

    @Test
    public void testEarlyTime() {
        final long time = System.currentTimeMillis();
        final long oldTime = time - 1000000l;
        final int driverId = 120;
        final int seqId = 12;

        RotationId18 test = RotationId18.generateForUnitTests(time, seqId, driverId);
        assertEquals(time, test.getTimeMillis());
        assertEquals(driverId, test.getDriverId());
        assertEquals(seqId, test.getSequenceId());

        // try and shovel in an older time. It shouldn't "revert" back.
        RotationId18 next = new RotationId18(test, driverId, oldTime);
        assertEquals(time, next.getTimeMillis());
        assertEquals(driverId, next.getDriverId());
        assertEquals(seqId+1, next.getSequenceId());

        // Create a new time. It should "advance"
        final long newTime = time + 1000000l;
        RotationId18 future = new RotationId18(next, driverId, newTime);
        assertEquals(newTime, future.getTimeMillis());
        assertEquals(driverId, future.getDriverId());
        assertEquals(0, future.getSequenceId());
    }

    @Test
    public void testZeroDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0;
        RotationId18 snid = new RotationId18(driver);
        assertEquals(snid.getDriverId(), driver);
        assertEquals(0, snid.getSequenceId());
        assertTrue(snid.getTimeMillis() - time <= 1000l);

        // Increment
        RotationId18 snid2 = new RotationId18(snid, driver);
        assertEquals(snid2.getDriverId(), driver);
        assertEquals(1, snid2.getSequenceId());
        assertEquals(snid2.getTimeMillis(), snid.getTimeMillis());
        Thread.sleep(1000);

        RotationId18 snid3 = new RotationId18(snid2, driver);
        assertEquals(snid3.getDriverId(), driver);
        assertEquals(0, snid3.getSequenceId());
        assertNotEquals(snid3.getTimeMillis(), snid.getTimeMillis());
        System.out.println(snid3.toDebugString());
        System.out.println(snid.toDebugString());
    }

    @Test
    public void testMaxDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0xFF;
        RotationId18 snid = new RotationId18(driver);

        assertEquals(snid.getDriverId(), driver);
        assertEquals(0, snid.getSequenceId());
        assertTrue(snid.getTimeMillis() - time <= 1000l);

        // Increment
        RotationId18 snid2 = new RotationId18(snid, driver);
        assertEquals(snid2.getDriverId(), driver);
        boolean isIncrement = snid2.getSequenceId() == 1 && snid.getTimeMillis() == snid2.getTimeMillis();
        boolean isAfter = snid2.getSequenceId() == 0 && snid2.getTimeMillis() > snid.getTimeMillis();
        assertTrue(isIncrement || isAfter);
        Thread.sleep(1000);

        RotationId18 snid3 = new RotationId18(snid2, driver);
        assertEquals(snid3.getDriverId(), driver);
        assertEquals(0, snid3.getSequenceId());
        assertNotEquals(snid3.getTimeMillis(), snid.getTimeMillis());
    }

    @Test
    public void testIntermediateDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0x78;
        RotationId18 snid = new RotationId18(driver);
        assertEquals(snid.getDriverId(), driver);
        assertEquals(0, snid.getSequenceId());
        assertTrue(snid.getTimeMillis() - time <= 1000l);

        // Increment
        RotationId18 snid2 = new RotationId18(snid, driver, time);
        assertEquals(snid2.getDriverId(), driver);
        assertEquals(1, snid2.getSequenceId());
        assertEquals(snid2.getTimeMillis(), snid.getTimeMillis());
        Thread.sleep(1000);

        RotationId18 snid3 = new RotationId18(snid2, driver);
        assertEquals(snid3.getDriverId(), driver);
        assertEquals(0, snid3.getSequenceId());
        assertNotEquals(snid3.getTimeMillis(), snid.getTimeMillis());
    }

    private static class RotationId18ThreadTest implements Runnable {
        private final int driverId = 3;

        private final List<RotationId18> snids = Collections
                .<RotationId18>synchronizedList(new ArrayList<RotationId18>());

        @Override
        public void run() {
            RotationId18 before = RotationId18.getNext(driverId);
            snids.add(before);
            for (int i = 1; i < 200; ++i) {
                RotationId18 current = RotationId18.getNext(driverId);
                assertTrue(current.getRepresentation() > before.getRepresentation());
                assertEquals(current.getDriverId(), driverId);
                snids.add(current);
                before = current;
            }
        }

        public List<RotationId18> getRotationId18s() {
            List<RotationId18> copy;
            synchronized (snids) {
                copy = new ArrayList<RotationId18>(snids.size());
                copy.addAll(snids);
            }
            return copy;
        }
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        final int numThreads = 5;
        Thread [] t = new Thread [numThreads];
        RotationId18ThreadTest [] runnables = new RotationId18ThreadTest [numThreads];

        for (int i = 0; i < numThreads; ++i) {
            runnables[i] = new RotationId18ThreadTest();
            t[i] = new Thread(runnables[i]);
        }

        for (int i = 0; i < numThreads; ++i)
            t[i].start();

        for (int i = 0; i < numThreads; ++i)
            t[i].join();

        // Assert no duplicates were made.
        Set<RotationId18> allRotationId18s = new HashSet<RotationId18>(numThreads * 200);
        for (RotationId18ThreadTest runnable : runnables) {
            List<RotationId18> snids = runnable.getRotationId18s();
            allRotationId18s.addAll(snids);
        }
        assertEquals(allRotationId18s.size(), numThreads * 200);
        assertTrue(RotationId18.getCounter() >= numThreads * 200);
        assertTrue(RotationId18.checkAndClearCounter(numThreads * 200l) >= numThreads * 200l);
        assertEquals(0l, RotationId18.getCounter());

        allRotationId18s.add(new RotationId18(0l));
        allRotationId18s.add(new RotationId18(-1l));
        List<RotationId18> sorted = Arrays.<RotationId18>asList(Iterables.toArray(allRotationId18s, RotationId18.class));
        Collections.sort(sorted);

        for (int i = 1; i < sorted.size(); ++i) {
            RotationId18 s1 = sorted.get(i - 1);
            RotationId18 s2 = sorted.get(i);
            assertTrue(s1.getTimeMillis() <= s2.getTimeMillis());
            if (s1.getTimeMillis() == s2.getTimeMillis()) {
                assertTrue(s1.getDriverId() <= s2.getDriverId());
                if (s1.getDriverId() == s2.getDriverId()) {
                    assertTrue(s1.getSequenceId() < s2.getSequenceId());
                }
            }
        }
    }

    @Test
    public void testTimestampInRotationId18(){
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, 2017);
        c.set(Calendar.MONTH, 10);
        c.set(Calendar.DATE, 26);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);

        Calendar max = Calendar.getInstance();
        max.set(Calendar.YEAR, 2110);
        max.set(Calendar.MONTH, 10);
        max.set(Calendar.DATE, 27);
        max.set(Calendar.HOUR_OF_DAY, 9);
        max.set(Calendar.MINUTE, 0);
        max.set(Calendar.SECOND, 0);
        RotationId18 snid = new RotationId18(1, c.getTimeInMillis());
        while(c.getTimeInMillis() == snid.getTimeMillis()){
            if(c.getTimeInMillis() > max.getTimeInMillis()){
                break;
            }
            c.add(Calendar.HOUR, 1);
            snid = new RotationId18(1, c.getTimeInMillis());
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        assertEquals("210905", sdf.format(c.getTime()));
        c.setTimeInMillis(snid.getTimeMillis());
        assertEquals("197001", sdf.format(c.getTime()));
        assertEquals("211011", sdf.format(max.getTime()));
    }
}


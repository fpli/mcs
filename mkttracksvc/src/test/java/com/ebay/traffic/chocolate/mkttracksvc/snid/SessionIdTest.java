package com.ebay.traffic.chocolate.mkttracksvc.snid;


import jersey.repackaged.com.google.common.collect.Iterables;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

@SuppressWarnings("javadoc")
public class SessionIdTest {

    @Test
    public void testGenerationForTesting() {
        final long time = System.currentTimeMillis();
        final int driver = 1;

        // Equality check. There's no way to guarantee a particular
        // sequence value under current design in the "normal" API, deliberately
        {
            SessionId test = SessionId.generateForUnitTests(time, 0, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), time);
            assertEquals(test.getSequenceId(), 0l);
        }

        // Sequence check.
        final int sequenceId = 2300;
        {
            SessionId test = SessionId.generateForUnitTests(time, sequenceId, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), time);
            assertEquals(test.getSequenceId(), sequenceId);
        }

        // Time check.
        {
            long past = time - 100000l;
            SessionId test = SessionId.generateForUnitTests(past, sequenceId, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), past);
            assertEquals(test.getSequenceId(), sequenceId);
        }

        // Check for time.
        {
            final long past = System.currentTimeMillis() - 300000;
            SessionId snid = SessionId.getNext(driver, past);
            assertEquals(0, snid.getSequenceId());
            assertEquals(driver, snid.getDriverId());
            assertEquals(past, snid.getTimeMillis());
        }
    }

    @Test
    public void testEarlyTime() {
        final long time = System.currentTimeMillis();
        final long oldTime = time - 1000000l;
        final int driverId = 120;
        final int seqId = 12;

        SessionId test = SessionId.generateForUnitTests(time, seqId, driverId);
        assertEquals(time, test.getTimeMillis());
        assertEquals(driverId, test.getDriverId());
        assertEquals(seqId, test.getSequenceId());

        // try and shovel in an older time. It shouldn't "revert" back.
        SessionId next = new SessionId(test, driverId, oldTime);
        assertEquals(time, next.getTimeMillis());
        assertEquals(driverId, next.getDriverId());
        assertEquals(seqId+1, next.getSequenceId());

        // Create a new time. It should "advance"
        final long newTime = time + 1000000l;
        SessionId future = new SessionId(next, driverId, newTime);
        assertEquals(newTime, future.getTimeMillis());
        assertEquals(driverId, future.getDriverId());
        assertEquals(0, future.getSequenceId());
    }

    @Test
    public void testZeroDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0;
        SessionId snid = new SessionId(driver);
        assertEquals(snid.getDriverId(), driver);
        assertEquals(0, snid.getSequenceId());
        assertTrue(snid.getTimeMillis() - time <= 1000l);

        // Increment
        SessionId snid2 = new SessionId(snid, driver);
        assertEquals(snid2.getDriverId(), driver);
        assertEquals(1, snid2.getSequenceId());
        assertEquals(snid2.getTimeMillis(), snid.getTimeMillis());
        Thread.sleep(1000);

        SessionId snid3 = new SessionId(snid2, driver);
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
        SessionId snid = new SessionId(driver);

        assertEquals(snid.getDriverId(), driver);
        assertEquals(0, snid.getSequenceId());
        assertTrue(snid.getTimeMillis() - time <= 1000l);

        // Increment
        SessionId snid2 = new SessionId(snid, driver);
        assertEquals(snid2.getDriverId(), driver);
        boolean isIncrement = snid2.getSequenceId() == 1 && snid.getTimeMillis() == snid2.getTimeMillis();
        boolean isAfter = snid2.getSequenceId() == 0 && snid2.getTimeMillis() > snid.getTimeMillis();
        assertTrue(isIncrement || isAfter);
        Thread.sleep(1000);

        SessionId snid3 = new SessionId(snid2, driver);
        assertEquals(snid3.getDriverId(), driver);
        assertEquals(0, snid3.getSequenceId());
        assertNotEquals(snid3.getTimeMillis(), snid.getTimeMillis());
    }

    @Test
    public void testIntermediateDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0x78;
        SessionId snid = new SessionId(driver);
        assertEquals(snid.getDriverId(), driver);
        assertEquals(0, snid.getSequenceId());
        assertTrue(snid.getTimeMillis() - time <= 1000l);

        // Increment
        SessionId snid2 = new SessionId(snid, driver);
        assertEquals(snid2.getDriverId(), driver);
        assertEquals(1, snid2.getSequenceId());
        assertEquals(snid2.getTimeMillis(), snid.getTimeMillis());
        Thread.sleep(1000);

        SessionId snid3 = new SessionId(snid2, driver);
        assertEquals(snid3.getDriverId(), driver);
        assertEquals(0, snid3.getSequenceId());
        assertNotEquals(snid3.getTimeMillis(), snid.getTimeMillis());
    }

    private static class SessionIdThreadTest implements Runnable {
        private final int driverId = 3;

        private final List<SessionId> snids = Collections
                .<SessionId>synchronizedList(new ArrayList<SessionId>());

        @Override
        public void run() {
            SessionId before = SessionId.getNext(driverId);
            snids.add(before);
            for (int i = 1; i < 1000; ++i) {
                SessionId current = SessionId.getNext(driverId);
                assertTrue(current.getRepresentation() > before.getRepresentation());
                assertEquals(current.getDriverId(), driverId);
                snids.add(current);
                before = current;
            }
        }

        public List<SessionId> getSessionIds() {
            List<SessionId> copy;
            synchronized (snids) {
                copy = new ArrayList<SessionId>(snids.size());
                copy.addAll(snids);
            }
            return copy;
        }
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        final int numThreads = 5;
        Thread [] t = new Thread [numThreads];
        SessionIdThreadTest [] runnables = new SessionIdThreadTest [numThreads];

        for (int i = 0; i < numThreads; ++i) {
            runnables[i] = new SessionIdThreadTest();
            t[i] = new Thread(runnables[i]);
        }

        for (int i = 0; i < numThreads; ++i)
            t[i].start();

        for (int i = 0; i < numThreads; ++i)
            t[i].join();

        // Assert no duplicates were made.
        Set<SessionId> allSessionIds = new HashSet<SessionId>(numThreads * 1000);
        for (SessionIdThreadTest runnable : runnables) {
            List<SessionId> snids = runnable.getSessionIds();
            allSessionIds.addAll(snids);
        }
        assertEquals(allSessionIds.size(), numThreads * 1000);
        assertTrue(SessionId.getCounter() >= numThreads * 1000);
        assertTrue(SessionId.checkAndClearCounter(numThreads * 1000l) >= numThreads * 1000l);
        assertEquals(0l, SessionId.getCounter());

        allSessionIds.add(new SessionId(0l));
        allSessionIds.add(new SessionId(-1l));
        List<SessionId> sorted = Arrays.<SessionId>asList(Iterables.toArray(allSessionIds, SessionId.class));
        Collections.sort(sorted);

        for (int i = 1; i < sorted.size(); ++i) {
            SessionId s1 = sorted.get(i - 1);
            SessionId s2 = sorted.get(i);
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
    public void testTimestampInSessionId(){
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
        SessionId snid = new SessionId(1, c.getTimeInMillis());
        while(c.getTimeInMillis() == snid.getTimeMillis()){
            if(c.getTimeInMillis() > max.getTimeInMillis()){
                break;
            }
            c.add(Calendar.HOUR, 1);
            snid = new SessionId(1, c.getTimeInMillis());
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        assertEquals("210905", sdf.format(c.getTime()));
        c.setTimeInMillis(snid.getTimeMillis());
        assertEquals("197001", sdf.format(c.getTime()));
        assertEquals("211011", sdf.format(max.getTime()));
    }
}


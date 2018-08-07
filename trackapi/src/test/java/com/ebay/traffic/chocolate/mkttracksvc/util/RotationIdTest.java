package com.ebay.traffic.chocolate.mkttracksvc.util;


import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import jersey.repackaged.com.google.common.collect.Iterables;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings("javadoc")
public class RotationIdTest {

    @Test
    public void testGenerationForTesting() {
        final long time = System.currentTimeMillis();
        final int driver = 1;

        // Equality check. There's no way to guarantee a particular
        // sequence value under current design in the "normal" API, deliberately
        {
            RotationId test = RotationId.generateForUnitTests(time, 0, driver);
            assertEquals(driver, test.getDriverId());
            assertEquals(time, test.getTimeMillis());
            assertEquals(0L, test.getSequenceId());
        }

        // Sequence check.
        final int sequenceId = 3;
        {
            RotationId test = RotationId.generateForUnitTests(time, sequenceId, driver);
            assertEquals(driver, test.getDriverId());
            assertEquals(time, test.getTimeMillis());
            assertEquals(sequenceId, test.getSequenceId());
        }

        // Sequence check.
        final int sequenceId2 = 8;
        {
            RotationId test = RotationId.generateForUnitTests(time, sequenceId2, driver);
            assertEquals(driver, test.getDriverId());
            assertEquals(time, test.getTimeMillis());
            assertEquals(8, test.getSequenceId());
        }

        // Time check.
        {
            long past = time - 100000l;
            RotationId test = RotationId.generateForUnitTests(past, sequenceId, driver);
            assertEquals(driver, test.getDriverId());
            assertEquals(past, test.getTimeMillis());
            assertEquals(sequenceId, test.getSequenceId());
        }

        // Check for time.
        {
            final long past = System.currentTimeMillis() - 300000;
            RotationId rid = RotationId.getNext(driver, past);
            assertEquals(driver, rid.getDriverId());
        }
    }

    @Test
    public void testEarlyTime() {
        final long time = System.currentTimeMillis();
        final long oldTime = time - 1000000l;
        final int driverId = 120;
        final int seqId = 8;

        RotationId test = RotationId.generateForUnitTests(time, seqId, driverId);
        assertEquals(time, test.getTimeMillis());
        assertEquals(driverId, test.getDriverId());
        assertEquals(seqId, test.getSequenceId());

        // try and shovel in an older time. It shouldn't "revert" back.
        RotationId next = new RotationId(test, driverId, oldTime);
        assertEquals(time, next.getTimeMillis());
        assertEquals(driverId, next.getDriverId());
        assertEquals(seqId+1, next.getSequenceId());

        // Create a new time. It should "advance"
        final long newTime = time + 1000000l;
        RotationId future = new RotationId(next, driverId, newTime);
        assertEquals(newTime, future.getTimeMillis());
        assertEquals(driverId, future.getDriverId());
        assertEquals(0, future.getSequenceId());
    }

    @Test
    public void testZeroDriver() throws InterruptedException {
        final long time = System.currentTimeMillis()+1;
        final int driver = 0;

        RotationId rid1 = RotationId.getNext(driver, time);
        assertTrue(rid1.getTimeMillis() - time <= 1000l);
        assertEquals(driver, rid1.getDriverId());
//        assertEquals(0, rid1.getSequenceId());

        // Increment
        RotationId rid2 = RotationId.getNext(driver, time);
        assertEquals(rid1.getTimeMillis(), rid2.getTimeMillis());
        assertEquals(driver, rid2.getDriverId());
//        assertEquals(1, rid2.getSequenceId());
        Thread.sleep(1000);

        RotationId rid3 = new RotationId(rid2, driver);
        assertEquals(driver, rid3.getDriverId());
//        assertEquals(0, rid3.getSequenceId());
        assertNotEquals(rid1.getTimeMillis(), rid3.getTimeMillis());
        System.out.println(rid1.toDebugString());
        System.out.println(rid2.toDebugString());
        System.out.println(rid3.toDebugString());
    }

    @Test
    public void testMaxDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0xFF;
        RotationId rid = new RotationId(driver);

        assertEquals(rid.getDriverId(), driver);
        assertEquals(0, rid.getSequenceId());
        assertTrue(rid.getTimeMillis() - time <= 1000l);

        // Increment
        RotationId rid2 = new RotationId(rid, driver);
        assertEquals(rid2.getDriverId(), driver);
        boolean isIncrement = rid2.getSequenceId() == 1 && rid.getTimeMillis() == rid2.getTimeMillis();
        boolean isAfter = rid2.getSequenceId() == 0 && rid2.getTimeMillis() > rid.getTimeMillis();
        assertTrue(isIncrement || isAfter);
        Thread.sleep(1000);

        RotationId rid3 = new RotationId(rid2, driver);
        assertEquals(rid3.getDriverId(), driver);
        assertEquals(0, rid3.getSequenceId());
        assertNotEquals(rid3.getTimeMillis(), rid.getTimeMillis());
    }

    @Test
    public void testIntermediateDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0x78;
        RotationId rid = new RotationId(driver);
        assertEquals(rid.getDriverId(), driver);
        assertEquals(0, rid.getSequenceId());
        assertTrue(rid.getTimeMillis() - time <= 1000l);

        // Increment
        RotationId rid2 = new RotationId(rid, driver, time);
        assertEquals(rid2.getDriverId(), driver);
        assertEquals(1, rid2.getSequenceId());
        assertEquals(rid2.getTimeMillis(), rid.getTimeMillis());
        Thread.sleep(1000);

        RotationId rid3 = new RotationId(rid2, driver);
        assertEquals(rid3.getDriverId(), driver);
        assertEquals(0, rid3.getSequenceId());
        assertNotEquals(rid3.getTimeMillis(), rid.getTimeMillis());
    }

    private static class RotationIdThreadTest implements Runnable {
        private final int driverId = 3;

        private final List<RotationId> rids = Collections.<RotationId>synchronizedList(new ArrayList<RotationId>());

        @Override
        public void run() {
            RotationId before = RotationId.getNext(driverId);
            rids.add(before);
            for (int i = 1; i < 5; ++i) {
                RotationId current = RotationId.getNext(driverId);
                assertTrue(current.getRepresentation() > before.getRepresentation());
                assertEquals(driverId, current.getDriverId());
                rids.add(current);
                before = current;
            }
        }

        public List<RotationId> getRotationIds() {
            List<RotationId> copy;
            synchronized (rids) {
                copy = new ArrayList<RotationId>(rids.size());
                copy.addAll(rids);
            }
            return copy;
        }
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        final int numThreads = 3;
        Thread [] t = new Thread [numThreads];
        RotationIdThreadTest [] runnables = new RotationIdThreadTest [numThreads];

        for (int i = 0; i < numThreads; ++i) {
            runnables[i] = new RotationIdThreadTest();
            t[i] = new Thread(runnables[i]);
        }

        for (int i = 0; i < numThreads; ++i)
            t[i].start();

        for (int i = 0; i < numThreads; ++i)
            t[i].join();

        // Assert no duplicates were made.
        Set<RotationId> allRotationIds = new HashSet<RotationId>(numThreads * 200);
        for (RotationIdThreadTest runnable : runnables) {
            List<RotationId> rids = runnable.getRotationIds();
            allRotationIds.addAll(rids);
        }
        assertEquals(RotationId.SEQUENCE_MASK, allRotationIds.size());
        assertTrue(RotationId.getCounter() >= RotationId.SEQUENCE_MASK);
        assertTrue(RotationId.checkAndClearCounter(RotationId.SEQUENCE_MASK) >= RotationId.SEQUENCE_MASK);
        assertEquals(0l, RotationId.getCounter());

        allRotationIds.add(new RotationId(0l));
        allRotationIds.add(new RotationId(-1l));
        List<RotationId> sorted = Arrays.<RotationId>asList(Iterables.toArray(allRotationIds, RotationId.class));
        Collections.sort(sorted);

        for (int i = 1; i < sorted.size(); ++i) {
            RotationId s1 = sorted.get(i - 1);
            RotationId s2 = sorted.get(i);
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
    public void testTimestampInRotationId(){
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
        RotationId rid = new RotationId(1, c.getTimeInMillis());
        while(c.getTimeInMillis() == rid.getTimeMillis()){
            if(c.getTimeInMillis() > max.getTimeInMillis()){
                break;
            }
            c.add(Calendar.HOUR, 1);
            rid = new RotationId(1, c.getTimeInMillis());
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        assertEquals("210905", sdf.format(c.getTime()));
        c.setTimeInMillis(rid.getTimeMillis());
        assertEquals("197001", sdf.format(c.getTime()));
        assertEquals("211011", sdf.format(max.getTime()));
    }

    @Test
    public void testRotationStrWithoutCampaignId(){
        final long time = System.currentTimeMillis();
        RotationId rid1 = RotationId.getNext(1, time);
        String rid1Str = rid1.getRotationStr(707, null);
        RotationId rid2 = RotationId.getNext(1, time);
        String rid2Str = rid2.getRotationStr(707, null);

        String expectedRid1 = String.valueOf(rid1.getRepresentation());
        expectedRid1 = "707-" + expectedRid1.substring(0,6) + "-" + expectedRid1.substring(6,12) + "-" + expectedRid1.substring(12);
        String expectedRid2 = expectedRid1.substring(0, expectedRid1.length() -1);
        expectedRid2 = expectedRid2 + (Integer.valueOf(expectedRid1.substring(expectedRid1.length()-1)) + 1);

        System.out.println(rid1Str);
        System.out.println(rid2Str);
        assertEquals(expectedRid1, rid1Str);
        assertEquals(expectedRid2, rid2Str);
        assertNotEquals(rid1Str, rid2Str);


        rid1Str = rid1.getRotationStr(707, -1L);
        rid2Str = rid2.getRotationStr(707, -2L);

        expectedRid1 = String.valueOf(rid1.getRepresentation());
        expectedRid1 = "707-" + expectedRid1.substring(0,6) + "-" + expectedRid1.substring(6,12) + "-" + expectedRid1.substring(12);
        expectedRid2 = expectedRid1.substring(0, expectedRid1.length() -1);
        expectedRid2 = expectedRid2 + (Integer.valueOf(expectedRid1.substring(expectedRid1.length()-1)) + 1);

        System.out.println(rid1Str);
        System.out.println(rid2Str);
        assertEquals(expectedRid1, rid1Str);
        assertEquals(expectedRid2, rid2Str);
        assertNotEquals(rid1Str, rid2Str);

    }

    @Test
    public void testRotationStrWithCampaignId(){
        final long time = System.currentTimeMillis();
        RotationId rid1 = RotationId.getNext(1, time);
        String rid1Str = rid1.getRotationStr(707, 244491L);
        RotationId rid2 = RotationId.getNext(1, time);
        String rid2Str = rid2.getRotationStr(707, 244754L);

        String expectedRid1 = String.valueOf(rid1.getTimeMillis());
        expectedRid1 = "707-244491-" + expectedRid1.substring(0,6) + "-" + expectedRid1.substring(6);
        String expectedRid2 = String.valueOf(rid2.getTimeMillis());
        expectedRid2 = "707-244754-" + expectedRid2.substring(0,6) + "-" + expectedRid2.substring(6);

        System.out.println(rid1Str);
        System.out.println(rid2Str);
        assertEquals(expectedRid1, rid1Str);
        assertEquals(expectedRid2, rid2Str);
        assertNotEquals(rid1Str, rid2Str);
    }

    @Test
    public void testRotationStr(){
        Integer clientId = 707;
        final long time = System.currentTimeMillis();
        RotationId rid1 = RotationId.getNext(1, time);
        String rid1Str = rid1.getRotationStr(clientId);
        RotationId rid2 = RotationId.getNext(1, time+1);
        String rid2Str = rid2.getRotationStr(clientId);

        String expectedRid1 = String.valueOf(rid1.getTimeMillis());
        expectedRid1 = "707-" + expectedRid1.substring(0,6) + "-" + expectedRid1.substring(6,12) + "-" + expectedRid1.substring(12);
        String expectedRid2 =  String.valueOf(rid2.getTimeMillis());
        expectedRid2 = "707-" + expectedRid2.substring(0,6) + "-" + expectedRid2.substring(6,12) + "-" + expectedRid2.substring(12);

        System.out.println(rid1Str);
        System.out.println(rid2Str);
        assertEquals(expectedRid1, rid1Str);
        assertEquals(expectedRid1.replaceAll("-", ""), String.valueOf(rid1.getRotationId(rid1Str)));
        assertEquals(expectedRid2, rid2Str);
        assertEquals(expectedRid2.replaceAll("-", ""), String.valueOf(rid2.getRotationId(rid2Str)));
        assertNotEquals(rid1Str, rid2Str);
    }

    @Test
    public void testCampaignId() throws InterruptedException {
        RotationId cid1 = RotationId.getNext();
        RotationId cid2 = RotationId.getNext();

        System.out.println(cid1);
        System.out.println(cid2);
        assertEquals(cid1.getDriverId(), cid2.getDriverId());
        assertTrue(cid2.getRepresentation() > cid1.getRepresentation());
    }
}


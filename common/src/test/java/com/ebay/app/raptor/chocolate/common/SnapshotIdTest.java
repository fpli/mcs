package com.ebay.app.raptor.chocolate.common;

import com.google.common.collect.Iterables;
import org.junit.Ignore;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

@SuppressWarnings("javadoc")
public class SnapshotIdTest {

    @Test
    public void testGenerationForTesting() {
        final long time = System.currentTimeMillis();
        final int driver = 1;

        // Equality check. There's no way to guarantee a particular
        // sequence value under current design in the "normal" API, deliberately
        {
            SnapshotId test = SnapshotId.generateForUnitTests(time, 0, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), time);
            assertEquals(test.getSequenceId(), 0l);
        }

        // Sequence check.
        final int sequenceId = 2300;
        {
            SnapshotId test = SnapshotId.generateForUnitTests(time, sequenceId, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), time);
            assertEquals(test.getSequenceId(), sequenceId);
        }

        // Time check.
        {
            long past = time - 100000l;
            SnapshotId test = SnapshotId.generateForUnitTests(past, sequenceId, driver);
            assertEquals(test.getDriverId(), driver);
            assertEquals(test.getTimeMillis(), past);
            assertEquals(test.getSequenceId(), sequenceId);
        }
        
        // Check for time. 
        {
            final long past = System.currentTimeMillis() - 300000;
            SnapshotId snapshot = SnapshotId.getNext(driver, past);
            assertEquals(0, snapshot.getSequenceId());
            assertEquals(driver, snapshot.getDriverId());
            assertEquals(past, snapshot.getTimeMillis());
        }
    }
    
    @Test
    public void testEarlyTime() {
        final long time = System.currentTimeMillis();
        final long oldTime = time - 1000000l;
        final int driverId = 373;
        final int seqId = 12;
        
        SnapshotId test = SnapshotId.generateForUnitTests(time, seqId, driverId);
        assertEquals(time, test.getTimeMillis());
        assertEquals(driverId, test.getDriverId());
        assertEquals(seqId, test.getSequenceId());
        
        // try and shovel in an older time. It shouldn't "revert" back. 
        SnapshotId next = new SnapshotId(test, driverId, oldTime);
        assertEquals(time, next.getTimeMillis());
        assertEquals(driverId, next.getDriverId());
        assertEquals(seqId+1, next.getSequenceId());
        
        // Create a new time. It should "advance" 
        final long newTime = time + 1000000l;
        SnapshotId future = new SnapshotId(next, driverId, newTime);
        assertEquals(newTime, future.getTimeMillis());
        assertEquals(driverId, future.getDriverId());
        assertEquals(0, future.getSequenceId());
    }

    @Test
    public void testZeroDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0;
        SnapshotId snapshot = new SnapshotId(driver);
        assertEquals(snapshot.getDriverId(), driver);
        assertEquals(0, snapshot.getSequenceId());
        assertTrue(snapshot.getTimeMillis() - time <= 1000l);

        // Increment
        SnapshotId snapshot2 = new SnapshotId(snapshot, driver);
        assertEquals(snapshot2.getDriverId(), driver);
        assertEquals(1, snapshot2.getSequenceId());
        assertEquals(snapshot2.getTimeMillis(), snapshot.getTimeMillis());
        Thread.sleep(1000);

        SnapshotId snapshot3 = new SnapshotId(snapshot2, driver);
        assertEquals(snapshot3.getDriverId(), driver);
        assertEquals(0, snapshot3.getSequenceId());
        assertNotEquals(snapshot3.getTimeMillis(), snapshot.getTimeMillis());
        System.out.println(snapshot3.toDebugString());
        System.out.println(snapshot.toDebugString());
    }

    @Test
    public void testMaxDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0x3FF;
        SnapshotId snapshot = new SnapshotId(driver);

        assertEquals(snapshot.getDriverId(), driver);
        assertEquals(0, snapshot.getSequenceId());
        assertTrue(snapshot.getTimeMillis() - time <= 1000l);

        // Increment
        SnapshotId snapshot2 = new SnapshotId(snapshot, driver);        
        assertEquals(snapshot2.getDriverId(), driver);
        boolean isIncrement = snapshot2.getSequenceId() == 1 && snapshot.getTimeMillis() == snapshot2.getTimeMillis();
        boolean isAfter = snapshot2.getSequenceId() == 0 && snapshot2.getTimeMillis() > snapshot.getTimeMillis();
        assertTrue(isIncrement || isAfter);
        Thread.sleep(1000);

        SnapshotId snapshot3 = new SnapshotId(snapshot2, driver);
        assertEquals(snapshot3.getDriverId(), driver);
        assertEquals(0, snapshot3.getSequenceId());
        assertNotEquals(snapshot3.getTimeMillis(), snapshot.getTimeMillis());
    }

    @Test
    @Ignore
    public void testIntermediateDriver() throws InterruptedException {
        final long time = System.currentTimeMillis();
        final int driver = 0x132;
        SnapshotId snapshot = new SnapshotId(driver);
        assertEquals(snapshot.getDriverId(), driver);
        assertEquals(0, snapshot.getSequenceId());
        assertTrue(snapshot.getTimeMillis() - time <= 1000l);

        // Increment
        SnapshotId snapshot2 = new SnapshotId(snapshot, driver);
        assertEquals(snapshot2.getDriverId(), driver);
        assertEquals(1, snapshot2.getSequenceId());
        assertEquals(snapshot2.getTimeMillis(), snapshot.getTimeMillis());
        Thread.sleep(1000);

        SnapshotId snapshot3 = new SnapshotId(snapshot2, driver);
        assertEquals(snapshot3.getDriverId(), driver);
        assertEquals(0, snapshot3.getSequenceId());
        assertNotEquals(snapshot3.getTimeMillis(), snapshot.getTimeMillis());
    }

    private static class SnapshotThreadTest implements Runnable {
        private final int driverId = 3;

        private final List<SnapshotId> snapshots = Collections
                .<SnapshotId>synchronizedList(new ArrayList<SnapshotId>());

        @Override
        public void run() {
            SnapshotId before = SnapshotId.getNext(driverId);
            snapshots.add(before);
            for (int i = 1; i < 1000; ++i) {
                SnapshotId current = SnapshotId.getNext(driverId);
                assertTrue(current.getRepresentation() > before.getRepresentation());
                assertEquals(current.getDriverId(), driverId);
                snapshots.add(current);
                before = current;
            }
        }

        public List<SnapshotId> getSnapshots() {
            List<SnapshotId> copy;
            synchronized (snapshots) {
                copy = new ArrayList<SnapshotId>(snapshots.size());
                copy.addAll(snapshots);
            }
            return copy;
        }
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        final int numThreads = 5;
        Thread [] t = new Thread [numThreads];
        SnapshotThreadTest [] runnables = new SnapshotThreadTest [numThreads];

        for (int i = 0; i < numThreads; ++i) {
            runnables[i] = new SnapshotThreadTest();
            t[i] = new Thread(runnables[i]);
        }

        for (int i = 0; i < numThreads; ++i)
            t[i].start();

        for (int i = 0; i < numThreads; ++i)
            t[i].join();

        // Assert no duplicates were made.
        Set<SnapshotId> allSnapshots = new HashSet<SnapshotId>(numThreads * 1000);
        for (SnapshotThreadTest runnable : runnables) {
            List<SnapshotId> snapshots = runnable.getSnapshots();
            allSnapshots.addAll(snapshots);
        }
        assertEquals(allSnapshots.size(), numThreads * 1000);
        assertTrue(SnapshotId.getCounter() >= numThreads * 1000);
        assertTrue(SnapshotId.checkAndClearCounter(numThreads * 1000l) >= numThreads * 1000l);
        assertEquals(0l, SnapshotId.getCounter());

        allSnapshots.add(new SnapshotId(0l));
        allSnapshots.add(new SnapshotId(-1l));
        List<SnapshotId> sorted = Arrays.<SnapshotId>asList(Iterables.toArray(allSnapshots, SnapshotId.class));
        Collections.sort(sorted);

        for (int i = 1; i < sorted.size(); ++i) {
            SnapshotId s1 = sorted.get(i - 1);
            SnapshotId s2 = sorted.get(i);
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
    public void testTimestampInSnapshotId(){
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
        SnapshotId snapshotId = new SnapshotId(1, c.getTimeInMillis());
        while(c.getTimeInMillis() == snapshotId.getTimeMillis()){
            if(c.getTimeInMillis() > max.getTimeInMillis()){
                break;
            }
            c.add(Calendar.HOUR, 1);
            snapshotId = new SnapshotId(1, c.getTimeInMillis());
        }
    
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        assertEquals("210905", sdf.format(c.getTime()));
        c.setTimeInMillis(snapshotId.getTimeMillis());
        assertEquals("197001", sdf.format(c.getTime()));
        assertEquals("211011", sdf.format(max.getTime()));
    }
}

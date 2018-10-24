package com.ebay.app.raptor.chocolate.common;

import com.google.common.collect.Iterables;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("javadoc")
public class ShortSnapshotIdTest {

  @Test
  public void testGenerationForTesting() {
    final long time = System.currentTimeMillis();
    final int driverId = 1;

    SnapshotId test = new SnapshotId(driverId, time);
    ShortSnapshotId shortSid = new ShortSnapshotId(test.getRepresentation());
    assertEquals(test.getDriverId(), shortSid.getDriverId());
    assertEquals(test.getTimeMillis(), shortSid.getTimeMillis());
    assertEquals(test.getSequenceId(), shortSid.getSequenceId());
  }

  @Test
  public void testMaxDriver() throws InterruptedException {
    final long time = System.currentTimeMillis();
    final int driver = 0x3FF;
    SnapshotId snapshot = new SnapshotId(driver);
    ShortSnapshotId shortSid = new ShortSnapshotId(snapshot.getRepresentation());

    assertTrue(snapshot.getTimeMillis() - time <= 1000l);
    assertEquals(snapshot.getTimeMillis(), shortSid.getTimeMillis());
    assertEquals(snapshot.getDriverId(), shortSid.getDriverId());
    assertEquals(snapshot.getSequenceId(), shortSid.getSequenceId());

    // Increment
    SnapshotId snapshot2 = new SnapshotId(snapshot, driver, time);
    assertEquals(snapshot2.getDriverId(), driver);
    boolean isIncrement = snapshot2.getSequenceId() == 1 && snapshot.getTimeMillis() == snapshot2.getTimeMillis();
    boolean isAfter = snapshot2.getSequenceId() == 0 && snapshot2.getTimeMillis() > snapshot.getTimeMillis();
    assertTrue(isIncrement || isAfter);
    ShortSnapshotId shortSid2 = new ShortSnapshotId(snapshot2.getRepresentation());
    assertTrue(snapshot.getTimeMillis() - time <= 1000l);
    assertEquals(snapshot2.getTimeMillis(), shortSid2.getTimeMillis());
    assertEquals(snapshot2.getDriverId(), shortSid2.getDriverId());
    assertEquals(snapshot2.getSequenceId(), shortSid2.getSequenceId());
    Thread.sleep(1000);

    SnapshotId snapshot3 = new SnapshotId(snapshot2, driver, time);
    ShortSnapshotId shortSid3 = new ShortSnapshotId(snapshot3.getRepresentation());
    assertEquals(snapshot3.getDriverId(), driver);
    assertEquals(2, snapshot3.getSequenceId());
    assertEquals(snapshot3.getTimeMillis(), snapshot.getTimeMillis());
    assertEquals(snapshot3.getTimeMillis(), shortSid3.getTimeMillis());
    assertEquals(snapshot3.getDriverId(), shortSid3.getDriverId());
    assertEquals(snapshot3.getSequenceId(), shortSid3.getSequenceId());
  }

  @Test
  public void testThreadSafety() throws InterruptedException {
    final int numThreads = 5;
    Thread[] t = new Thread[numThreads];
    SnapshotThreadTest[] runnables = new SnapshotThreadTest[numThreads];

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
    assertEquals(numThreads * 1000, allSnapshots.size());
    assertTrue(SnapshotId.getCounter() >= numThreads * 1000);
    assertTrue(SnapshotId.checkAndClearCounter(numThreads * 1000) >= numThreads * 1000);
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
  public void testLengthOfShortedSnapshotId() {
    SnapshotId snapshotId = new SnapshotId(5);
    ShortSnapshotId shortedSnapshotId = new ShortSnapshotId(snapshotId.getRepresentation());

    assertEquals(19, String.valueOf(snapshotId.getRepresentation()).length());
    assertEquals(18, String.valueOf(shortedSnapshotId.getRepresentation()).length());
  }

  private static class SnapshotThreadTest implements Runnable {
    private final int driverId = 3;

    private final List<SnapshotId> snapshots = Collections.<SnapshotId>synchronizedList(new ArrayList<SnapshotId>());

    @Override
    public void run() {
      SnapshotId before = SnapshotId.getNext(driverId);
      snapshots.add(before);
      SnapshotId current = null;
      ShortSnapshotId beforeShort = new ShortSnapshotId(before.getRepresentation());
      ShortSnapshotId currentShort = null;
      for (int i = 1; i < 1000; ++i) {
        current = SnapshotId.getNext(driverId);
        try {
          Thread.sleep(0, 100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        assertTrue(current.getRepresentation() > before.getRepresentation());
        assertEquals(driverId, current.getDriverId());
        snapshots.add(current);
        currentShort = new ShortSnapshotId(current.getRepresentation());
        assertEquals(current.getTimeMillis(), currentShort.getTimeMillis());
        assertEquals(current.getDriverId(), currentShort.getDriverId());
        assertEquals(current.getSequenceId(), currentShort.getSequenceId());
        assertTrue(currentShort.getRepresentation() > beforeShort.getRepresentation());
        assertEquals(18, String.valueOf(currentShort.getRepresentation()).length());
        assertEquals(current.getRepresentation(), currentShort.getOriginalRepresentation());
        before = current;
        beforeShort = currentShort;
      }
    }

    public List<SnapshotId> getSnapshots() {
      return snapshots;
    }
  }
}

package com.ebay.app.raptor.chocolate.common;

import com.google.common.collect.Iterables;
import org.junit.Ignore;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

@SuppressWarnings("javadoc")
public class ShortedSnapshotIdTest {

  @Test
  public void testGenerationForTesting() {
    final long time = System.currentTimeMillis();
    final int driverId = 1;

    {
      ShortedSnapshotId test = new ShortedSnapshotId(time, driverId, 0);
      assertEquals(driverId, test.getDriverId());
      assertEquals(time, test.getTimeMillis());
      assertEquals(0l, test.getSequenceId());
    }

    // Sequence check.
    final int sequenceId = 511;
    {
      ShortedSnapshotId test = new ShortedSnapshotId(time, driverId, sequenceId);
      assertEquals(driverId, test.getDriverId());
      assertEquals(time, test.getTimeMillis());
      assertEquals(sequenceId, test.getSequenceId());
    }

    // Time check.
    {
      long past = time - 100000l;
      ShortedSnapshotId test = new ShortedSnapshotId(past, sequenceId, sequenceId);
      assertEquals(sequenceId, test.getDriverId());
      assertEquals(past, test.getTimeMillis());
      assertEquals(sequenceId, test.getSequenceId());
    }
  }

  @Test
  public void testEarlyTime() {
    final long time = System.currentTimeMillis();
    final long oldTime = time - 1000000l;
    final int driverId = 373;
    final int seqId = 12;

    ShortedSnapshotId test = new ShortedSnapshotId(time, driverId, seqId);
    assertEquals(time, test.getTimeMillis());
    assertEquals(driverId, test.getDriverId());
    assertEquals(seqId, test.getSequenceId());

    // try and shovel in an older time. It shouldn't "revert" back.
    ShortedSnapshotId next = new ShortedSnapshotId(test, driverId, oldTime);
    assertEquals(time, next.getTimeMillis());
    assertEquals(driverId, next.getDriverId());
    assertEquals(seqId + 1, next.getSequenceId());

    // Create a new time. It should "advance"
    final long newTime = time + 1000000l;
    ShortedSnapshotId future = new ShortedSnapshotId(next, driverId, newTime);
    assertEquals(newTime, future.getTimeMillis());
    assertEquals(driverId, future.getDriverId());
    assertEquals(0, future.getSequenceId());
  }

  @Test
  public void testZeroDriver() throws InterruptedException {
    final long time = System.currentTimeMillis();
    final int driver = 0;
    ShortedSnapshotId snapshot = new ShortedSnapshotId(driver);
    assertEquals(snapshot.getDriverId(), driver);
    assertEquals(0, snapshot.getSequenceId());
    assertTrue(snapshot.getTimeMillis() - time <= 1000l);

    // Increment
    ShortedSnapshotId snapshot2 = new ShortedSnapshotId(snapshot, driver);
    assertEquals(snapshot2.getDriverId(), driver);
    assertEquals(1, snapshot2.getSequenceId());
    assertEquals(snapshot2.getTimeMillis(), snapshot.getTimeMillis());
    Thread.sleep(1000);

    ShortedSnapshotId snapshot3 = new ShortedSnapshotId(snapshot2, driver);
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
    ShortedSnapshotId snapshot = new ShortedSnapshotId(driver);

    assertEquals(snapshot.getDriverId(), driver);
    assertEquals(0, snapshot.getSequenceId());
    assertTrue(snapshot.getTimeMillis() - time <= 1000l);

    // Increment
    ShortedSnapshotId snapshot2 = new ShortedSnapshotId(snapshot, driver);
    assertEquals(snapshot2.getDriverId(), driver);
    boolean isIncrement = snapshot2.getSequenceId() == 1 && snapshot.getTimeMillis() == snapshot2.getTimeMillis();
    boolean isAfter = snapshot2.getSequenceId() == 0 && snapshot2.getTimeMillis() > snapshot.getTimeMillis();
    assertTrue(isIncrement || isAfter);
    Thread.sleep(1000);

    ShortedSnapshotId snapshot3 = new ShortedSnapshotId(snapshot2, driver);
    assertEquals(snapshot3.getDriverId(), driver);
    assertEquals(0, snapshot3.getSequenceId());
    assertNotEquals(snapshot3.getTimeMillis(), snapshot.getTimeMillis());
  }

  @Ignore
  public void testIntermediateDriver() throws InterruptedException {
    final long time = System.currentTimeMillis();
    final int driver = 0x132;
    ShortedSnapshotId snapshot = new ShortedSnapshotId(driver);
    assertEquals(snapshot.getDriverId(), driver);
    assertEquals(0, snapshot.getSequenceId());
    assertTrue(snapshot.getTimeMillis() - time <= 1000l);

    // Increment
    ShortedSnapshotId snapshot2 = new ShortedSnapshotId(snapshot, driver);
    assertEquals(snapshot2.getDriverId(), driver);
    assertEquals(1, snapshot2.getSequenceId());
    assertEquals(snapshot2.getTimeMillis(), snapshot.getTimeMillis());
    Thread.sleep(1000);

    ShortedSnapshotId snapshot3 = new ShortedSnapshotId(snapshot2, driver);
    assertEquals(snapshot3.getDriverId(), driver);
    assertEquals(0, snapshot3.getSequenceId());
    assertNotEquals(snapshot3.getTimeMillis(), snapshot.getTimeMillis());
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
    Set<ShortedSnapshotId> allSnapshots = new HashSet<ShortedSnapshotId>(numThreads * 1000);
    for (SnapshotThreadTest runnable : runnables) {
      List<ShortedSnapshotId> snapshots = runnable.getSnapshots();
      allSnapshots.addAll(snapshots);
    }
    assertEquals(numThreads * 200, allSnapshots.size());
    assertTrue(ShortedSnapshotId.getCounter() >= numThreads * 200);
    assertTrue(ShortedSnapshotId.checkAndClearCounter(numThreads * 200l) >= numThreads * 200l);
    assertEquals(0l, ShortedSnapshotId.getCounter());

    allSnapshots.add(new ShortedSnapshotId(0l));
    allSnapshots.add(new ShortedSnapshotId(-1l));
    List<ShortedSnapshotId> sorted = Arrays.<ShortedSnapshotId>asList(Iterables.toArray(allSnapshots, ShortedSnapshotId.class));
    Collections.sort(sorted);

    for (int i = 1; i < sorted.size(); ++i) {
      ShortedSnapshotId s1 = sorted.get(i - 1);
      ShortedSnapshotId s2 = sorted.get(i);
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
  public void testTimestampInShortedSnapshotId() {
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
    ShortedSnapshotId ShortedSnapshotId = new ShortedSnapshotId(1, c.getTimeInMillis());
    while (c.getTimeInMillis() == ShortedSnapshotId.getTimeMillis()) {
      if (c.getTimeInMillis() > max.getTimeInMillis()) {
        break;
      }
      c.add(Calendar.HOUR, 1);
      ShortedSnapshotId = new ShortedSnapshotId(1, c.getTimeInMillis());
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
    assertEquals("210905", sdf.format(c.getTime()));
    c.setTimeInMillis(ShortedSnapshotId.getTimeMillis());
    assertEquals("197001", sdf.format(c.getTime()));
    assertEquals("211011", sdf.format(max.getTime()));
  }

  @Test
  public void testLengthOfShortedSnapshotId() {
    SnapshotId snapshotId = new SnapshotId(5);
    ShortedSnapshotId shortedSnapshotId = new ShortedSnapshotId(snapshotId.getRepresentation());

    assertEquals(19, String.valueOf(snapshotId.getRepresentation()).length());
    assertEquals(18, String.valueOf(shortedSnapshotId.getRepresentation()).length());
  }

  private static class SnapshotThreadTest implements Runnable {
    private final int driverId = 3;

    private final List<ShortedSnapshotId> snapshots = Collections
        .<ShortedSnapshotId>synchronizedList(new ArrayList<ShortedSnapshotId>());

    @Override
    public void run() {
      ShortedSnapshotId before = ShortedSnapshotId.getNext(driverId);
      snapshots.add(before);
      for (int i = 1; i < 200; ++i) {
        ShortedSnapshotId current = ShortedSnapshotId.getNext(driverId);
        assertTrue(current.getRepresentation() > before.getRepresentation());
        assertEquals(driverId, current.getDriverId());
        snapshots.add(current);
        before = current;
      }
    }

    public List<ShortedSnapshotId> getSnapshots() {
      List<ShortedSnapshotId> copy;
      synchronized (snapshots) {
        copy = new ArrayList<ShortedSnapshotId>(snapshots.size());
        copy.addAll(snapshots);
      }
      return copy;
    }
  }
}

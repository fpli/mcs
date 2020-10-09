package com.ebay.app.raptor.chocolate.jdbc.repo;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import junit.framework.TestCase;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@SpringBootTest(classes = EventListenerApplication.class)
@FixMethodOrder()
public class DriverIdServiceImplTest extends TestCase {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Resource
  private DriverIdRepository driverIdRepository;

  @Autowired
  private DriverIdServiceImpl driverIdService;

  @Test
  public void test() {
    driverIdRepository.deleteAll();
    driverIdRepository.flush();
    assertEquals(0, driverIdService.getDriverId("test-host-0", "test-ip-0", 2, 1));
    assertEquals(0, driverIdService.getDriverId("test-host-0", "test-ip-0", 2, 1));

    driverIdRepository.deleteAll();
    driverIdRepository.flush();
  }

  @Test
  public void testIllegalArgumentException() {
    driverIdRepository.deleteAll();
    driverIdRepository.flush();

    assertEquals(-1, driverIdService.getDriverId("test-host-1", "test-ip-1", 2, 0));

    driverIdRepository.deleteAll();
    driverIdRepository.flush();
  }

  @Test
  public void testIllegalArgumentException1() {
    driverIdRepository.deleteAll();
    driverIdRepository.flush();

    for (int i = 0; i <= 2; i++) {
      driverIdService.getDriverId("test-host-" + i, "test-ip-" + i, 2, 1);
    }

    assertEquals(-1, driverIdService.getDriverId("test-host-3", "test-ip-3", 2, 1));

    driverIdRepository.deleteAll();
    driverIdRepository.flush();
  }

  @Test
  public void testConcurrent() {
    driverIdRepository.deleteAll();
    driverIdRepository.flush();

    Set<Integer> actualDriverIdSet = new HashSet<>();

    ExecutorService executors = Executors.newFixedThreadPool(10);
    try {

      List<Future<Integer>> list = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        list.add(executors.submit(new MyRunnable(i)));
      }

      for (Future<Integer> future : list) {
        Integer driverId = future.get();
        actualDriverIdSet.add(driverId);
      }

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      executors.shutdown();
    }

    Set<Integer> expectedDriverIdSet = new HashSet<>();
    expectedDriverIdSet.add(0);
    expectedDriverIdSet.add(1);
    expectedDriverIdSet.add(2);
    expectedDriverIdSet.add(3);
    expectedDriverIdSet.add(4);

    assertEquals(expectedDriverIdSet, actualDriverIdSet);

    driverIdRepository.deleteAll();
    driverIdRepository.flush();
  }

  private class MyRunnable implements Callable<Integer> {
    private final int index;

    public MyRunnable(int index) {
      this.index = index;
    }

    @Override
    public Integer call() {
      return driverIdService.getDriverId("test-host-" + index, "test-ip-" + index, 100, 10);
    }
  }
}
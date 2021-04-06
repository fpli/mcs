package com.ebay.traffic.chocolate.flink.nrt.provider.ersxid;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ErsXidServiceTest {
  private ErsXidService ersxidService;

  @Before
  public void setUp() throws Exception {
    Properties properties = PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
    SherlockioMetrics.init(properties.getProperty(PropertyConstants.SHERLOCKIO_NAMESPACE),
            properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT), properties.getProperty(PropertyConstants.SHERLOCKIO_USER));

    ersxidService = ErsXidService.getInstance();
  }

  @Test
  public void getAccountId() throws ExecutionException, InterruptedException {
    Future<Response> call = ersxidService.call("8fd4097b16c0ada5af056046ffffa47a");
    Response response = call.get();
    assertNotNull(response);
  }

}
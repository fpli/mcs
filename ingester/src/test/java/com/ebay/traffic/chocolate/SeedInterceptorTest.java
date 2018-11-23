package com.ebay.traffic.chocolate;

import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SeedInterceptorTest {

  @Test
  public void testGetUserId() {
    String message = "{\"event\":101,\"uuid\":\"af10ee94-97f0-474c-8c55-6e48601b0c21\",\"site\":\"0\",\"items\":[{\"site\":\"0\",\"meta\":\"11450\",\"leaf\":\"57990\",\"itemId\":\"264026670073\",\"l2\":\"1059\"}],\"time\":1541700632970,\"user\":{\"data\":\"{\\\"matchedUsers\\\":1}\",\"seg\":0,\"fm\":\"11\",\"deviceType\":\"desktop\",\"userId\":\"298083745\",\"deviceId\":\"f4557c741660ada1cbb23c96edea84c3\"}}";

    byte[] messageBytes = message.getBytes();
    JsonObject mesObj = new SeedInterceptor().getBody(messageBytes);
    assertEquals("1541700632970", mesObj.get("time").getAsString());
    assertEquals("298083745", mesObj.getAsJsonObject("user").get("userId").getAsString());
  }
}

/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.adservice.component;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

import javax.ws.rs.client.Client;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class FideliusClientTest {

  private boolean initialized = false;
  private Client client;
  private String svcEndPoint;

  @LocalServerPort
  private int port;

  @Autowired
  FideliusClient fideliusClient;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test(expected = RuntimeException.class)
  public void create() {
    fideliusClient.create("UT", "Test");
    assertEquals(fideliusClient.getContent("UT"), "Test");
    fideliusClient.delete("UT");
    assertEquals(fideliusClient.getContent("UT"), "");
  }
}
package com.ebay.app.raptor.chocolate.adservice.component;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EsrXidClientTest {
    @Autowired
    private EsrXidClient esrXidClient;

    @Test
    public void testGetUserId() {
        String userIdByGuid = esrXidClient.getUserIdByGuid("fc77ec531730a9c42defc807ffffea20");
        assert userIdByGuid.equals("");
    }
}

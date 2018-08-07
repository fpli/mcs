package com.ebay.app.raptor.chocolate.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author jepounds
 */
public class EnvironmentTest {
    @SuppressWarnings("javadoc")
    @Test
    public void testEnvironments() {
        assertEquals(Environment.DEV, Environment.fromString("dev"));
        assertEquals(Environment.DEV, Environment.fromString("DeVeLoPer"));

        assertEquals(Environment.PROD, Environment.fromString("PROD"));
        assertEquals(Environment.PROD, Environment.fromString("prod"));
        assertEquals(Environment.PROD, Environment.fromString("production"));

        assertEquals(Environment.QE, Environment.fromString("QE"));
        assertEquals(Environment.QE, Environment.fromString("qa"));

        assertEquals(Environment.STAGING, Environment.fromString("stage"));
        assertEquals(Environment.STAGING, Environment.fromString("Staging"));

        assertEquals(Environment.TEST, Environment.fromString("test"));
        assertEquals(Environment.TEST, Environment.fromString("TeSt"));

        assertNull(Environment.fromString(null));
        assertNull(Environment.fromString("nonexistent"));
        assertNull(Environment.fromString(""));
        assertNull(Environment.fromString("produ"));
        assertNull(Environment.fromString("pro"));
    }
}

package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author jepounds
 */
public class HttpMethodEnumTest {

    @Test
    public void testHttpMethodEnum() {
        assertEquals("CONNECT", HttpMethodEnum.CONNECT.getValue());
        assertEquals(HttpMethod.CONNECT, HttpMethodEnum.CONNECT.getAvro());
        assertEquals(HttpMethodEnum.CONNECT, HttpMethodEnum.parse(HttpMethodEnum.CONNECT.getValue()));
        assertEquals(HttpMethodEnum.CONNECT, HttpMethodEnum.parse(HttpMethodEnum.CONNECT.getValue().toLowerCase()));
        
        assertEquals("DELETE", HttpMethodEnum.DELETE.getValue());
        assertEquals(HttpMethod.DELETE, HttpMethodEnum.DELETE.getAvro());
        assertEquals(HttpMethodEnum.DELETE, HttpMethodEnum.parse(HttpMethodEnum.DELETE.getValue()));
        assertEquals(HttpMethodEnum.DELETE, HttpMethodEnum.parse(HttpMethodEnum.DELETE.getValue().toLowerCase()));
            
        assertEquals("GET", HttpMethodEnum.GET.getValue());
        assertEquals(HttpMethod.GET, HttpMethodEnum.GET.getAvro());
        assertEquals(HttpMethodEnum.GET, HttpMethodEnum.parse(HttpMethodEnum.GET.getValue()));
        assertEquals(HttpMethodEnum.GET, HttpMethodEnum.parse(HttpMethodEnum.GET.getValue().toLowerCase()));
        
        assertEquals("HEAD", HttpMethodEnum.HEAD.getValue());
        assertEquals(HttpMethod.HEAD, HttpMethodEnum.HEAD.getAvro());
        assertEquals(HttpMethodEnum.HEAD, HttpMethodEnum.parse(HttpMethodEnum.HEAD.getValue()));
        assertEquals(HttpMethodEnum.HEAD, HttpMethodEnum.parse(HttpMethodEnum.HEAD.getValue().toLowerCase()));
        
        assertEquals("OPTIONS", HttpMethodEnum.OPTIONS.getValue());
        assertEquals(HttpMethod.OPTIONS, HttpMethodEnum.OPTIONS.getAvro());
        assertEquals(HttpMethodEnum.OPTIONS, HttpMethodEnum.parse(HttpMethodEnum.OPTIONS.getValue()));
        assertEquals(HttpMethodEnum.OPTIONS, HttpMethodEnum.parse(HttpMethodEnum.OPTIONS.getValue().toLowerCase()));
        
        assertEquals("PATCH", HttpMethodEnum.PATCH.getValue());
        assertEquals(HttpMethod.PATCH, HttpMethodEnum.PATCH.getAvro());
        assertEquals(HttpMethodEnum.PATCH, HttpMethodEnum.parse(HttpMethodEnum.PATCH.getValue()));
        assertEquals(HttpMethodEnum.PATCH, HttpMethodEnum.parse(HttpMethodEnum.PATCH.getValue().toLowerCase()));
        
        assertEquals("PUT", HttpMethodEnum.PUT.getValue());
        assertEquals(HttpMethod.PUT, HttpMethodEnum.PUT.getAvro());
        assertEquals(HttpMethodEnum.PUT, HttpMethodEnum.parse(HttpMethodEnum.PUT.getValue()));
        assertEquals(HttpMethodEnum.PUT, HttpMethodEnum.parse(HttpMethodEnum.PUT.getValue().toLowerCase()));
        
        assertEquals("POST", HttpMethodEnum.POST.getValue());
        assertEquals(HttpMethod.POST, HttpMethodEnum.POST.getAvro());
        assertEquals(HttpMethodEnum.POST, HttpMethodEnum.parse(HttpMethodEnum.POST.getValue()));
        assertEquals(HttpMethodEnum.POST, HttpMethodEnum.parse(HttpMethodEnum.POST.getValue().toLowerCase()));
        
        assertEquals("TRACE", HttpMethodEnum.TRACE.getValue());
        assertEquals(HttpMethod.TRACE, HttpMethodEnum.TRACE.getAvro());
        assertEquals(HttpMethodEnum.TRACE, HttpMethodEnum.parse(HttpMethodEnum.TRACE.getValue()));
        assertEquals(HttpMethodEnum.TRACE, HttpMethodEnum.parse(HttpMethodEnum.TRACE.getValue().toLowerCase()));

        assertNull(HttpMethodEnum.parse(null));
        assertNull(HttpMethodEnum.parse(""));
        assertNull(HttpMethodEnum.parse("hea"));
        assertNull(HttpMethodEnum.parse("postget"));
    }
}

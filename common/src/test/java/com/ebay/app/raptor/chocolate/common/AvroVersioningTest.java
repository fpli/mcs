package com.ebay.app.raptor.chocolate.common;

import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1;
import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV1;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Sanity test to make sure that schema versioning works.
 * Is supposed to break when schema is changed, and needs to be fixed to verify that previous version of the messages
 * can still be read by the new version of the schema
 */
public class AvroVersioningTest {
    @Test
    public void writeReadListenerMessage() throws IOException {
        ListenerMessage om = new ListenerMessage();
        om.setSnapshotId(1234L);
        om.setUserId("");
        om.setCguid("");
        om.setGuid("");
        om.setClientRemoteIp("");
        om.setReferer("");
        om.setRequestHeaders("");
        om.setResponseHeaders("");
        om.setUri("");
        om.setChannelAction(ChannelAction.CLICK);
        om.setHttpMethod(HttpMethod.GET);
        om.setChannelType(ChannelType.EPN);
        om.setSnid("foo");

        String omjson = om.writeToJSON();

        ListenerMessage tm2 = ListenerMessage.readFromJSON(omjson);
        assertEquals(1234L, tm2.getSnapshotId().longValue());
        assertEquals("foo", tm2.getSnid());
        assertEquals(ChannelType.EPN, tm2.getChannelType());
        assertEquals(ChannelAction.CLICK, tm2.getChannelAction());
    }

    @Test
    public void readOldLietenerMessageSchema() throws IOException {
        ListenerMessageV1 om = new ListenerMessageV1();
        om.setSnapshotId(321L);
        om.setRequestHeaders("");
        om.setResponseHeaders("");
        om.setUri("");
        om.setChannelAction(ChannelAction.CLICK);
        om.setHttpMethod(HttpMethod.GET);
        om.setChannelType(ChannelType.EPN);
        om.setSnid("foo");

        DatumWriter<ListenerMessageV1> writer = new SpecificDatumWriter<>(ListenerMessageV1.getClassSchema());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(ListenerMessageV1.getClassSchema(), out);
        writer.write(om, encoder);
        encoder.flush();
        String json = out.toString();

        ListenerMessage tm = ListenerMessage.readFromJSON(json);
        assertEquals(321L, tm.getSnapshotId().longValue());
        assertEquals(ChannelType.EPN, tm.getChannelType());
        assertEquals(ChannelAction.CLICK, tm.getChannelAction());
    }

    @Test
    public void writeReadFilterMessage() throws IOException {
        FilterMessage om = new FilterMessage();
        om.setSnapshotId(1234L);
        om.setUserId("");
        om.setClientRemoteIp("");
        om.setGuid("");
        om.setCguid("");
        om.setReferer("");
        om.setRequestHeaders("");
        om.setResponseHeaders("");
        om.setUri("");
        om.setChannelAction(ChannelAction.CLICK);
        om.setHttpMethod(HttpMethod.GET);
        om.setChannelType(ChannelType.EPN);
        om.setSnid("foo");

        String omjson = om.writeToJSON();

        FilterMessage tm2 = FilterMessage.readFromJSON(omjson);
        assertEquals(1234L, tm2.getSnapshotId().longValue());
        assertEquals("foo", tm2.getSnid());
        assertEquals(ChannelType.EPN, tm2.getChannelType());
        assertEquals(ChannelAction.CLICK, tm2.getChannelAction());
    }

    @Test
    public void readOldFilterMessageSchema() throws IOException {
        FilterMessageV1 om = new FilterMessageV1();
        om.setSnapshotId(321L);
        om.setRequestHeaders("");
        om.setResponseHeaders("");
        om.setUri("");
        om.setChannelAction(ChannelAction.CLICK);
        om.setHttpMethod(HttpMethod.GET);
        om.setChannelType(ChannelType.EPN);
        om.setSnid("foo");

        DatumWriter<FilterMessageV1> writer = new SpecificDatumWriter<>(FilterMessageV1.getClassSchema());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(FilterMessageV1.getClassSchema(), out);
        writer.write(om, encoder);
        encoder.flush();
        String json = out.toString();

        FilterMessage tm = FilterMessage.readFromJSON(json);
        assertEquals(321L, tm.getSnapshotId().longValue());
        assertEquals(ChannelType.EPN, tm.getChannelType());
        assertEquals(ChannelAction.CLICK, tm.getChannelAction());
    }
}

package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransformerFactoryTest {
  private FilterMessage filterMessage;

  @Before
  public void setUp() throws Exception {
    filterMessage = new FilterMessage();
  }

  @Test
  public void getConcreteTransformer() {
    assertEquals("BaseTransformer", TransformerFactory.getConcreteTransformer(filterMessage).getClass().getSimpleName());

    filterMessage.setChannelType(ChannelType.DISPLAY);
    assertEquals("DisplayTransformer", TransformerFactory.getConcreteTransformer(filterMessage).getClass().getSimpleName());

    filterMessage.setChannelType(ChannelType.ROI);
    assertEquals("RoiTransformer", TransformerFactory.getConcreteTransformer(filterMessage).getClass().getSimpleName());

    filterMessage.setChannelType(ChannelType.PAID_SEARCH);
    assertEquals("BaseTransformer", TransformerFactory.getConcreteTransformer(filterMessage).getClass().getSimpleName());

  }
}
package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.traffic.chocolate.cappingrules.dto.EventSchema;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperEvent;
import com.ebay.traffic.chocolate.cappingrules.hdfs.HDFSFileGenerator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TestHDFSFileGenerator extends AbstractCappingRuleTest {
  private static String stopTime;

  @BeforeClass
  public static void initialHbaseTable() throws IOException {
    setDataIntoTransactionTable();

    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Assert.assertEquals(3, getCount(iter));
    iter.close();
    
    Calendar c = Calendar.getInstance();
    stopTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
  }
  
  @Test
  public void testHDFSFileGenerator() throws Exception {
    String filePath = "/Users/yimeng/Downloads/chocolate-";
    HDFSFileGenerator job = new HDFSFileGenerator("TestHDFSFileGenerator", "local[4]", TRANSACTION_TABLE_NAME,
        "EPN", stopTime, 24 * 60, filePath, 1);
    job.run();

    Dataset<EventSchema> dr = job.readFromParquet(filePath);
    Assert.assertEquals(2, dr.count());
    Assert.assertEquals(new Long(98765432101l), dr.first().getCampaignId());

    job.stop();
  }


  
  protected static void setDataIntoTransactionTable() throws IOException {
    
    Calendar c = Calendar.getInstance();
    c.add(Calendar.HOUR, -2);

    SnapshotId snapshotId1 = new SnapshotId(0, c.getTimeInMillis());
    // click happens after impression on same host and different host
    addEvent(transactionalTable, new EventSchema(generateIdentifier(snapshotId1), snapshotId1.getRepresentation(),
        98765432101l, 12345678901l,  "IMPRESSION", "EPN", "request-header-1", c.getTimeInMillis(),  "snid-1",
        IdentifierUtil.getMonthFromSnapshotId(snapshotId1.getRepresentation()), true, false, "IAB_BOT_BLACK_LIST",   null, null,
        false,"IP_CAPPING_RULE"));

    c.add(Calendar.MINUTE, 1);
    SnapshotId snapshotId2 = new SnapshotId(0, c.getTimeInMillis());
    addEvent(transactionalTable, new EventSchema(generateIdentifier(snapshotId2), snapshotId2.getRepresentation(),
        98765432102l, 12345678902l,  "CLICK", "EPN", "request-header-2", c.getTimeInMillis(),  "snid-1",
        IdentifierUtil.getMonthFromSnapshotId(snapshotId2.getRepresentation()), true, true, null,   true, generateIdentifier(snapshotId1),
        true,null));

    c.add(Calendar.MINUTE, 1);
    SnapshotId snapshotId3 = new SnapshotId(0, c.getTimeInMillis());
    addEvent(transactionalTable, new EventSchema(generateIdentifier(snapshotId3), snapshotId3.getRepresentation(),
        98765432102l, 12345678902l,  "CLICK", "DAP", "request-header-2", c.getTimeInMillis(),  "snid-1",
        IdentifierUtil.getMonthFromSnapshotId(snapshotId3.getRepresentation()), true, true, null,   false, null,
        true,null));
  }



  /**
   * Generate 10 bytes row key -- For Testing
   *
   * @return row key byte array
   */
  public static byte[] generateIdentifier(SnapshotId snapshotId) throws IOException {
    return IdentifierUtil.generateIdentifier(snapshotId.getTimeMillis(), 0, (short)1);
  }
  
  private static void addEvent(HTable table, EventSchema eventSchema) throws IOException {
    Put put = new Put(eventSchema.getRowIdentifier());

    putCell(put, TRANSACTION_CF_DEFAULT, "snapshot_id", eventSchema.getSnapshotId());
    putCell(put, TRANSACTION_CF_DEFAULT, "campaign_id", eventSchema.getCampaignId());
    putCell(put, TRANSACTION_CF_DEFAULT, "partner_id", eventSchema.getPartnerId());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", eventSchema.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_type", eventSchema.getChannelType());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", eventSchema.getRequestHeaders());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_timestamp", eventSchema.getRequestTimestamp());
    putCell(put, TRANSACTION_CF_DEFAULT, "http_method", eventSchema.getHttpMethod());
    putCell(put, TRANSACTION_CF_DEFAULT, "snid", eventSchema.getSnid());
    putCell(put, TRANSACTION_CF_DEFAULT, "month", String.valueOf(eventSchema.getMonth()));
    putCell(put, TRANSACTION_CF_DEFAULT, "is_mobile", eventSchema.getMobile());
    putCell(put, TRANSACTION_CF_DEFAULT, "filter_passed", eventSchema.getFilterPassed());
    putCell(put, TRANSACTION_CF_DEFAULT, "filter_failed_rule", eventSchema.getFilterFailedRule());
    if(eventSchema.getImpressed() != null){
      putCell(put, TRANSACTION_CF_DEFAULT, "is_impressed", eventSchema.getImpressed());
    }
    putCell(put, TRANSACTION_CF_DEFAULT, "imp_row_key", eventSchema.getImpRowIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "capping_passed", eventSchema.getCappingPassed());
    putCell(put, TRANSACTION_CF_DEFAULT, "capping_failed_rule", eventSchema.getCappingFailedRule());
    table.put(put);
  }
}

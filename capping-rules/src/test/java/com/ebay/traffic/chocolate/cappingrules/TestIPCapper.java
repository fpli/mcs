package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.cappingrules.Rules.IPCapper;
import com.ebay.traffic.chocolate.cappingrules.dto.IPCapperEvent;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Unit test for IPCAppingRuleJob
 *
 * @author xiangli4
 */

public class TestIPCapper extends AbstractCappingRuleTest {
  
  @BeforeClass
  public static void initialHbaseTables() throws Exception {
    setDataIntoTransactionTable();
  }
  
  @Test
  public void testHBaseScanIterator() throws Exception {
    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Assert.assertEquals(getCount(iter), 37);
    iter.close();
  }
  
  @Test
  public void testParseOptions() throws Exception {
    String[] args = {"--jobName", "IPCappingRule", "--mode", "yarn", "--originalTable", "prod_transactional",
        "--resultTable", "capping_result", "--channelType", "EPN",
        "--scanStopTime", "2017-10-12 00:00:00",
        "--scanTimeWindow", "24",
        "--updateTimeWindow", "15",
        "--threshold", "1000"};
    CommandLine cmd = IPCapper.parseOptions(args);
    Assert.assertEquals("IPCappingRule", cmd.getOptionValue("jobName"));
    Assert.assertEquals("yarn", cmd.getOptionValue("mode"));
    Assert.assertEquals("prod_transactional", cmd.getOptionValue("originalTable"));
    Assert.assertEquals("capping_result", cmd.getOptionValue("resultTable"));
    Assert.assertEquals("EPN", cmd.getOptionValue("channelType"));
    Assert.assertEquals("2017-10-12 00:00:00", cmd.getOptionValue("scanStopTime"));
    Assert.assertEquals("24", cmd.getOptionValue("scanTimeWindow"));
    Assert.assertEquals("15", cmd.getOptionValue("updateTimeWindow"));
    Assert.assertEquals("1000", cmd.getOptionValue("threshold"));
  }
  
  @Test
  public void testIPCappingRuleJob() throws Exception {
    Calendar today = Calendar.getInstance();
    String stopTime = IdentifierUtil.INPUT_DATE_FORMAT.format(today.getTime());
    today.add(Calendar.DATE, -1);

    IPCapper job = new IPCapper("TestIPCappingRuleJob", "local[4]", TRANSACTION_TABLE_NAME, RESULT_TABLE_NAME,
        "EPN", stopTime, 24*60, 4*60, 8);
  
    //IPCappingRuleJob.setMod((short) 3);
    job.run();
    HBaseScanIterator resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME);
    Assert.assertEquals(16, getCount(resultTableItr));
    resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME);
    Assert.assertEquals(10, getFailedCount(resultTableItr));
    job.stop();
  }
  
  @Test
  public void testGetTimestampFromIdentifier() throws IOException {
    long time = 1512527672000l;
    byte[] identifier = IdentifierUtil.generateIdentifier(time, (short) 3);
    long timestamp = IdentifierUtil.getTimeMillisFromRowkey(identifier);
    Assert.assertEquals(time, timestamp);
  }
  
  protected int getFailedCount(HBaseScanIterator iter) {
    int count = 0;
    while (iter.hasNext()) {
      Result result = iter.next();
      String failedRule = Bytes.toString(result.getValue(Bytes.toBytes("x"), Bytes.toBytes("capping_failed_rule")));
      if (failedRule.equals("IPCappingRule")) {
        count++;
      }
    }
    return count;
  }
  
  
  
  private static byte[] generateIdentity(int hour, int minute, short modValue) throws IOException {
    Calendar yesterday = Calendar.getInstance();
    yesterday.add(Calendar.DATE, - 1);
    yesterday.add(Calendar.HOUR, hour);
    yesterday.add(Calendar.MINUTE, minute);
    return IdentifierUtil.generateIdentifier(yesterday.getTimeInMillis(), modValue);
  }
  
  private static void setDataIntoTransactionTable() throws IOException {
    
    String requestHeaderValid = "Cookie: aaa ;|X-eBay-Client-IP: 50.206.232.22|Connection: keep-alive|User-Agent: " +
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 " +
        "Safari/537.36";
    String requestHeaderInvalid = "Cookie: aaa ;|X-eBay-Client-IP: 11.11.11.11|Connection: keep-alive|User-Agent: " +
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 " +
        "Safari/537.36";
    
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 0, (short) 0), "IMPRESSION", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 1, (short) 0), "IMPRESSION", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 2, (short) 0), "IMPRESSION", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 3, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 4, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 5, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 6, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 7, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 8, (short) 0), "CLICK", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 9, (short) 0), "CLICK", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 10, (short) 0), "CLICK", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 11, (short) 0), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 12, (short) 0), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 13, (short) 0), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 14, (short) 0), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(24, 1, (short) 0), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(19, 0, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(19, 1, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(19, 2, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(19, 3, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(19, 4, (short) 0), "CLICK", requestHeaderInvalid, "EPN"));
    
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 0, (short) 1), "IMPRESSION", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 1, (short) 1), "IMPRESSION", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 2, (short) 1), "IMPRESSION", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 3, (short) 1), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 4, (short) 1), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 5, (short) 1), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 6, (short) 1), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 7, (short) 1), "CLICK", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 8, (short) 1), "CLICK", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 9, (short) 1), "CLICK", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 10, (short) 1), "CLICK", requestHeaderValid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 11, (short) 1), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 12, (short) 1), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(20, 13, (short) 1), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(14, 0, (short) 0), "IMPRESSION", requestHeaderInvalid, "EPN"));
    addEvent(transactionalTable, new IPCapperEvent(generateIdentity(25, 0, (short) 0), "IMPRESSION", requestHeaderInvalid, "EPN"));
  }
  
  protected static <T> void putCell(Put put, String family, String qualifier, T value) {
    
    byte[] bytes;
    if (value instanceof Long) {
      bytes = Bytes.toBytes(((Long) value).longValue());
    } else if (value instanceof String) {
      bytes = Bytes.toBytes((String) value);
    } else if (value instanceof Boolean) {
      bytes = Bytes.toBytes(((Boolean) value).booleanValue());
    } else if (value instanceof Timestamp) {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      String str = ((Timestamp) value).toString();
      int bufferSize = str.getBytes().length;
      System.arraycopy(str.getBytes(), 0, buffer.array(), 0, bufferSize);
      bytes = buffer.array();
    } else if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else {
      bytes = null;
    }
    
    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes);
  }
  
  private static void addEvent(HTable table, IPCapperEvent event) throws IOException {
    
    Put put = new Put(event.getRowIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", event.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", event.getRequestHeaders());
    table.put(put);
  }
}
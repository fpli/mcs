package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.cappingrules.Rules.SNIDCapper;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperEvent;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TestSNIDCapper extends AbstractCappingRuleTest {
  protected static final String RESULT_TABLE_NAME_WITH_CHANNEL = "capping_result_with_channel";
  protected static final String RESULT_TABLE_NAME_WITH_TIME_WINDOW = "capping_result_with_time_window";
  private static String stopTime;
  
  @BeforeClass
  public static void initialHbaseTable() throws IOException {
    setDataIntoTransactionTable();
    initHBaseCappingResultTable(RESULT_TABLE_NAME_WITH_CHANNEL);
    initHBaseCappingResultTable(RESULT_TABLE_NAME_WITH_TIME_WINDOW);
    
    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Assert.assertEquals(35, getCount(iter));
    iter.close();
    
    Calendar c = Calendar.getInstance();
    stopTime = IdentifierUtil.INPUT_DATE_FORMAT.format(c.getTime());
  }
  
  @Test
  public void testSNIDCapper() throws Exception {
    SNIDCapper job = new SNIDCapper("TestSNIDCapper", "local[4]", TRANSACTION_TABLE_NAME,
        RESULT_TABLE_NAME, stopTime, 24 * 60);
    job.run();
    
    HBaseScanIterator resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME);
    Assert.assertEquals(20, getCount(resultTableItr));
    resultTableItr.close();
    
    job.stop();
  }
  
  @Test
  public void testSNIDCapperWithChannel() throws Exception {
    SNIDCapper job = new SNIDCapper("TestSNIDCapper", "local[4]", TRANSACTION_TABLE_NAME,
        RESULT_TABLE_NAME_WITH_CHANNEL, "EPN", stopTime, 24 * 60);
    job.run();
    
    HBaseScanIterator resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME_WITH_CHANNEL);
    Assert.assertEquals(17, getCount(resultTableItr));
    resultTableItr.close();
    
    job.stop();
  }
  
  @Test
  public void testSNIDCapperWithTimeWindow() throws Exception {
    SNIDCapper job = new SNIDCapper("TestSNIDCapper", "local[4]", TRANSACTION_TABLE_NAME,
        RESULT_TABLE_NAME_WITH_TIME_WINDOW, "EPN", stopTime, 24 * 60, 8);
    job.run();
    
    HBaseScanIterator resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME_WITH_TIME_WINDOW);
    Assert.assertEquals(5, getCount(resultTableItr));
    resultTableItr.close();
    
    job.stop();
  }
  
  protected static void setDataIntoTransactionTable() throws IOException {
  
//    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
//    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
//        .setCompressionType(Compression.Algorithm.NONE));
//    hbaseUtility.getHBaseAdmin().createTable(tableDesc);
    
    Calendar c = Calendar.getInstance();
    c.add(Calendar.MINUTE, -5);
    
   // HTable transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), HBaseConnection.getConnection
    // ());
    
    // click happens after impression on same host and different host
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "100", "IMPRESSION", "EPN"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), "100", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), "100", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), "100", "CLICK", "EPN"));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "100", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 10), "100", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 3), null, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 105,
        (short) 2), "undefined", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 106,
        (short) 1), "UNDEFINED", "CLICK", "EPN"));
    
    // click happens after impression on same host and different host with empty snid
    c.add(Calendar.MINUTE, -10);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "", "IMPRESSION", "EPN"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), null, "IMPRESSION", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), "", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), null, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), "", "CLICK", "EPN"));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 10), "", "CLICK", "EPN"));
    
    // click&impression happens at same time
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 200,
        (short) 0), "200", "IMPRESSION", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 201,
        (short) 1), "200", "CLICK", "EPN"));
    
    // click happens before impression
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 300,
        (short) 1), "300", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 301,
        (short) 1), "300", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 303,
        (short) 2), "300", "CLICK", "EPN"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 300,
        (short) 0), "300", "IMPRESSION", "EPN"));
    
    // only click no impression
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 400,
        (short) 1), "400", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 401,
        (short) 1), "400", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 402,
        (short) 1), "400", "CLICK", "EPN"));
    
    // click happens after impression on other channels
    c.add(Calendar.MINUTE, -5);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "100", "IMPRESSION", "DAP"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), "100", "CLICK", "DAP"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), "100", "CLICK", "DAP"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), "100", "CLICK", "DAP"));
  
    // click happens after impression on same host and different host before 30mins
    c.add(Calendar.MINUTE, -10);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "100", "IMPRESSION", "EPN"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), "100", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), "100", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), "100", "CLICK", "EPN"));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "100", "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 10), "100", "CLICK", "EPN"));
  }
  
  private static void addEvent(HTable table, SNIDCapperEvent snidCapperEvent) throws IOException {
    Put put = new Put(snidCapperEvent.getRowIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "snid", snidCapperEvent.getSnid());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", snidCapperEvent.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_type", snidCapperEvent.getChannelType());
    table.put(put);
  }
}

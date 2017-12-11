package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.cappingrules.Rules.TempSNIDCapper;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperEvent;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TestTempSNIDCapper extends AbstractCappingRuleTest {
  private static String stopTime;
  
  @BeforeClass
  public static void initialHbaseTable() throws IOException {
    setDataIntoTransactionTable();
    
    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Assert.assertEquals(25, getCount(iter));
    iter.close();
    
    Calendar c = Calendar.getInstance();
    stopTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
  }
  
  @Test
  public void testTempSNIDCapper() throws Exception {
    TempSNIDCapper job = new TempSNIDCapper("TestTempSNIDCapper", "local[4]", TRANSACTION_TABLE_NAME,
        RESULT_TABLE_NAME, "EPN", stopTime, 24*60, 30);
    job.run();
    
    HBaseScanIterator resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME);
    Assert.assertEquals(12, getCount(resultTableItr));
    resultTableItr.close();
    
    job.stop();
  }
  
  
  protected static void setDataIntoTransactionTable() throws IOException {
    
    Calendar c = Calendar.getInstance();
    c.add(Calendar.MINUTE, -10);
    
    String requestHeader = "Cookie: aaa ;|X-eBay-Client-IP: 50.206.232.22|Connection: keep-alive|User-Agent: " +
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 " +
        "Safari/537.36";
    
    
    // click happens after impression on same host and different host
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), requestHeader, "IMPRESSION", "EPN"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), requestHeader, "CLICK", "EPN"));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 10), requestHeader, "CLICK", "EPN"));
  
    // click&impression happens at same time
    requestHeader = requestHeader.replace("50.206.232.22", "20.206.232.22");
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 200,
        (short) 0), requestHeader, "IMPRESSION", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 201,
        (short) 1), requestHeader, "CLICK", "EPN"));
    
    // click happens before impression
    requestHeader = requestHeader.replace("20.206.232.22", "10.206.232.22");
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 300,
        (short) 1), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 301,
        (short) 1), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 303,
        (short) 2), requestHeader, "CLICK", "EPN"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 300,
        (short) 0), requestHeader, "IMPRESSION", "EPN"));
    
    // only click no impression
    requestHeader = requestHeader.replace("10.206.232.22", "10.207.232.22");
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 400,
        (short) 1), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 401,
        (short) 1), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 402,
        (short) 1), requestHeader, "CLICK", "EPN"));
    
    // click happens after impression on different channel
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), requestHeader, "IMPRESSION", "DAP"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), requestHeader, "CLICK", "DAP"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), requestHeader, "CLICK", "DAP"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), requestHeader, "CLICK", "DAP"));
  
    // click happens after impression on same host and different host before 60mins
    c.add(Calendar.MINUTE, -60);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), requestHeader, "IMPRESSION", "EPN"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), requestHeader, "CLICK", "EPN"));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), requestHeader, "CLICK", "EPN"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 10), requestHeader, "CLICK", "EPN"));
  }
  
  private static void addEvent(HTable table, SNIDCapperEvent snidCapperEvent) throws IOException {
    Put put = new Put(snidCapperEvent.getRowIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", snidCapperEvent.getSnid());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", snidCapperEvent.getChannelAction());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_type", snidCapperEvent.getChannelType());
    table.put(put);
  }
}

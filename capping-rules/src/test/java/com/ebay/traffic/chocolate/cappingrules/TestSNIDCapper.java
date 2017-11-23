package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.cappingrules.Rules.SNIDCapper;
import com.ebay.traffic.chocolate.cappingrules.dto.SNIDCapperEvent;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TestSNIDCapper extends AbstractCappingRuleTest {
  
  @Test
  public void testSNIDCapper() throws Exception {
    HBaseScanIterator iter = new HBaseScanIterator(TRANSACTION_TABLE_NAME);
    Assert.assertEquals(22, getCount(iter));
    iter.close();
    
    Calendar c = Calendar.getInstance();
    String stopTime = new SimpleDateFormat("yyyy-MM-dd 59:59:59").format(c.getTime());
    c.add(Calendar.DATE, -1);
    String startTime = new SimpleDateFormat("yyyy-MM-dd 00:00:00").format(c.getTime());
    
    SNIDCapper job = new SNIDCapper("TestSNIDCapper", "local[4]", TRANSACTION_TABLE_NAME,
        RESULT_TABLE_NAME, startTime, stopTime);
    job.run();
    
    HBaseScanIterator resultTableItr = new HBaseScanIterator(RESULT_TABLE_NAME);
    Assert.assertEquals(12, getCount(resultTableItr));
    resultTableItr.close();
    
    job.stop();
  }
  
  @Override
  protected void initHBaseTransactionTable() throws IOException {
    
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
        .setCompressionType(Compression.Algorithm.NONE));
    hbaseUtility.getHBaseAdmin().createTable(tableDesc);
    
    Calendar c = Calendar.getInstance();
    c.add(Calendar.HOUR, -5);
    c.add(Calendar.MINUTE, 10);
    
    HTable transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), HBaseConnection.getConnection());
    
    // click happens after impression on same host and different host
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "100", "IMPRESSION"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), "100", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), "100", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), "100", "CLICK"));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "100", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 10), "100", "CLICK"));
    
    // click happens after impression on same host and different host with empty snid
    c.add(Calendar.MINUTE, 1);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "", "IMPRESSION"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "", "IMPRESSION"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 1), "", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 102,
        (short) 2), "", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 103,
        (short) 2), "", "CLICK"));
    c.add(Calendar.SECOND, 40);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 101,
        (short) 0), "", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 104,
        (short) 10), "", "CLICK"));
    
    // click&impression happens at same time
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 200,
        (short) 0), "200", "IMPRESSION"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 201,
        (short) 1), "200", "CLICK"));
    
    // click happens before impression
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 300,
        (short) 1), "300", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 301,
        (short) 1), "300", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 303,
        (short) 2), "300", "CLICK"));
    c.add(Calendar.SECOND, 20);
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 300,
        (short) 0), "300", "IMPRESSION"));
    
    // only click no impression
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 400,
        (short) 1), "400", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 401,
        (short) 1), "400", "CLICK"));
    addEvent(transactionalTable, new SNIDCapperEvent(IdentifierUtil.generateIdentifier(c.getTimeInMillis(), 402,
        (short) 1), "400", "CLICK"));
  }
  
  private void addEvent(HTable table, SNIDCapperEvent snidCapperEvent) throws IOException {
    Put put = new Put(snidCapperEvent.getRowIdentifier());
    putCell(put, TRANSACTION_CF_DEFAULT, "snid", snidCapperEvent.getSnid());
    putCell(put, TRANSACTION_CF_DEFAULT, "channel_action", snidCapperEvent.getChannelAction());
    table.put(put);
  }
}

package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.BaseFunSuite;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * Created by yimeng on 11/15/17.
 */
@Ignore
public abstract class AbstractSparkHbaseTest extends BaseFunSuite {
  
  protected static final String TRANSACTION_TABLE_NAME = "prod_transactional";
  protected static final String RESULT_TABLE_NAME = "capping_result";
  protected static final String TRANSACTION_CF_DEFAULT = "x";
  protected static HBaseTestingUtility hbaseUtility;
  protected static HTable transactionalTable;
  
  @BeforeClass
  public static void setUp() throws Exception {
    hbaseUtility = new HBaseTestingUtility();
    hbaseUtility.startMiniCluster();
    
    HBaseConnection.setConfiguration(hbaseUtility.getConfiguration());
    
    initHBaseTransactionTable();
    initHBaseCappingResultTable(RESULT_TABLE_NAME);
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    hbaseUtility.shutdownMiniCluster();
  }
 
  protected static void initHBaseTransactionTable() throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
        .setCompressionType(Compression.Algorithm.NONE));
    hbaseUtility.getHBaseAdmin().createTable(tableDesc);
    transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), HBaseConnection.getConnection());
  }
  
  protected static void initHBaseCappingResultTable(String resultTable) throws IOException {
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(resultTable));
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
        .setCompressionType(Compression.Algorithm.NONE));
    hbaseUtility.getHBaseAdmin().createTable(tableDesc);
  }
  
  //protected abstract void initHBaseTransactionTable() throws IOException;
  
  protected static  <T> void putCell(Put put, String family, String qualifier, T value) {
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
    } else {
      bytes = null;
    }
    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes);
  }
  
  protected static int getCount(HBaseScanIterator iter) {
    int numberOfRow = 0;
    while (iter.hasNext()) {
      iter.next();
      numberOfRow++;
    }
    return numberOfRow;
  }
}

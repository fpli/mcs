package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yliu29 on 11/16/17.
 * <p>
 * The iterator for HBase scan.
 */
public class HBaseScanIterator implements Iterator<Result> {
  private Iterator<Result> iter;
  private HTable table;
  private ResultScanner rs;
  
  public HBaseScanIterator(String tableName) throws IOException {
    this(tableName, null, null);
  }
  
  public HBaseScanIterator(String tableName, byte[] startRowKey, byte[] stopRowKey, String channelType)
      throws IOException {
    table = new HTable(TableName.valueOf(tableName), HBaseConnection.getConnection());
    Scan scan = new Scan();
    if (startRowKey != null) {
      scan.setStartRow(startRowKey);
    }
    if (stopRowKey != null) {
      scan.setStopRow(stopRowKey);
    }
    if (StringUtils.isNotEmpty(channelType)) {
      SingleColumnValueFilter colValFilter = new SingleColumnValueFilter(HBaseConstant.COLUMN_FAMILY_X, HBaseConstant.COL_CHANNEL_TYPE
          , CompareFilter.CompareOp.EQUAL, Bytes.toBytes(channelType));
      scan.setFilter(colValFilter);
    }
    
    rs = table.getScanner(scan);
    iter = rs.iterator();
  }
  
  public HBaseScanIterator(String tableName, byte[] startRowKey, byte[] stopRowKey)
      throws IOException {
    this(tableName, startRowKey, stopRowKey, null);
  }
  
  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }
  
  @Override
  public Result next() {
    return iter.next();
  }
  
  public void close() throws IOException {
    if (rs != null) {
      rs.close();
    }
    if (table != null) {
      table.close();
    }
  }
  
  @Override
  protected void finalize() throws Throwable {
    close();
  }
}

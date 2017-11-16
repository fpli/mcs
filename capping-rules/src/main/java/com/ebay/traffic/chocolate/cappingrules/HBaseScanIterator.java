package com.ebay.traffic.chocolate.cappingrules;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by yliu29 on 11/16/17.
 *
 * The iterator for HBase scan.
 */
public class HBaseScanIterator implements Iterator<Result> {
  private HTable table;
  private final Iterator<Result> iter;
  private ResultScanner rs;

  public HBaseScanIterator(String tableName)throws IOException {
    this(tableName, null, null);
  }

  public HBaseScanIterator(String tableName, byte[] startRowKey, byte[] stopRowKey)
          throws IOException {
    table = new HTable(TableName.valueOf(tableName), HBaseConnection.getConnection());
    Scan scan = new Scan();
    if (startRowKey != null) {
      scan.setStartRow(startRowKey);
    }
    if (stopRowKey != null) {
      scan.setStopRow(stopRowKey);
    }
    rs = table.getScanner(scan);
    iter = rs.iterator();
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

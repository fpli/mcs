package com.ebay.traffic.chocolate.cappingrules.hdfs;

import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.traffic.chocolate.cappingrules.IdentifierUtil;
import com.ebay.traffic.chocolate.cappingrules.constant.HBaseConstant;
import org.apache.hadoop.hbase.util.Bytes;
import scala.tools.cmd.gen.AnyVals;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

public class TestMain {

  public static void main(String[] args) throws IOException {
    Calendar c = Calendar.getInstance();

    System.out.println(c.getTimeInMillis());

    SnapshotId snapshotId = new SnapshotId(0, c.getTimeInMillis());
    System.out.println(snapshotId.getTimeMillis());

    System.out.println(IdentifierUtil.getMonthFromSnapshotId(snapshotId.getRepresentation()));

    byte[] b = Bytes.toBytes(201811);
    System.out.println( Bytes.toInt(b));
    System.out.println( b.length);
  }
}

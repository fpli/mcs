package com.ebay.traffic.html;

import com.ebay.traffic.chocolate.html.BenchMarkTable;
import com.ebay.traffic.chocolate.pojo.BenchMarkInfo;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestBenchMarkTable {

  @Test
  public void testParseBenchMarkProject(){
    List<BenchMarkInfo> list = new ArrayList<>();
    setList(list);

    BenchMarkTable.parseBenchMarkProject(list);
  }

  private void setList(List<BenchMarkInfo> list) {
    BenchMarkInfo benchMarkInfo = new BenchMarkInfo();

    benchMarkInfo.setChannelType("1");
    benchMarkInfo.setActionType("1");
    benchMarkInfo.setOnedayCount("100");
    benchMarkInfo.setAvg("120");

    list.add(benchMarkInfo);
  }

}

package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.MetricCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HTMLParse {

	private static final Logger logger = LoggerFactory.getLogger(HTMLParse.class);

	public static String parse(HashMap<String, ArrayList<MetricCount>> map, HashMap<String, ArrayList<MetricCount>> historymap, String runPeriod) {
		String html = "";

		int len = map.size();
		int count = 1;
		HashMap<Integer, String> numMap = toMap(map.keySet());
		while (count <= len) {
			String project_name = numMap.get(count);
			html = html + Table.parseProject(map.get(project_name + "_" + count), project_name);
			count++;
		}

		if (historymap != null && historymap.size() > 1) {
			Iterator<String> historyIts = historymap.keySet().iterator();
			html = html + "\n\n\n\n" + Table.getTtile("history list");
			ArrayList<ArrayList<MetricCount>> list = new ArrayList<ArrayList<MetricCount>>();
			while (historyIts.hasNext()) {
				list.add(historymap.get(historyIts.next()));
			}

			html = html + HistoryTable.parseHistoryProject(list);
		}

		if(runPeriod.equalsIgnoreCase("daily")) {
			html = html + "Hdfs file number compare\n" + HdfsCompareTable.parseHdfsCompare(HdfsCompareUtil.getHdfsFileNumberCompares());
		}else if(runPeriod.equalsIgnoreCase("hourly")){
			html = html + "Done file information\n" + DoneFileTable.parseDoneFileProject(DoneFileUtil.getDoneFileInfos());
		}

		return html;
	}

	private static HashMap<Integer, String> toMap(Set<String> set) {
		HashMap<Integer, String> map = new HashMap<Integer, String>();

		Iterator<String> iterator = set.iterator();
		while (iterator.hasNext()) {
			String[] arr = iterator.next().split("_");
			map.put(Integer.parseInt(arr[1]), arr[0]);
		}

		return map;
	}


}

package com.ebay.traffic.chocolate.xml;

import com.ebay.traffic.chocolate.channel.elasticsearch.ESReport;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author lxiong1
 */
public class XMLUtil {

  private static final Logger logger = LoggerFactory.getLogger(XMLUtil.class);

  public static HashMap<String, ArrayList<String>> read(String fileName) {
    HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();

    SAXReader reader = new SAXReader();
    try {
      Document document = reader.read(new File(fileName));
      Element projects = document.getRootElement();
      Iterator it = projects.elementIterator();
      while (it.hasNext()) {
        Element project = (Element) it.next();
        String name = project.attribute("name").getValue();
        String filterMetrics= project.attribute("filter").getValue();

        map.put(name, getFilterMerticList(filterMetrics));
      }
    } catch (DocumentException e) {
      e.printStackTrace();
    }

    return map;
  }

  private static ArrayList<String> getFilterMerticList(String filterMetrics) {
    ArrayList<String> filterMerticList = new ArrayList<String>();
    if(filterMetrics == null || filterMetrics.length() < 1){
      return filterMerticList;
    }

    String[] filterMetricArr = filterMetrics.split(",");
    for (String filterMetric:filterMetricArr){
      filterMerticList.add(filterMetric);
    }

    return filterMerticList;
  }

}

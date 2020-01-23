package com.ebay.traffic.chocolate.xml;

import com.ebay.traffic.chocolate.pojo.Azkaban;
import com.ebay.traffic.chocolate.pojo.Metric;
import com.ebay.traffic.chocolate.pojo.Project;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * @author lxiong1
 */
public class XMLUtil {

  private static final Logger logger = LoggerFactory.getLogger(XMLUtil.class);

  public static LinkedList<Project> read(String fileName) {
    LinkedList<Project> projectList = new LinkedList<Project>();

    SAXReader reader = new SAXReader();
    try {
      Document document = reader.read(new File(fileName));
      Element projects = document.getRootElement();
      Iterator it = projects.elementIterator();
      while (it.hasNext()) {
        ArrayList<Metric> list = new ArrayList<Metric>();
        Element project = (Element) it.next();
        String project_name = project.attribute("name").getValue() + "_" + project.attribute("id").getValue();
        Integer id = Integer.parseInt(project.attribute("id").getValue());
        List<Element> metrics = project.elements("metric");
        for (Element elem : metrics) {
          Metric metric = new Metric();
          metric.setProject_name(project_name);
          metric.setName(elem.attribute("name").getValue());
          metric.setValue(elem.attribute("value").getValue());
          metric.setSource(elem.attribute("source").getValue());
          metric.setCondition(getCondition(elem.attribute("condition").getValue()));
          metric.setThreshold(Long.parseLong(elem.attribute("threshold").getValue()));
          metric.setThresholdFactor(Double.parseDouble(elem.attribute("thresholdFactor").getValue()));
          metric.setComputeType(elem.attribute("computeType").getValue());
          metric.setAlert(elem.attribute("alert").getValue());
          list.add(metric);
        }
        Project pro = new Project();
        pro.setName(project_name);
        pro.setId(id);
        pro.setList(list);
        projectList.add(pro);
      }
    } catch (DocumentException e) {
      e.printStackTrace();
    }

    return projectList;
  }

  public static HashMap<String, ArrayList<Azkaban>> readAzkabanMap(String fileName) {
    HashMap<String, ArrayList<Azkaban>> map = new HashMap<>();

    SAXReader reader = new SAXReader();
    try {
      Document document = reader.read(new File(fileName));
      Element projects = document.getRootElement();
      Iterator it = projects.elementIterator();
      while (it.hasNext()) {
        ArrayList<Azkaban> list = new ArrayList<>();
        Element project = (Element) it.next();
        String subjectArea = project.attribute("name").getValue();
        List<Element> metrics = project.elements("metric");
        for (Element elem : metrics) {
          Azkaban azkaban = new Azkaban();
          azkaban.setSubjectArea(subjectArea);
          azkaban.setProjectName(elem.attribute("projectName").getValue());
          azkaban.setFlowName(elem.attribute("flowName").getValue());
          azkaban.setTotal(elem.attribute("total").getValue());
          azkaban.setAlertCount(elem.attribute("alertCount").getValue());
          list.add(azkaban);
        }
        map.put(subjectArea, list);
      }
    } catch (DocumentException e) {
      e.printStackTrace();
    }

    return map;
  }

  private static String getCondition(String condition) {
    if (condition == null) {
      return "";
    } else {
      return condition;
    }
  }

}
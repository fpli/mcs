package com.ebay.traffic.chocolate.map;

import com.ebay.traffic.chocolate.map.constant.JobParamConstant;
import com.ebay.traffic.chocolate.map.entity.DataCountInfo;
import com.ebay.traffic.chocolate.map.service.EpnMapTableService;
import com.ebay.traffic.chocolate.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;

@SpringBootApplication
public class ChocomapApplication implements CommandLineRunner {
  private static Logger logger = LoggerFactory.getLogger(ChocomapApplication.class);

  @Autowired
  EpnMapTableService epnMapTableService;

  public static void main(String[] args) {
    SpringApplication.run(ChocomapApplication.class, args);
    System.exit(0);
  }

  @Override
  public void run(String[] args) throws Exception {
    JobParamConstant.START_TIME = args[0];
    JobParamConstant.END_TIME = args[1];
    String env = args[2];
    if (env.equalsIgnoreCase("prod")) {
      JobParamConstant.ENV = "";
    } else {
      JobParamConstant.ENV = "dev_";
    }
    String path = args[3];

    ArrayList<DataCountInfo> list = new ArrayList<>();

    //one time
    epnMapTableService.getTrafficSource(list);
    logger.info("Traffic Source Info get finished");

    //one time
    epnMapTableService.getClickFilterType(list);
    logger.info("Click Filter Type Info get finished");

    //one time
    epnMapTableService.getProgramInfo(list);
    logger.info("Program Info get finished");

    epnMapTableService.getClickFilterMap(JobParamConstant.START_TIME, JobParamConstant.END_TIME, list, JobParamConstant.ENV);
    logger.info("PublisherId Rule Map Info get finished");

    epnMapTableService.getProgPubMap(JobParamConstant.START_TIME, JobParamConstant.END_TIME, list, JobParamConstant.ENV);
    logger.info("Program Publisher Map Info get finished");

    epnMapTableService.getPublisherCampaignInfo(JobParamConstant.START_TIME, JobParamConstant.END_TIME, list, JobParamConstant.ENV);
    logger.info("Publisher Campaign Info get finished");

    epnMapTableService.getPublisherInfo(JobParamConstant.START_TIME, JobParamConstant.END_TIME, list, JobParamConstant.ENV);
    logger.info("Publisher Info get finished ");

    epnMapTableService.getPubDomainInfo(JobParamConstant.START_TIME, JobParamConstant.END_TIME, list, JobParamConstant.ENV);
    logger.info("Publisher Domain Info get finished");

    FileUtils.writeCSV(list, path);

  }
}

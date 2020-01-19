package com.ebay.traffic.chocolate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class FileReadUtil {
  private static final Logger logger = LoggerFactory.getLogger(FileReadUtil.class);

  public static HashMap<String, String> getRotationAlertMap(String dirName) {
    HashMap<String, String> map = new HashMap<>();
    String fileName = dirName + "/" + "000000_0";

    List<String> lines = Collections.emptyList();
    try {
      logger.info("rotation alert dirName: " + fileName);
      lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (lines.size() > 0) {
      String line = lines.get(0);
      logger.info("line: " + line);
      String[] arr = line.split("\001");
      if (arr.length == 2) {
        map.put("count", arr[0]);
        map.put("distinctCount", arr[1]);
        logger.info("arr[0] : " + arr[0]);
        logger.info("arr[1] : " + arr[1]);
      }
    }

    return map;
  }

}
package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.html.HourlyEmailHtml;
import com.ebay.traffic.chocolate.pojo.BatchDoneConfig;
import com.ebay.traffic.chocolate.pojo.BatchDoneFile;
import com.ebay.traffic.chocolate.util.Constants;
import com.ebay.traffic.chocolate.util.DoneFileReadUtil;
import org.apache.commons.io.FileUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.junit.Test;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * <pre>
 * BatchDoneTest
 * </pre>
 *
 * @author jinzhiheng
 * @version BatchDoneTest, 2022-12-02 13:58:59
 */
public class BatchDoneTest {

    @Test
    public void getBatchDoneHtml(){
        String batchDoneFileHtml = HourlyEmailHtml.getBatchDoneFileHtml();
        System.out.println(batchDoneFileHtml);
    }
}

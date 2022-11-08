package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.parse.HTMLParse;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

/**
 * <pre>
 *
 * </pre>
 *
 * @author jinzhiheng
 * @version HourlyReportTest, 2022-11-02 20:53:43
 */
public class HourlyReportTest {

    @Test
    public void getHourlyReport(){
        String parse = HTMLParse.parseHourly("appollo");
        System.out.println(parse);
    }

    @Test
    public void readFiles() throws Exception {
        File file = new File("src/test/resources/1102HDFSFiles2/apollo_done_files.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));

        ArrayList<String> list = new ArrayList<>();
        String str = "";
        while ((str = br.readLine()) != null) {
            list.add(str);
        }

        list.forEach(System.out::println);
    }

}

package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.client.HerculesHdfsClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class TestHerculesHdfsClient {

	@Test
	public void testReadFiles() throws Exception {
		ArrayList<String> list = HerculesHdfsClient.readFiles( this.getClass().getResource("/").getPath() + "hercules_done_files.txt");

		Assert.assertEquals(8, list.size());
	}

	@Test
	public void testGetDoneFileList() throws Exception {
		ArrayList<String> list = HerculesHdfsClient.getDoneFileList( this.getClass().getResource("/").getPath() + "hercules_done_files.txt","","ams_click_hourly");

		Assert.assertEquals(8, list.size());
	}

}

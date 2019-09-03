package com.ebay.traffic.chocolate.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

public class ChocolateHdfsClient {

	public static ArrayList<String> getFileList(String path) {
		ArrayList<String> list = null;
		try {
			list = readFiles(path);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return list;
	}

	public static ArrayList<String> readFiles(String path) throws Exception {
		File file = new File(path);
		BufferedReader br = new BufferedReader(new FileReader(file));

		ArrayList<String> list = new ArrayList<>();
		String str = "";
		while ((str = br.readLine()) != null) {
			list.add(str);
		}

		return list;
	}

}

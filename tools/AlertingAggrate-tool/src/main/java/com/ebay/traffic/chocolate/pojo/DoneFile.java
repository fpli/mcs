package com.ebay.traffic.chocolate.pojo;

public class DoneFile {

	private String dataSource;
	private String status;
	private int delay;
	private String currentDoneFile;
	private String clusterName;

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public String getCurrentDoneFile() {
		return currentDoneFile;
	}

	public void setCurrentDoneFile(String currentDoneFile) {
		this.currentDoneFile = currentDoneFile;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

}

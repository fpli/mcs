package com.ebay.traffic.chocolate.pojo;

public class HourlyClickCount {

	private String count_dt;
	private int click_hour;
	private int click_count;
	private int rsn_cd;
	private int roi_fltr_yn_ind;

	public String getCount_dt() {
		return count_dt;
	}

	public void setCount_dt(String count_dt) {
		this.count_dt = count_dt;
	}

	public int getClick_hour() {
		return click_hour;
	}

	public void setClick_hour(int click_hour) {
		this.click_hour = click_hour;
	}

	public int getClick_count() {
		return click_count;
	}

	public void setClick_count(int click_count) {
		this.click_count = click_count;
	}

	public int getRsn_cd() {
		return rsn_cd;
	}

	public void setRsn_cd(int rsn_cd) {
		this.rsn_cd = rsn_cd;
	}

	public int getRoi_fltr_yn_ind() {
		return roi_fltr_yn_ind;
	}

	public void setRoi_fltr_yn_ind(int roi_fltr_yn_ind) {
		this.roi_fltr_yn_ind = roi_fltr_yn_ind;
	}

}

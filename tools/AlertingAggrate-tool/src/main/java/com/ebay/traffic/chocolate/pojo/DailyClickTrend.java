package com.ebay.traffic.chocolate.pojo;

public class DailyClickTrend {

	private String click_dt;
	private int click_cnt;
	private int rsn_cd;
	private int roi_fltr_yn_ind;

	public String getClick_dt() {
		return click_dt;
	}

	public void setClick_dt(String click_dt) {
		this.click_dt = click_dt;
	}

	public int getClick_cnt() {
		return click_cnt;
	}

	public void setClick_cnt(int click_cnt) {
		this.click_cnt = click_cnt;
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

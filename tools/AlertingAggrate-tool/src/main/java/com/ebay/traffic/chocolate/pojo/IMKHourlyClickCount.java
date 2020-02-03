package com.ebay.traffic.chocolate.pojo;

/**
 * Created by shuangxu on 10/23/19.
 */
public class IMKHourlyClickCount {
    private Integer channel_id;
    private String event_dt;
    private Integer click_hour;
    private Integer click_count;
    private Integer distinct_click_count;
    private Integer differences;

    public String getEvent_dt() {
        return event_dt;
    }

    public void setEvent_dt(String event_dt) {
        this.event_dt = event_dt;
    }

    public Integer getClick_hour() {
        return click_hour;
    }

    public void setClick_hour(Integer click_hour) {
        this.click_hour = click_hour;
    }

    public Integer getClick_count() {
        return click_count;
    }

    public void setClick_count(Integer click_count) {
        this.click_count = click_count;
    }

    public Integer getChannel_id() {
        return channel_id;
    }

    public void setChannel_id(Integer channel_id) {
        this.channel_id = channel_id;
    }

    public Integer getDistinct_click_count() {
        return distinct_click_count;
    }

    public void setDistinct_click_count(Integer distinct_click_count) {
        this.distinct_click_count = distinct_click_count;
    }

    public Integer getDifferences() {
        return differences;
    }

    public void setDifferences(Integer differences) {
        this.differences = differences;
    }
}

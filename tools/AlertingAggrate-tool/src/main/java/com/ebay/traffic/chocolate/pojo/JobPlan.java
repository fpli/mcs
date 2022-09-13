package com.ebay.traffic.chocolate.pojo;

public class JobPlan {

    private String id;
    private String type;
    private String alert;
    private String projectName;
    private String lastStartDate;
    private String lastRunningTime;
    private int total;
    private int success;
    private int failed;
    private int running;
    private String flag;
    private String state;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAlert() {
        return alert;
    }

    public void setAlert(String alert) {
        this.alert = alert;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getLastStartDate() {
        return lastStartDate;
    }

    public void setLastStartDate(String lastStartDate) {
        this.lastStartDate = lastStartDate;
    }

    public String getLastRunningTime() {
        return lastRunningTime;
    }

    public void setLastRunningTime(String lastRunningTime) {
        this.lastRunningTime = lastRunningTime;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getSuccess() {
        return success;
    }

    public void setSuccess(int success) {
        this.success = success;
    }

    public int getFailed() {
        return failed;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public int getRunning() {
        return running;
    }

    public void setRunning(int running) {
        this.running = running;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString(){
        return "id:" + id + ";" +"type:" + type + ";" +"alert:" + alert + ";" +"projectName:" + projectName + ";" +"lastStartDate:" + lastStartDate + ";" +"lastRunningTime:" + lastRunningTime + ";" +"total:" + total + ";" +"success:" + success + ";" +"failed:" + failed + ";" +"running:" + running + ";" +"flag:" + flag + ";" +"state:" + state + ";";
    }
}

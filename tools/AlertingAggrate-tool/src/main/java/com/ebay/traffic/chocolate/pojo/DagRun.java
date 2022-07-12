package com.ebay.traffic.chocolate.pojo;

public class DagRun {

    private String dag_id;
    private String dag_run_id;
    private String end_date;
    private String execution_date;
    private Boolean external_trigger;
    private String start_date;
    private String state;

    public String getDag_id() {
        return dag_id;
    }

    public void setDag_id(String dag_id) {
        this.dag_id = dag_id;
    }

    public String getDag_run_id() {
        return dag_run_id;
    }

    public void setDag_run_id(String dag_run_id) {
        this.dag_run_id = dag_run_id;
    }

    public String getEnd_date() {
        return end_date;
    }

    public void setEnd_date(String end_date) {
        this.end_date = end_date;
    }

    public String getExecution_date() {
        return execution_date;
    }

    public void setExecution_date(String execution_date) {
        this.execution_date = execution_date;
    }

    public Boolean getExternal_trigger() {
        return external_trigger;
    }

    public void setExternal_trigger(Boolean external_trigger) {
        this.external_trigger = external_trigger;
    }

    public String getStart_date() {
        return start_date;
    }

    public void setStart_date(String start_date) {
        this.start_date = start_date;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}

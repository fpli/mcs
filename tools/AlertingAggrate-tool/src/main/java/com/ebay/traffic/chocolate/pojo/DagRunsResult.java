package com.ebay.traffic.chocolate.pojo;

import java.util.List;

public class DagRunsResult {
    private Integer total_entries;
    private List<DagRun> dag_runs;

    public Integer getTotal_entries() {
        return total_entries;
    }

    public void setTotal_entries(Integer total_entries) {
        this.total_entries = total_entries;
    }

    public List<DagRun> getDag_runs() {
        return dag_runs;
    }

    public void setDag_runs(List<DagRun> dag_runs) {
        this.dag_runs = dag_runs;
    }
}

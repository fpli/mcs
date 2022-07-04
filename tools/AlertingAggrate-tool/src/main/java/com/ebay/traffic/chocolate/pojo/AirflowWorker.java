package com.ebay.traffic.chocolate.pojo;

public class AirflowWorker {
    private Double success;
    private Double failed;
    private Double active;
    private Double processed;
    private String workerName;
    private String status;

    public Double getSuccess() {
        return success;
    }

    public void setSuccess(Double success) {
        this.success = success;
    }

    public Double getFailed() {
        return failed;
    }

    public void setFailed(Double failed) {
        this.failed = failed;
    }

    public Double getActive() {
        return active;
    }

    public void setActive(Double active) {
        this.active = active;
    }

    public Double getProcessed() {
        return processed;
    }

    public void setProcessed(Double processed) {
        this.processed = processed;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}

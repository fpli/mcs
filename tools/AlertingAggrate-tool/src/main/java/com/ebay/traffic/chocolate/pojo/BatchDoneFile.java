package com.ebay.traffic.chocolate.pojo;

import org.apache.arrow.flatbuf.Int;

/**
 * <pre>
 *
 * </pre>
 *
 * @author jinzhiheng
 * @version BatchDoneFile, 2022-12-05 13:40:12
 */
public class BatchDoneFile {
    public String doneName;
    public String status;
    public int delay;
    public String currentDoneFile;
    public String cluster;
    public String support;
    public String jobLink;

    public BatchDoneFile(String doneName, String status, int delay, String currentDoneFile, String cluster, String support, String jobLink) {
        this.doneName = doneName;
        this.status = status;
        this.delay = delay;
        this.currentDoneFile = currentDoneFile;
        this.cluster = cluster;
        this.support = support;
        this.jobLink = jobLink;
    }

    public BatchDoneFile() {
    }

    public String getDoneName() {
        return doneName;
    }

    public void setDoneName(String doneName) {
        this.doneName = doneName;
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

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getSupport() {
        return support;
    }

    public void setSupport(String support) {
        this.support = support;
    }

    public String getJobLink() {
        return jobLink;
    }

    public void setJobLink(String jobLink) {
        this.jobLink = jobLink;
    }

    @Override
    public String toString() {
        return "BatchDoneFile{" +
                "doneName='" + doneName + '\'' +
                ", status='" + status + '\'' +
                ", delay=" + delay +
                ", currentDoneFile='" + currentDoneFile + '\'' +
                ", cluster='" + cluster + '\'' +
                ", support='" + support + '\'' +
                ", jobLink='" + jobLink + '\'' +
                '}';
    }
}

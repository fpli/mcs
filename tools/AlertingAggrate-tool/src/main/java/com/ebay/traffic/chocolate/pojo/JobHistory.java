package com.ebay.traffic.chocolate.pojo;

import java.util.ArrayList;
import java.util.Objects;

/**
 * <pre>
 * Uc4 POJO
 * </pre>
 *
 * @author jinzhiheng
 * @version Uc4, 2022-08-22 16:27:19
 */
public class JobHistory {
    private String client;
    private String runId;
    private String parentRunId;
    private String parentAct;
    private String parentType;
    private String name;
    private String prettyName;
    private String alias;
    private String type;
    private String startTime;
    private String endTime;
    private String duration;
    private String durationMins;
    private String agentName;
    private String returnCode;
    private String status;
    private String statusInfo;
    private String comments;
    private int statusCode;
    private int restartCode;
    private String referenceRunId;
    private String runtimeVariables;
    private Boolean editable;
    private String severity;
    private String sourceHost;
    private String destinationHost;
    private Boolean blockedExisted;

    public JobHistory() {
    }

    public JobHistory(String client, String runId, String parentRunId, String parentAct, String parentType, String name, String prettyName, String alias, String type, String startTime, String endTime, String duration, String durationMins, String agentName, String returnCode, String status, String statusInfo, String comments, int statusCode, int restartCode, String referenceRunId, String runtimeVariables, Boolean editable, String severity, String sourceHost, String destinationHost, Boolean blockedExisted) {
        this.client = client;
        this.runId = runId;
        this.parentRunId = parentRunId;
        this.parentAct = parentAct;
        this.parentType = parentType;
        this.name = name;
        this.prettyName = prettyName;
        this.alias = alias;
        this.type = type;
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.durationMins = durationMins;
        this.agentName = agentName;
        this.returnCode = returnCode;
        this.status = status;
        this.statusInfo = statusInfo;
        this.comments = comments;
        this.statusCode = statusCode;
        this.restartCode = restartCode;
        this.referenceRunId = referenceRunId;
        this.runtimeVariables = runtimeVariables;
        this.editable = editable;
        this.severity = severity;
        this.sourceHost = sourceHost;
        this.destinationHost = destinationHost;
        this.blockedExisted = blockedExisted;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getParentRunId() {
        return parentRunId;
    }

    public void setParentRunId(String parentRunId) {
        this.parentRunId = parentRunId;
    }

    public String getParentAct() {
        return parentAct;
    }

    public void setParentAct(String parentAct) {
        this.parentAct = parentAct;
    }

    public String getParentType() {
        return parentType;
    }

    public void setParentType(String parentType) {
        this.parentType = parentType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrettyName() {
        return prettyName;
    }

    public void setPrettyName(String prettyName) {
        this.prettyName = prettyName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getDurationMins() {
        return durationMins;
    }

    public void setDurationMins(String durationMins) {
        this.durationMins = durationMins;
    }

    public String getAgentName() {
        return agentName;
    }

    public void setAgentName(String agentName) {
        this.agentName = agentName;
    }

    public String getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(String returnCode) {
        this.returnCode = returnCode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatusInfo() {
        return statusInfo;
    }

    public void setStatusInfo(String statusInfo) {
        this.statusInfo = statusInfo;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getRestartCode() {
        return restartCode;
    }

    public void setRestartCode(int restartCode) {
        this.restartCode = restartCode;
    }

    public String getReferenceRunId() {
        return referenceRunId;
    }

    public void setReferenceRunId(String referenceRunId) {
        this.referenceRunId = referenceRunId;
    }

    public String getRuntimeVariables() {
        return runtimeVariables;
    }

    public void setRuntimeVariables(String runtimeVariables) {
        this.runtimeVariables = runtimeVariables;
    }

    public Boolean getEditable() {
        return editable;
    }

    public void setEditable(Boolean editable) {
        this.editable = editable;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public String getDestinationHost() {
        return destinationHost;
    }

    public void setDestinationHost(String destinationHost) {
        this.destinationHost = destinationHost;
    }

    public Boolean getBlockedExisted() {
        return blockedExisted;
    }

    public void setBlockedExisted(Boolean blockedExisted) {
        this.blockedExisted = blockedExisted;
    }

    @Override
    public String toString() {
        return "JobHistory{" +
                "client='" + client + '\'' +
                ", runId='" + runId + '\'' +
                ", parentRunId='" + parentRunId + '\'' +
                ", parentAct='" + parentAct + '\'' +
                ", parentType='" + parentType + '\'' +
                ", name='" + name + '\'' +
                ", prettyName='" + prettyName + '\'' +
                ", alias='" + alias + '\'' +
                ", type='" + type + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", duration='" + duration + '\'' +
                ", durationMins='" + durationMins + '\'' +
                ", agentName='" + agentName + '\'' +
                ", returnCode='" + returnCode + '\'' +
                ", status='" + status + '\'' +
                ", statusInfo='" + statusInfo + '\'' +
                ", comments='" + comments + '\'' +
                ", statusCode=" + statusCode +
                ", restartCode=" + restartCode +
                ", referenceRunId='" + referenceRunId + '\'' +
                ", runtimeVariables='" + runtimeVariables + '\'' +
                ", editable=" + editable +
                ", severity='" + severity + '\'' +
                ", sourceHost='" + sourceHost + '\'' +
                ", destinationHost='" + destinationHost + '\'' +
                ", blockedExisted=" + blockedExisted +
                '}';
    }
}

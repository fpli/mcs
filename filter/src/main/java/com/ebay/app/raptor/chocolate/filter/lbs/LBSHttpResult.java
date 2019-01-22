package com.ebay.app.raptor.chocolate.filter.lbs;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LBSHttpResult {

    private int httpStatus;

    private List<LBSQueryResult> queryResult = new ArrayList<LBSQueryResult>();

    public int getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public List<LBSQueryResult> getQueryResult() {
        return queryResult;
    }

    public void setQueryResult(List<LBSQueryResult> queryResult) {
        this.queryResult = queryResult;
    }
}

package com.ebay.traffic.chocolate.flink.nrt.provider.monitor;


/**
 * @author yuhxiao
 */
public class DimensionEntity {
    private String dimensionName;
    private String dimensionVal;

    public String getDimensionName() {
        return dimensionName;
    }

    public void setDimensionName(String dimensionName) {
        this.dimensionName = dimensionName;
    }

    public String getDimensionVal() {
        return dimensionVal;
    }

    public void setDimensionVal(String dimensionVal) {
        this.dimensionVal = dimensionVal;
    }
}

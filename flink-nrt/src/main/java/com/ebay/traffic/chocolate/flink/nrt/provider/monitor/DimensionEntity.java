package com.ebay.traffic.chocolate.flink.nrt.provider.monitor;

import lombok.Data;

import java.sql.Timestamp;

/**
 * @author yuhxiao
 */
@Data
public class DimensionEntity {
    private String dimensionName;
    private String dimensionVal;
    private Timestamp createTs;
    private Timestamp updateTs;
    private Timestamp expireTs;
    private String createUsr;
    private String updateUsr;
    private int hotspot;
}

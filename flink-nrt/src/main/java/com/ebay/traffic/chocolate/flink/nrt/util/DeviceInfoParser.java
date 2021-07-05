package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;

/**
 * Created by zhofan on 06/30/21
 */
public class DeviceInfoParser {
    private DDSResponse deviceInfo;
    private String deviceFamily;
    private String deviceType;
    private String browserFamily;
    private String browserVersion;
    private String osFamily;
    private String osVersion;

    public DeviceInfoParser parse(UserAgentInfo agentInfo) {
        this.deviceInfo = agentInfo.getDeviceInfo();
        if (deviceInfo != null) {
            this.deviceFamily = getDeviceFamily(deviceInfo);
            this.deviceType = deviceInfo.getOsName();
            this.browserFamily = deviceInfo.getBrowser();
            this.browserVersion = deviceInfo.getBrowserVersion();
            this.osFamily = deviceInfo.getDeviceOS();
            this.osVersion = deviceInfo.getDeviceOSVersion();
        }

        return this;
    }

    /**
     * Get device family
     */
    private static String getDeviceFamily(DDSResponse deviceInfo) {
        String deviceFamily;

        if (deviceInfo.isTablet()) {
            deviceFamily = "Tablet";
        } else if (deviceInfo.isTouchScreen()) {
            deviceFamily = "TouchScreen";
        } else if (deviceInfo.isDesktop()) {
            deviceFamily = "Desktop";
        } else if (deviceInfo.isMobile()) {
            deviceFamily = "Mobile";
        } else {
            deviceFamily = "Other";
        }

        return deviceFamily;
    }

    public DDSResponse getDeviceInfo() {
        return deviceInfo;
    }

    public String getDeviceFamily() {
        return deviceFamily;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public String getBrowserFamily() {
        return browserFamily;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public String getOsFamily() {
        return osFamily;
    }

    public String getOsVersion() {
        return osVersion;
    }

}
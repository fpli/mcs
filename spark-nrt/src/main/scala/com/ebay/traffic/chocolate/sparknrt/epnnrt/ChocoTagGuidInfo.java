package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import java.io.Serializable;

public class ChocoTagGuidInfo implements Serializable {
    private String chocoTag;
    private String guid;

    public ChocoTagGuidInfo() {
        chocoTag = "";
        guid = "";
    }

    public String getChocoTag() {
        return chocoTag;
    }

    public void setChocoTag(String chocoTag) {
        this.chocoTag = chocoTag;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }
}

package com.ebay.traffic.chocolate.flink.nrt.model;

import com.google.gson.annotations.SerializedName;

public class TrackingEvent implements EventTsCompatibleEvent {

  @SerializedName("batch_id")
  private String batchId;

  @SerializedName("file_id")
  private int fileId;

  @SerializedName("file_schm_vrsn")
  private int fileSchmVrsn;

  @SerializedName("rvr_id")
  private long rvrId;

  @SerializedName("event_dt")
  private String eventDt;

  @SerializedName("srvd_pstn")
  private int srvdPstn;

  @SerializedName("rvr_cmnd_type_cd")
  private String rvrCmndTypeCd;

  @SerializedName("rvr_chnl_type_cd")
  private String rvrChnlTypeCd;

  @SerializedName("cntry_cd")
  private String cntryCd;

  @SerializedName("lang_cd")
  private String langCd;

  @SerializedName("trckng_prtnr_id")
  private int trckngPrtnrId;

  @SerializedName("cguid")
  private String cguid;

  @SerializedName("guid")
  private String guid;

  @SerializedName("user_id")
  private long userId;

  @SerializedName("clnt_remote_ip")
  private String clntRemoteIp;

  @SerializedName("brwsr_type_id")
  private int brwsrTypeId;

  @SerializedName("brwsr_name")
  private String brwsrName;

  @SerializedName("rfrr_dmn_name")
  private String rfrrDmnName;

  @SerializedName("rfrr_url")
  private String rfrrUrl;

  @SerializedName("url_encrptd_yn_ind")
  private int urlEncrptdYnInd;

  @SerializedName("src_rotation_id")
  private long srcRotationId;

  @SerializedName("dst_rotation_id")
  private long dstRotationId;

  @SerializedName("dst_client_id")
  private int dstClientId;

  @SerializedName("pblshr_id")
  private String pblshrId;

  @SerializedName("lndng_page_dmn_name")
  private String lndngPageDmnName;

  @SerializedName("lndng_page_url")
  private String lndngPageUrl;

  @SerializedName("user_query")
  private String userQuery;

  @SerializedName("rule_bit_flag_strng")
  private String ruleBitFlagStrng;

  @SerializedName("flex_field_vrsn_num")
  private int flexFieldVrsnNum;

  @SerializedName("flex_field_1")
  private String flexField1;

  @SerializedName("flex_field_2")
  private String flexField2;

  @SerializedName("flex_field_3")
  private String flexField3;

  @SerializedName("flex_field_4")
  private String flexField4;

  @SerializedName("flex_field_5")
  private String flexField5;

  @SerializedName("flex_field_6")
  private String flexField6;

  @SerializedName("flex_field_7")
  private String flexField7;

  @SerializedName("flex_field_8")
  private String flexField8;

  @SerializedName("flex_field_9")
  private String flexField9;

  @SerializedName("flex_field_10")
  private String flexField10;

  @SerializedName("flex_field_11")
  private String flexField11;

  @SerializedName("flex_field_12")
  private String flexField12;

  @SerializedName("flex_field_13")
  private String flexField13;

  @SerializedName("flex_field_14")
  private String flexField14;

  @SerializedName("flex_field_15")
  private String flexField15;

  @SerializedName("flex_field_16")
  private String flexField16;

  @SerializedName("flex_field_17")
  private String flexField17;

  @SerializedName("flex_field_18")
  private String flexField18;

  @SerializedName("flex_field_19")
  private String flexField19;

  @SerializedName("flex_field_20")
  private String flexField20;

  @SerializedName("event_ts")
  private String eventTs;

  @SerializedName("dflt_bhrv_id")
  private int dfltBhrvId;

  @SerializedName("perf_track_name_value")
  private String perfTrackNameValue;

  @SerializedName("keyword")
  private String keyword;

  @SerializedName("kw_id")
  private long kwId;

  @SerializedName("mt_id")
  private String mtId;

  @SerializedName("geo_id")
  private String geoId;

  @SerializedName("crlp")
  private String crlp;

  @SerializedName("mfe_name")
  private String mfeName;

  @SerializedName("mfe_id")
  private int mfeId;

  @SerializedName("user_map_ind")
  private String userMapInd;

  @SerializedName("creative_id")
  private long creativeId;

  @SerializedName("test_ctrl_flag")
  private int testCtrlFlag;

  @SerializedName("transaction_type")
  private String transactionType;

  @SerializedName("transaction_id")
  private String transactionId;

  @SerializedName("item_id")
  private String itemId;

  @SerializedName("roi_item_id")
  private String roiItemId;

  @SerializedName("cart_id")
  private String cartId;

  @SerializedName("extrnl_cookie")
  private String extrnlCookie;

  @SerializedName("ebay_site_id")
  private String ebaySiteId;

  @SerializedName("rvr_url")
  private String rvrUrl;

  @SerializedName("mgvalue")
  private String mgvalue;

  @SerializedName("mgvalue_rsn_cd")
  private String mgvalueRsnCd;

  @SerializedName("cre_date")
  private String creDate;


  @SerializedName("cre_user")
  private String creUser;

  @SerializedName("upd_date")
  private String updDate;

  @SerializedName("upd_user")
  private String updUser;

  public String getBatchId() {
    return this.batchId;
  }

  public void setBatchId(final String batchId) {
    this.batchId = batchId;
  }

  public int getFileId() {
    return this.fileId;
  }

  public void setFileId(final int fileId) {
    this.fileId = fileId;
  }

  public int getFileSchmVrsn() {
    return this.fileSchmVrsn;
  }

  public void setFileSchmVrsn(final int fileSchmVrsn) {
    this.fileSchmVrsn = fileSchmVrsn;
  }

  public long getRvrId() {
    return this.rvrId;
  }

  public void setRvrId(final long rvrId) {
    this.rvrId = rvrId;
  }

  public String getEventDt() {
    return this.eventDt;
  }

  public void setEventDt(final String eventDt) {
    this.eventDt = eventDt;
  }

  public int getSrvdPstn() {
    return this.srvdPstn;
  }

  public void setSrvdPstn(final int srvdPstn) {
    this.srvdPstn = srvdPstn;
  }

  public String getRvrCmndTypeCd() {
    return this.rvrCmndTypeCd;
  }

  public void setRvrCmndTypeCd(final String rvrCmndTypeCd) {
    this.rvrCmndTypeCd = rvrCmndTypeCd;
  }

  public String getRvrChnlTypeCd() {
    return this.rvrChnlTypeCd;
  }

  public void setRvrChnlTypeCd(final String rvrChnlTypeCd) {
    this.rvrChnlTypeCd = rvrChnlTypeCd;
  }

  public String getCntryCd() {
    return this.cntryCd;
  }

  public void setCntryCd(final String cntryCd) {
    this.cntryCd = cntryCd;
  }

  public String getLangCd() {
    return this.langCd;
  }

  public void setLangCd(final String langCd) {
    this.langCd = langCd;
  }

  public int getTrckngPrtnrId() {
    return this.trckngPrtnrId;
  }

  public void setTrckngPrtnrId(final int trckngPrtnrId) {
    this.trckngPrtnrId = trckngPrtnrId;
  }

  public String getCguid() {
    return this.cguid;
  }

  public void setCguid(final String cguid) {
    this.cguid = cguid;
  }

  public String getGuid() {
    return this.guid;
  }

  public void setGuid(final String guid) {
    this.guid = guid;
  }

  public long getUserId() {
    return this.userId;
  }

  public void setUserId(final long userId) {
    this.userId = userId;
  }

  public String getClntRemoteIp() {
    return this.clntRemoteIp;
  }

  public void setClntRemoteIp(final String clntRemoteIp) {
    this.clntRemoteIp = clntRemoteIp;
  }

  public int getBrwsrTypeId() {
    return this.brwsrTypeId;
  }

  public void setBrwsrTypeId(final int brwsrTypeId) {
    this.brwsrTypeId = brwsrTypeId;
  }

  public String getBrwsrName() {
    return this.brwsrName;
  }

  public void setBrwsrName(final String brwsrName) {
    this.brwsrName = brwsrName;
  }

  public String getRfrrDmnName() {
    return this.rfrrDmnName;
  }

  public void setRfrrDmnName(final String rfrrDmnName) {
    this.rfrrDmnName = rfrrDmnName;
  }

  public String getRfrrUrl() {
    return this.rfrrUrl;
  }

  public void setRfrrUrl(final String rfrrUrl) {
    this.rfrrUrl = rfrrUrl;
  }

  public int getUrlEncrptdYnInd() {
    return this.urlEncrptdYnInd;
  }

  public void setUrlEncrptdYnInd(final int urlEncrptdYnInd) {
    this.urlEncrptdYnInd = urlEncrptdYnInd;
  }

  public long getSrcRotationId() {
    return this.srcRotationId;
  }

  public void setSrcRotationId(final long srcRotationId) {
    this.srcRotationId = srcRotationId;
  }

  public long getDstRotationId() {
    return this.dstRotationId;
  }

  public void setDstRotationId(final long dstRotationId) {
    this.dstRotationId = dstRotationId;
  }

  public int getDstClientId() {
    return this.dstClientId;
  }

  public void setDstClientId(final int dstClientId) {
    this.dstClientId = dstClientId;
  }

  public String getPblshrId() {
    return this.pblshrId;
  }

  public void setPblshrId(final String pblshrId) {
    this.pblshrId = pblshrId;
  }

  public String getLndngPageDmnName() {
    return this.lndngPageDmnName;
  }

  public void setLndngPageDmnName(final String lndngPageDmnName) {
    this.lndngPageDmnName = lndngPageDmnName;
  }

  public String getLndngPageUrl() {
    return this.lndngPageUrl;
  }

  public void setLndngPageUrl(final String lndngPageUrl) {
    this.lndngPageUrl = lndngPageUrl;
  }

  public String getUserQuery() {
    return this.userQuery;
  }

  public void setUserQuery(final String userQuery) {
    this.userQuery = userQuery;
  }

  public String getRuleBitFlagStrng() {
    return this.ruleBitFlagStrng;
  }

  public void setRuleBitFlagStrng(final String ruleBitFlagStrng) {
    this.ruleBitFlagStrng = ruleBitFlagStrng;
  }

  public int getFlexFieldVrsnNum() {
    return this.flexFieldVrsnNum;
  }

  public void setFlexFieldVrsnNum(final int flexFieldVrsnNum) {
    this.flexFieldVrsnNum = flexFieldVrsnNum;
  }

  public String getFlexField1() {
    return this.flexField1;
  }

  public void setFlexField1(final String flexField1) {
    this.flexField1 = flexField1;
  }

  public String getFlexField2() {
    return this.flexField2;
  }

  public void setFlexField2(final String flexField2) {
    this.flexField2 = flexField2;
  }

  public String getFlexField3() {
    return this.flexField3;
  }

  public void setFlexField3(final String flexField3) {
    this.flexField3 = flexField3;
  }

  public String getFlexField4() {
    return this.flexField4;
  }

  public void setFlexField4(final String flexField4) {
    this.flexField4 = flexField4;
  }

  public String getFlexField5() {
    return this.flexField5;
  }

  public void setFlexField5(final String flexField5) {
    this.flexField5 = flexField5;
  }

  public String getFlexField6() {
    return this.flexField6;
  }

  public void setFlexField6(final String flexField6) {
    this.flexField6 = flexField6;
  }

  public String getFlexField7() {
    return this.flexField7;
  }

  public void setFlexField7(final String flexField7) {
    this.flexField7 = flexField7;
  }

  public String getFlexField8() {
    return this.flexField8;
  }

  public void setFlexField8(final String flexField8) {
    this.flexField8 = flexField8;
  }

  public String getFlexField9() {
    return this.flexField9;
  }

  public void setFlexField9(final String flexField9) {
    this.flexField9 = flexField9;
  }

  public String getFlexField10() {
    return this.flexField10;
  }

  public void setFlexField10(final String flexField10) {
    this.flexField10 = flexField10;
  }

  public String getFlexField11() {
    return this.flexField11;
  }

  public void setFlexField11(final String flexField11) {
    this.flexField11 = flexField11;
  }

  public String getFlexField12() {
    return this.flexField12;
  }

  public void setFlexField12(final String flexField12) {
    this.flexField12 = flexField12;
  }

  public String getFlexField13() {
    return this.flexField13;
  }

  public void setFlexField13(final String flexField13) {
    this.flexField13 = flexField13;
  }

  public String getFlexField14() {
    return this.flexField14;
  }

  public void setFlexField14(final String flexField14) {
    this.flexField14 = flexField14;
  }

  public String getFlexField15() {
    return this.flexField15;
  }

  public void setFlexField15(final String flexField15) {
    this.flexField15 = flexField15;
  }

  public String getFlexField16() {
    return this.flexField16;
  }

  public void setFlexField16(final String flexField16) {
    this.flexField16 = flexField16;
  }

  public String getFlexField17() {
    return this.flexField17;
  }

  public void setFlexField17(final String flexField17) {
    this.flexField17 = flexField17;
  }

  public String getFlexField18() {
    return this.flexField18;
  }

  public void setFlexField18(final String flexField18) {
    this.flexField18 = flexField18;
  }

  public String getFlexField19() {
    return this.flexField19;
  }

  public void setFlexField19(final String flexField19) {
    this.flexField19 = flexField19;
  }

  public String getFlexField20() {
    return this.flexField20;
  }

  public void setFlexField20(final String flexField20) {
    this.flexField20 = flexField20;
  }

  public String getEventTs() {
    return this.eventTs;
  }

  public void setEventTs(final String eventTs) {
    this.eventTs = eventTs;
  }

  public int getDfltBhrvId() {
    return this.dfltBhrvId;
  }

  public void setDfltBhrvId(final int dfltBhrvId) {
    this.dfltBhrvId = dfltBhrvId;
  }

  public String getPerfTrackNameValue() {
    return this.perfTrackNameValue;
  }

  public void setPerfTrackNameValue(final String perfTrackNameValue) {
    this.perfTrackNameValue = perfTrackNameValue;
  }

  public String getKeyword() {
    return this.keyword;
  }

  public void setKeyword(final String keyword) {
    this.keyword = keyword;
  }

  public long getKwId() {
    return this.kwId;
  }

  public void setKwId(final long kwId) {
    this.kwId = kwId;
  }

  public String getMtId() {
    return this.mtId;
  }

  public void setMtId(final String mtId) {
    this.mtId = mtId;
  }

  public String getGeoId() {
    return this.geoId;
  }

  public void setGeoId(final String geoId) {
    this.geoId = geoId;
  }

  public String getCrlp() {
    return this.crlp;
  }

  public void setCrlp(final String crlp) {
    this.crlp = crlp;
  }

  public String getMfeName() {
    return this.mfeName;
  }

  public void setMfeName(final String mfeName) {
    this.mfeName = mfeName;
  }

  public int getMfeId() {
    return this.mfeId;
  }

  public void setMfeId(final int mfeId) {
    this.mfeId = mfeId;
  }

  public String getUserMapInd() {
    return this.userMapInd;
  }

  public void setUserMapInd(final String userMapInd) {
    this.userMapInd = userMapInd;
  }

  public long getCreativeId() {
    return this.creativeId;
  }

  public void setCreativeId(final long creativeId) {
    this.creativeId = creativeId;
  }

  public int getTestCtrlFlag() {
    return this.testCtrlFlag;
  }

  public void setTestCtrlFlag(final int testCtrlFlag) {
    this.testCtrlFlag = testCtrlFlag;
  }

  public String getTransactionType() {
    return this.transactionType;
  }

  public void setTransactionType(final String transactionType) {
    this.transactionType = transactionType;
  }

  public String getTransactionId() {
    return this.transactionId;
  }

  public void setTransactionId(final String transactionId) {
    this.transactionId = transactionId;
  }

  public String getItemId() {
    return this.itemId;
  }

  public void setItemId(final String itemId) {
    this.itemId = itemId;
  }

  public String getRoiItemId() {
    return this.roiItemId;
  }

  public void setRoiItemId(final String roiItemId) {
    this.roiItemId = roiItemId;
  }

  public String getCartId() {
    return this.cartId;
  }

  public void setCartId(final String cartId) {
    this.cartId = cartId;
  }

  public String getExtrnlCookie() {
    return this.extrnlCookie;
  }

  public void setExtrnlCookie(final String extrnlCookie) {
    this.extrnlCookie = extrnlCookie;
  }

  public String getEbaySiteId() {
    return this.ebaySiteId;
  }

  public void setEbaySiteId(final String ebaySiteId) {
    this.ebaySiteId = ebaySiteId;
  }

  public String getRvrUrl() {
    return this.rvrUrl;
  }

  public void setRvrUrl(final String rvrUrl) {
    this.rvrUrl = rvrUrl;
  }

  public String getMgvalue() {
    return this.mgvalue;
  }

  public void setMgvalue(final String mgvalue) {
    this.mgvalue = mgvalue;
  }

  public String getMgvalueRsnCd() {
    return this.mgvalueRsnCd;
  }

  public void setMgvalueRsnCd(final String mgvalueRsnCd) {
    this.mgvalueRsnCd = mgvalueRsnCd;
  }

  public String getCreDate() {
    return this.creDate;
  }

  public void setCreDate(final String creDate) {
    this.creDate = creDate;
  }

  public String getCreUser() {
    return this.creUser;
  }

  public void setCreUser(final String creUser) {
    this.creUser = creUser;
  }

  public String getUpdDate() {
    return this.updDate;
  }

  public void setUpdDate(final String updDate) {
    this.updDate = updDate;
  }

  public String getUpdUser() {
    return this.updUser;
  }

  public void setUpdUser(final String updUser) {
    this.updUser = updUser;
  }
}

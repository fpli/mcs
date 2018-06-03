package com.ebay.app.raptor.chocolate.constant;

public class RotationConstant {

  public static final String FILE_NAME_SUFFIX_TXT = ".txt";
  public static final String FILE_NAME_SUFFIX_ZIP = ".gz";

  public static final String FILE_NAME_POSITION = "positions";
  public static final String FILE_NAME_POSITION_ROTATION = "position_rotations";
  public static final String FILE_NAME_POSITION_RULES = "position_rules";
  public static final String FILE_NAME_ROI = "roi_v2";
  public static final String FILE_NAME_ROI_CREDIT = "roi_credit_v2";
  public static final String FILE_NAME_ROTATIONS = "rotations";
  public static final String FILE_NAME_ROTATION_CREATIVE = "rotation-creative";
  public static final String FILE_NAME_RULES = "rules";
  public static final String FILE_NAME_CAMPAIGN = "campaigns";
  public static final String FILE_NAME_CREATIVES = "creatives";
  public static final String FILE_NAME_DF = "df.status";
  public static final String FILE_NAME_LT = "lt_roi";
  public static final String FILE_HEADER_POSITION = "Position Id|Position Name|Size|Org Code|Active";
  public static final String FILE_HEADER_POSITION_ROTATION = "Position Id|Set Name|Rotation ID|Active|Start Date|End Date";
  public static final String FILE_HEADER_POSITION_RULES = "Position Id|Rule ID|Rule Name|Set Name|Active|Start Date|End Date";
  public static final String FILE_HEADER_ROI = "PDT_TIMESTAMP|ROI_PLACEMENT_ID|ROI_EVENT_NAME|ROI_EVENT_COUNT|COOKIE_ID|FREQUENCY|UNIQUE_ID|USER_ID|ITEM_ID|TRANSACTION_ID|CART_ID|CJ_ACTION_ID|CONVERSION_TYPE_IND|DUPLICATE|REV_SHARE_IND|LATENCY_TIME|CLICK1_UNIQUE_ID|CLICK2_UNIQUE_ID|CJ_ONLY_SID|CJ_ONLY_PID|CJ_ONLY_AID";
  public static final String FILE_HEADER_ROI_CREDIT = "CLICK UNIQUE ID|IM PLACEMENT ID|CREATIVE ID|RULE ID|CLICK TIMESTAMP|IM PLACEMENT IP|IM PLACEMENT IP COUNTRY|PERF TRACK 1|PERF TRACK 2|PERF TRACK 3|PERF TRACK 4|PERF TRACK 5|PERF TRACK 6|PERF TRACK 7|PERF TRACK 8|PERF TRACK 9|PERF TRACK 10|PERF TRACK 11|DEVICE|OS|DEVICE_TYPE|SID|PID|AID|UID|IMP_RVR_ID|CLK_RVR_ID|EXT_ID|NS_RVR_ID|PARM2_ID|PARM3_ID";
  public static final String FILE_HEADER_ROTATIONS = "Rotation ID|Rotation String|Rotation Name|Size|Channel ID|Rotation Click Thru URL|Rotation Status|Rotation Cost (Rate)|Rotation Count|Rotation Count Type|Rotation Date Start|Rotation Date End|Rotation Description|Org Code|TO-Std|TO-JS|TO-text|TO-text-tracer|Vendor ID|Vendor Name|Vendor URL|Vendor Type|Client ID|Campaign ID|Client name|Campaign Name|Placement ID|Perf track 1|Perf track 2|Perf track 3|Perf track 4|Perf track 5|Perf track 6|Perf track 7|Perf track 8|Perf track 9|Perf track 10";
  public static final String FILE_HEADER_CREATIVES = "Creative ID|Creative File Name|Creative Location|Size|Org Code|Creative Type|Campaign ID|Campaign Name|Client ID|Client Name";
  public static final String FILE_HEADER_ROTATION_CREATIVE = "Rot-Cr ID|Rotation ID|Creative ID|Creative Set Name|Weight|Creative Click Thru URL|Creative Date Start|Creative Date End|Rot-Cr Status|Org Code";
  public static final String FILE_HEADER_RULES = "RULE ID|RULE NAME|CREATIVE SET|CREATIVE ID|RULE CLICK THRU URL|ROTATION ID|ROTATION NAME|CAMPAIGN ID|CAMPAIGN NAME|CLIENT ID|CLIENT NAME";
  public static final String FILE_HEADER_CAMPAIGN = "CLIENT ID|CAMPAIGN ID|CLIENT NAME|CAMPAIGN NAME";
  public static final String FILE_HEADER_LT = "PACIFIC TIMESTAMP|LT PLACEMENT ID|IM PLACEMENT ID|CREATIVE ID|RULE ID|LT LATENCY TIME|ROI PLACEMENT ID|ROI EVENT NAME|ROI CATEGORY ID|ROI EVENT COUNT|COOKIE ID|UNIQUE ID|USER ID|ITEM ID|TRANSACTION ID|CART ID|VIEWTHRU/CLICKTHRU|DUPLICATE|LT KEYWORD|OQ KEYWORD|REFERRING DOMAIN|DESTINATION URL|CLICK_UNIQUE_ID|DEVICE|OS|DEVICE_TYPE|SID|PID|AID|UID|IMP_RVR_ID|CLK_RVR_ID|EXT_ID|NS_RVR_ID|PARM2_ID|PARM3_ID";

  public static final byte FIELD_SEPARATOR = '|';
  public static final byte RECORD_SEPARATOR = '\n';
  public static final byte ROTATION_SEPARATOR = '-';

  public static final String CHOCO_ROTATION_INFO = "rotation_info";
  public static final String FIELD_ROTATION_ID = "rid";
  public static final String CHOCO_ROTATION_ID = "rotation_id";
  public static final String CHOCO_ROTATION_TAG = "rotation_tag";

  public static final String CHOCO_SITE_ID = "site_id";
  public static final String FIELD_CHANNEL_ID = "channel_id";
  public static final String FIELD_ROTATION_NAME = "rotation_name";
  public static final String FIELD_CAMPAIGN_ID = "campaign_id";
  public static final String FIELD_CAMPAIGN_NAME = "campaign_name";
  public static final String FIELD_ROTATION_CLICK_THRU_URL = "rotation_click_thru_url";
  public static final String FIELD_ROTATION_COUNT_TYPE = "rotation_count_type";
  public static final String FIELD_ROTATION_START_DATE = "rotation_start_date";
  public static final String FIELD_ROTATION_END_DATE = "rotation_end_date";
  public static final String FIELD_ROTATION_DESCRIPTION = "rotation_description";
  public static final String FIELD_ROTATION_STATUS = "status";
  public static final String FIELD_VENDOR_ID = "vendor_id";
  public static final String FIELD_VENDOR_NAME = "vendor_name";
  public static final String FIELD_VENDOR_URL = "vendor_url";
  public static final String FIELD_VENDOR_TYPE = "active";
  public static final String FIELD_CLIENT_ID = "client_id";
  public static final String FIELD_CLIENT_NAME = "client_name";
  public static final String FIELD_PLACEMENT_ID = "placement_id";

  public static final String FIELD_TAG_SITE_NAME = "site_name";
  public static final String FIELD_TAG_CHANNEL_NAME = "channel_id";
  public static final String FIELD_TAG_PERFORMACE_STRATEGIC = "performance_strategic";
  public static final String FIELD_TAG_DEVICE = "device";
  public static final String FIELD_TAG_MEDIA_TYPE = "media_type";
  public static final String FIELD_TAG_ACTIVITY_TYPE = "activity_type";


  public static final String FIELD_MPLX_ROTATION_ID = "mplx_rotation_id";

  public static final String FIELD_CREATIVE_SETS = "creative_sets";
  public static final String FIELD_CREATIVE_ID = "creative_id";
  public static final String FIELD_CREATIVE_SET_NAME = "creative_set_name";
  public static final String FIELD_CREATIVE_FILE_NAME = "creative_file_name";
  public static final String FIELD_CREATIVE_LOCATION = "creative_location";
  public static final String FIELD_CREATIVE_WEIGHT = "creative_weight";
  public static final String FIELD_CREATIVE_CLICK_THRU_URL = "creative_click_thru_url";
  public static final String FIELD_CREATIVE_DATE_START = "creative_date_start";
  public static final String FIELD_CREATIVE_DATE_END = "creative_date_end";
  public static final String FIELD_CREATIVE_SIZE = "size";
  public static final String FIELD_CREATIVE_ORG_CODE = "org_code";
  public static final String FIELD_CREATIVE_TYPE = "creative_type";
  public static final String FIELD_PACIFIC_TIMESTAMP = "pacific_tempstamp";
  public static final String FIELD_LT_PLACEMENT_ID = "lt_placement_id";
  public static final String FIELD_IM_PLACEMENT_ID = "im_placement_id";
  public static final String FIELD_IM_PLACEMENT_IP = "im_placement_ip";
  public static final String FIELD_IM_PLACEMENT_IP_COUNTRY = "im_placement_country";
  public static final String FIELD_RULE_ID = "rule_id";
  public static final String FIELD_RULE_NAME = "rule_name";
  public static final String FIELD_LT_LATENCY_TIME = "lt_latency_time";
  public static final String FIELD_ROI_PLACEMENT_ID = "roi_placement_id";
  public static final String FIELD_ROI_EVENT_NAME = "roi_event_name";
  public static final String FIELD_ROI_CATEGORY_ID = "roi_category_id";
  public static final String FIELD_ROI_EVENT_COUNT = "roi_event_count";
  public static final String FIELD_COOKIE_ID = "cookie_id";
  public static final String FIELD_UNIQUE_ID = "unique_id";
  public static final String FIELD_USER_ID = "user_id";
  public static final String FIELD_ITEM_ID = "item_id";
  public static final String FIELD_TRANSACTION_ID = "transaction_id";
  public static final String FIELD_CART_ID = "cart_id";
  public static final String FIELD_VIEWTHRU_CLICKTHRU = "view_click_thru";
  public static final String FIELD_DUPLICATE = "duplicate";
  public static final String FIELD_LT_KEYWORD = "lt_keyword";
  public static final String FIELD_OQ_KEYWORD = "oq_keyword";
  public static final String FIELD_REFERRING_DOMAIN = "referring_domain";
  public static final String FIELD_DESTINATION_URL = "destination_url";
  public static final String FIELD_CLICK_UNIQUE_ID = "click_unique_id";
  public static final String FIELD_CLICK_TIMPSTAMP = "click_timestamp";
  public static final String FIELD_DEVICE = "device";
  public static final String FIELD_OS = "os";
  public static final String FIELD_DEVICE_TYPE = "device_type";
  public static final String FIELD_SID = "sid";
  public static final String FIELD_PID = "pid";
  public static final String FIELD_AID = "aid";
  public static final String FIELD_UID = "uid";
  public static final String FIELD_IMP_RVR_ID = "imp_rvr_id";
  public static final String FIELD_CLK_RVR_ID = "clk_rvr_id";
  public static final String FIELD_EXT_ID = "ext_id";
  public static final String FIELD_NS_RVR_ID = "ns_rvr_id";
  public static final String FIELD_PARM2_ID = "parm2_id";
  public static final String FIELD_PARM3_ID = "parm3_id";
  public static final String FIELD_POSITIONS = "posititions";
  public static final String FIELD_POSITION_RULES = "position_rules";
  public static final String FIELD_POSITION_ID = "position_id";
  public static final String FIELD_POSITION_NAME = "position_name";
  public static final String FIELD_POSITION_SIZE = "position_size";
  public static final String FIELD_POSITION_ACTIVE = "position_active";
  public static final String FIELD_POSITION_ROTATION_SET_NAME = "rotation_set_name";
  public static final String FIELD_POSITION_ROTATION_ACTIVE = "position_rotation_active";
  public static final String FIELD_POSITION_START_DATE = "position_start_date";
  public static final String FIELD_POSITION_END_DATE = "position_end_date";
  public static final String FIELD_RULE_SET_NAME = "rule_set_name";
  public static final String FIELD_RULE_ACTIVE = "rule_active";
  public static final String FIELD_RULE_START_DATE = "rule_start_date";
  public static final String FIELD_RULE_END_DATE = "rule_end_date";
  public static final String FIELD_LT_ROIS = "lt_rois";
  public static final String FIELD_ROI_CREDITS_V2 = "roi_credit_v2";
  public static final String FIELD_ROI_V2 = "roi_v2";
  public static final String FIELD_RULES = "rules";
  public static final String FIELD_PERF_TRACK_1 = "perf_track1";
  public static final String FIELD_PERF_TRACK_2 = "perf_track2";
  public static final String FIELD_PERF_TRACK_3 = "perf_track3";
  public static final String FIELD_PERF_TRACK_4 = "perf_track4";
  public static final String FIELD_PERF_TRACK_5 = "perf_track5";
  public static final String FIELD_PERF_TRACK_6 = "perf_track6";
  public static final String FIELD_PERF_TRACK_7 = "perf_track7";
  public static final String FIELD_PERF_TRACK_8 = "perf_track8";
  public static final String FIELD_PERF_TRACK_9 = "perf_track9";
  public static final String FIELD_PERF_TRACK_10 = "perf_track10";
  public static final String FIELD_PERF_TRACK_11 = "perf_track11";
  public static final String FIELD_PDT_TIMESTAMP = "pdt_timestamp";
  public static final String FIELD_FREQUENCY = "frequency";
  public static final String FIELD_CJ_ACTION_ID = "cj_action_id";
  public static final String FIELD_CONVERSION_TYPE_IND = "converstion_type_ind";
  public static final String FIELD_REV_SHARE_IND = "rev_share_ind";
  public static final String FIELD_LATENCY_TIME = "latency_time";
  public static final String FIELD_CLICK1_UNIQUE_ID = "click1_unique_id";
  public static final String FIELD_CLICK2_UNIQUE_ID = "click2_unique_id";
  public static final String FIELD_CJ_ONLY_SID = "cj_only_sid";
  public static final String FIELD_CJ_ONLY_PID = "cj_only_pid";
  public static final String FIELD_CJ_ONLY_AID = "cj_only_aid";
  public static final String FIELD_ROT_CR_ID = "rot_cr_id";
  public static final String FIELD_ROT_CR_STATUS = "rot_cr_status";

}

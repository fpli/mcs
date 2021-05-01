package com.ebay.traffic.chocolate.sparknrt.epnnrt_v2

import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema_v2
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{ByteType, DecimalType, IntegerType, ShortType, TimestampType}
import org.slf4j.LoggerFactory

class ClickDataFrame_v2(df: DataFrame, common: EpnNrtCommon_v2) extends Serializable {

  @transient lazy val schema_epn_click_table = TableSchema_v2("df_epn_click_v2.json")

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  def build(): DataFrame = {
    logger.debug("Building EPN Click dataframe")
    var clickDf  = df
      .withColumn("RFR_URL_NAME", col("referer"))
      .withColumn("google_fltr_do_flag", common.get_google_fltr_do_flag_udf(col("referer"), col("publisher_id")))
      .withColumn("traffic_source_code", common.get_trfc_src_cd_click_udf(col("user_agent")))
      .withColumn("ams_fltr_roi_value", common.get_roi_fltr_yn_ind_udf(col("uri"), col("publisher_id"), col("referer"), col("google_fltr_do_flag"), col("traffic_source_code"), col("rt_rule_flags")))
      .withColumn("last_view_item", common.get_last_view_item_info_udf(col("cguid"), col("guid"), col("timestamp")))
      .withColumn("CLICK_ID", col("short_snapshot_id").cast(DecimalType(18,0)))
      .withColumn("CHNL_ID", common.getChannelIdUdf(col("channel_type")).cast(IntegerType))
      .withColumn("CRLTN_GUID_TXT", col("cguid"))
      .withColumn("GUID_TXT", common.fixGuidUsingRoverLastClickUdf(col("guid"), col("uri")))
      .withColumn("USER_ID", common.getUserIdUdf(col("user_id"), col("guid")).cast(DecimalType(18,0)))
      .withColumn("CLNT_RMT_IP", col("remote_ip"))
      .withColumn("BRWSR_TYPE_NUM",  common.get_browser_type_udf(col("user_agent")))
      .withColumn("BRWSR_NAME", col("user_agent"))
      .withColumn("PLCMNT_DATA_TXT", common.getRelatedInfoFromUriUdf(col("uri"), lit(3), lit("mkrid")))
      .withColumn("PBLSHR_ID", col("publisher_id").cast(DecimalType(18,0)))
      .withColumn("AMS_PBLSHR_CMPGN_ID", col("campaign_id").cast(DecimalType(18,0)))
      .withColumn("AMS_TOOL_ID", common.getToolIdUdf(col("uri")).cast(DecimalType(18,0)))
      .withColumn("CSTM_ID", common.getCustomIdUdf(col("uri")))
      .withColumn("LNDNG_PAGE_URL_NAME", common.get_lnd_page_url_name_udf(col ("response_headers"), col("landing_page_url")))
      .withColumn("USER_QUERY_TXT",  common.getUserQueryTextUdf(col("uri"), lit("click")))
      .withColumn("FLEX_FLD_1_TXT",  common.getFFValueUdf(col("uri"), lit(1)))
      .withColumn("FLEX_FLD_2_TXT",  common.getFFValueUdf(col("uri"), lit(2)))
      .withColumn("FLEX_FLD_3_TXT",  common.getFFValueNotEmptyUdf(col("uri"), lit(3)))
      .withColumn("FLEX_FLD_4_TXT",  common.getFFValueNotEmptyUdf(col("uri"), lit(4)))
      .withColumn("CLICK_TS",  common.getDateTimeUdf(col("timestamp")).cast(TimestampType))
      .withColumn("LAST_VWD_ITEM_ID", col("last_view_item")(0).cast(DecimalType(18,0)))
      .withColumn("LAST_VWD_ITEM_TS", col("last_view_item")(1).cast(TimestampType))
      .withColumn("FLEX_FLD_5_TXT",  common.getFFValueNotEmptyUdf(col("uri"), lit(5)))
      .withColumn("FLEX_FLD_6_TXT",  common.getFFValueUdf(col("uri"), lit(6)))
      .withColumn("FLEX_FLD_7_TXT",  common.getFFValueUdf(col("uri"), lit(7)))
      .withColumn("FLEX_FLD_8_TXT",  common.getFFValueUdf(col("uri"), lit(8)))
      .withColumn("FLEX_FLD_9_TXT",  common.getFFValueUdf(col("uri"), lit(9)))
      .withColumn("FLEX_FLD_10_TXT",  common.getFFValueUdf(col("uri"), lit(10)))
      .withColumn("FLEX_FLD_11_TXT",  common.getFFValueUdf(col("uri"), lit(11)))
      .withColumn("FLEX_FLD_12_TXT",  common.getFFValueUdf(col("uri"), lit(12)))
      .withColumn("FLEX_FLD_13_TXT",  common.getFFValueUdf(col("uri"), lit(13)))
      .withColumn("FLEX_FLD_14_TXT",  common.getFFValueUdf(col("uri"), lit(14)))
      .withColumn("FLEX_FLD_15_TXT",  common.getFFValueUdf(col("uri"), lit(15)))
      .withColumn("FLEX_FLD_16_TXT",  common.getFFValueUdf(col("uri"), lit(16)))
      .withColumn("FLEX_FLD_17_TXT",  common.getFFValueUdf(col("uri"), lit(17)))
      .withColumn("FLEX_FLD_18_TXT",  common.getFFValueUdf(col("uri"), lit(18)))
      .withColumn("FLEX_FLD_19_TXT",  common.getFFValueUdf(col("uri"), lit(19)))
      .withColumn("FLEX_FLD_20_TXT",  common.getFFValueUdf(col("uri"), lit(20)))
      .withColumn("ICEP_FLEX_FLD_VRSN_ID",  common.get_IcepFlexFld_udf(col("uri"), lit("ffv")).cast(ShortType))
      .withColumn("ICEP_FLEX_FLD_1_TXT", common.get_IcepFlexFld1_udf(col("uri"), lit("ff1")))
      .withColumn("AMS_PRGRM_ID", common.get_ams_prgrm_id_Udf(col("uri")).cast(ByteType))
      .withColumn("ADVRTSR_ID", common.get_ams_advertise_id_Udf(col("uri"))(1).cast(ByteType))
      .withColumn("AMS_CLICK_FLTR_TYPE_ID", common.get_ams_clk_fltr_type_id_udf(col("publisher_id"), col("uri")).cast(ByteType))
      .withColumn("FLTR_YN_IND", common.get_filter_yn_ind_udf(col("rt_rule_flags"), col("nrt_rule_flags"), lit("click")).cast(ByteType))
      .withColumn("AMS_TRANS_RSN_CD", common.get_click_reason_code_udf(col("uri"), col("publisher_id"), col("campaign_id"), col("rt_rule_flags"), col("nrt_rule_flags"), col("ams_fltr_roi_value"), col("google_fltr_do_flag")).cast(ShortType))
      .withColumn("AMS_PAGE_TYPE_MAP_ID", common.get_page_id_udf(col("LNDNG_PAGE_URL_NAME"), col("uri")).cast(DecimalType(18,0)))
      .withColumn("RFRNG_DMN_NAME", common.getRefererHostUdf(col("referer")))
      .withColumn("SRC_PLCMNT_DATA_TXT", common.getRelatedInfoFromUriUdf(col("uri"), lit(3), lit("mkrid")))
      .withColumn("GEO_TRGTD_CNTRY_CD", common.get_country_locale_udf(col("request_headers"), col("lang_cd")))
      .withColumn("TOOL_LVL_OPTN_IND", common.get_lego_udf(col("uri")).cast(ByteType))
      .withColumn("GEO_TRGTD_IND", common.get_Geo_Trgtd_Ind_udf(col("uri")).cast(ByteType))
      .withColumn("PBLSHR_ACPTD_PRGRM_IND", common.get_Pblshr_Acptd_Prgrm_Ind_udf(col("uri")).cast(ByteType))
      .withColumn("INCMNG_CLICK_URL_VCTR_ID", common.get_icep_vectorid_udf(col("uri")).cast(DecimalType(18,0)))
      .withColumn("STR_NAME_TXT", common.get_icep_store_udf(col("uri")))
      .withColumn("ITEM_ID", common.get_item_id_udf(col("uri")).cast(DecimalType(18,0)))
      .withColumn("CTGRY_ID", common.get_cat_id_udf(col("uri")).cast(DecimalType(18,0)))
      .withColumn("KEYWORD_TXT", common.get_kw_udf(col("uri")))
      .withColumn("PRGRM_EXCPTN_LIST_IND", common.get_Prgrm_Excptn_List_udf(col("uri")).cast(ByteType))
      .withColumn("ROI_FLTR_YN_IND",  col("ams_fltr_roi_value").cast(ByteType))
      .withColumn("SELLER_NAME", common.get_seller_udf(col("uri")))
      .withColumn("ROVER_URL_TXT", col("uri"))
      .withColumn("UDID", common.get_udid_Udf(col("uri")))
      .withColumn("TRFC_SRC_CD",  col("traffic_source_code").cast(ByteType))
      .withColumn("ROI_RULE_VALUES",  common.get_roi_rule_value_udf(col("uri"), col("publisher_id"), col("referer"), col("google_fltr_do_flag"), col("traffic_source_code"), col("rt_rule_flags")))
      .withColumn("RT_RULE_FLAG1", common.get_rule_flag_udf(col("rt_rule_flags"), lit(11)).cast(ByteType))
      .withColumn("RT_RULE_FLAG2", common.get_rule_flag_udf(col("rt_rule_flags"), lit(1)).cast(ByteType))
      .withColumn("RT_RULE_FLAG3", common.get_rule_flag_udf(col("rt_rule_flags"), lit(14)).cast(ByteType))
      .withColumn("RT_RULE_FLAG4", common.get_rule_flag_udf(col("rt_rule_flags"), lit(15)).cast(ByteType))
      .withColumn("RT_RULE_FLAG5", common.get_rule_flag_udf(col("rt_rule_flags"), lit(10)).cast(ByteType))
      .withColumn("RT_RULE_FLAG6", common.get_rule_flag_udf(col("rt_rule_flags"), lit(5)).cast(ByteType))
      .withColumn("RT_RULE_FLAG7", common.get_rule_flag_udf(col("rt_rule_flags"), lit(2)).cast(ByteType))
      .withColumn("RT_RULE_FLAG8", common.get_rule_flag_udf(col("rt_rule_flags"), lit(12)).cast(ByteType))
      .withColumn("RT_RULE_FLAG9", common.get_rule_flag_udf(col("rt_rule_flags"), lit(13)).cast(ByteType))
      .withColumn("RT_RULE_FLAG10", common.get_rule_flag_udf(col("rt_rule_flags"), lit(6)).cast(ByteType))
      .withColumn("RT_RULE_FLAG15", common.get_rule_flag_udf(col("rt_rule_flags"), lit(4)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG37", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(0)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG39", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(1)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG43", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(2)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG51", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(5)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG53", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(6)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG54", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(3)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG56", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(4)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG72", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(9)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG73", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(10)).cast(ByteType))
      .withColumn("NRT_RULE_FLAG74", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(11)).cast(ByteType))
      .cache()

    schema_epn_click_table.filterNotColumns(clickDf.columns).foreach( e => {
      val columnType: String =schema_epn_click_table.defaultTypes(e)
      clickDf = clickDf.withColumn(e, lit(schema_epn_click_table.defaultValues(e)).cast(schema_epn_click_table.types(columnType)))
    })
    clickDf.select(schema_epn_click_table.dfColumns: _*)
  }
}

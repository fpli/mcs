package com.ebay.traffic.chocolate.sparknrt.epnnrt

import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.slf4j.LoggerFactory

class ImpressionDataFrame(df: DataFrame, common: EpnNrtCommon) extends Serializable {

  @transient lazy val schema_epn_impression_table = TableSchema("df_epn_impression.json")

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  def build(): DataFrame = {
    logger.debug("Building impression dataframe")
    var impressionDf = df
      .withColumn("RFR_URL_NAME", col("referer"))
      .withColumn("google_fltr_do_flag", common.get_google_fltr_do_flag_udf(col("referer"), col("publisher_id")))
      .withColumn("traffic_source_code", common.get_trfc_src_cd_impression_udf(col("user_agent")))
      .withColumn("ams_fltr_roi_value", common.get_roi_fltr_yn_ind_udf(col("uri"), col("publisher_id"), col("referer"), col("google_fltr_do_flag"), col("traffic_source_code"), col("rt_rule_flags")))
      .withColumn("IMPRSN_CNTNR_ID", col("short_snapshot_id"))
      .withColumn("CHNL_ID", common.getRoverUriInfoUdf(col("uri"), lit(4).cast(IntegerType)))
      .withColumn("CRLTN_GUID_TXT", col("cguid"))
      .withColumn("GUID_TXT", col("guid"))
      .withColumn("CLNT_RMT_IP", col("remote_ip"))
      .withColumn("BRWSR_TYPE_NUM", common.get_browser_type_udf(col("user_agent")))
      .withColumn("BRWSR_NAME", col("user_agent"))
      .withColumn("USER_QUERY_TXT", common.getUserQueryTextUdf(col("uri"), lit("impression")))
      .withColumn("PLCMNT_DATA_TXT", common.getRoverUriInfoUdf(col("uri"), lit(3).cast(IntegerType)))
      .withColumn("PBLSHR_ID", col("publisher_id"))
      .withColumn("AMS_PBLSHR_CMPGN_ID", col("campaign_id"))
      .withColumn("AMS_TOOL_ID", common.getToolIdUdf(col("uri")))
      .withColumn("CSTM_ID", common.getCustomIdUdf(col("uri")))
      .withColumn("FLEX_FLD_1_TXT",  common.getFFValueUdf(col("uri"), lit(1)))
      .withColumn("FLEX_FLD_2_TXT",  common.getFFValueUdf(col("uri"), lit(2)))
      .withColumn("FLEX_FLD_3_TXT",  common.getFFValueNotEmptyUdf(col("uri"), lit(3)))
      .withColumn("FLEX_FLD_4_TXT",  common.getFFValueNotEmptyUdf(col("uri"), lit(4)))
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
      .withColumn("CTX_TXT", common.getCtxUdf(col("uri")))
      .withColumn("CTX_CLD_TXT", common.getCtxCalledUdf(col("uri")))
      .withColumn("RFR_DMN_NAME", common.getRefererHostUdf(col("referer")))
      .withColumn("IMPRSN_TS", common.getDateTimeUdf(col("timestamp")))
      .withColumn("AMS_PRGRM_ID", common.get_ams_prgrm_id_Udf(col("uri"))(0))
      .withColumn("ADVRTSR_ID", common.get_ams_prgrm_id_Udf(col("uri"))(1))
      .withColumn("FILTER_YN_IND", common.get_filter_yn_ind_udf(col("rt_rule_flags"), col("nrt_rule_flags"), lit("impression")))
      .withColumn("ROVER_URL_TXT", col("uri"))
      .withColumn("CB_KW", common.getcbkwUdf(col("uri")))
      .withColumn("CB_CAT", common.getcbcatUdf(col("uri")))
      .withColumn("CB_EX_KW", common.get_cb_ex_kw_Udf(col("uri")))
      .withColumn("CB_EX_CAT", common.get_cb_ex_cat_Udf(col("uri")))
      .withColumn("FB_USED", common.get_fb_used_Udf(col("uri")))
      .withColumn("AD_FORMAT", common.get_ad_format_Udf(col("uri")))
      .withColumn("AD_CONTENT_TYPE", common.get_ad_content_type_Udf(col("uri")))
      .withColumn("LOAD_TIME", common.get_load_time_udf(col("uri")))
      .withColumn("UDID", common.get_udid_Udf(col("uri")))
      .withColumn("AMS_TRANS_RSN_CD", common.get_impression_reason_code_udf(col("uri"), col("publisher_id"), col("campaign_id"), col("rt_rule_flags"), col("nrt_rule_flags"), col("ams_fltr_roi_value"), col("google_fltr_do_flag")))
      .withColumn("TRFC_SRC_CD", col("traffic_source_code"))
      .withColumn("RT_RULE_FLAG1", common.get_rule_flag_udf(col("rt_rule_flags"), lit(11)))
      .withColumn("RT_RULE_FLAG2", common.get_rule_flag_udf(col("rt_rule_flags"), lit(1)))
      .withColumn("RT_RULE_FLAG3", common.get_rule_flag_udf(col("rt_rule_flags"), lit(14)))
      .withColumn("RT_RULE_FLAG4", common.get_rule_flag_udf(col("rt_rule_flags"), lit(15)))
      .withColumn("RT_RULE_FLAG5", common.get_rule_flag_udf(col("rt_rule_flags"), lit(10)))
      .withColumn("RT_RULE_FLAG6", common.get_rule_flag_udf(col("rt_rule_flags"), lit(5)))
      .withColumn("RT_RULE_FLAG7", common.get_rule_flag_udf(col("rt_rule_flags"), lit(2)))
      .withColumn("RT_RULE_FLAG8", common.get_rule_flag_udf(col("rt_rule_flags"), lit(12)))
      .withColumn("RT_RULE_FLAG9", common.get_rule_flag_udf(col("rt_rule_flags"), lit(13)))
      .withColumn("RT_RULE_FLAG10", common.get_rule_flag_udf(col("rt_rule_flags"), lit(6)))
      .withColumn("RT_RULE_FLAG15", common.get_rule_flag_udf(col("rt_rule_flags"), lit(4)))
      .withColumn("NRT_RULE_FLAG37", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(0)))
      .withColumn("NRT_RULE_FLAG39", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(1)))
      .withColumn("NRT_RULE_FLAG43", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(2)))
      .withColumn("NRT_RULE_FLAG51", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(5)))
      .withColumn("NRT_RULE_FLAG53", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(6)))
      .withColumn("NRT_RULE_FLAG54", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(3)))
      .withColumn("NRT_RULE_FLAG56", common.get_rule_flag_udf(col("nrt_rule_flags"), lit(4)))
      .cache()

    schema_epn_impression_table.filterNotColumns(impressionDf.columns).foreach( e => {
      impressionDf = impressionDf.withColumn(e, lit(schema_epn_impression_table.defaultValues(e)))
    })

    impressionDf.select(schema_epn_impression_table.dfColumns: _*)

  }
}
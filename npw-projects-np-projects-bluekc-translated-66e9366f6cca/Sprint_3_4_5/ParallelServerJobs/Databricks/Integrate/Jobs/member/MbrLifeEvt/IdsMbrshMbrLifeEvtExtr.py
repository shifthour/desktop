# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: IdsMbrshMbrLifeEvtCntl
# MAGIC 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 7;hf_ids_mbr_life_evt_clm_stts_not;hf_ids_mbr_life_evt_mbr_proc;hf_ids_mbr_life_evt_mbr_svrc_dt;hf_ids_mbr_life_evt_mbr_svrc_max;hf_ids_mbr_life_evt_all_clms;hf_ids_mbr_life_evt_get_max_clm;hf_ids_mbr_life_evt_mbr_clm_1
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Jagadesh Yelavarthi   2010-12-15     Alineo                                                               New ETL                                                                       IntegrateNewDevl          Steph Goddard          01/06/2011

# MAGIC Job to load IDS-MBR_LIFE_EVT table; Main source data for this table comes from Claim and claim line tables
# MAGIC Determine max claim ID for each procedure code per member
# MAGIC -Fliter out reversal Or adjusted claims
# MAGIC -Consider Subscriber and Spouse claims only
# MAGIC Pull mbr and claim information for specific procedure codes.
# MAGIC Determine the max service date for each procedure code level type per member
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycleToExtract = get_widget_value('IDSRunCycleToExtract','')
RunID = get_widget_value('RunID','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_life_event_claims = f"""SELECT 
MBR.SRC_SYS_CD_SK,
MBR.MBR_UNIQ_KEY,
SUB.SUB_UNIQ_KEY,
PROC.ALT_KEY_WORD_2 AS PROC_ALT_KEY_WORD_2,
CLM.CLM_ID,
LN.SVC_STRT_DT_SK,
CLM.CLM_STTUS_CD_SK,
CLM.CLM_SK,
MBR.MBR_SK,
SUB.SUB_SK,
MBR.MBR_RELSHP_CD_SK,
CLM.CLM_SUBTYP_CD_SK,
CLM.CLM_TYP_CD_SK,
'CLM' as LIFE_EVT_SRC_TYP_CD
FROM {IDSOwner}.CLM CLM,
     {IDSOwner}.CLM_LN LN,
     {IDSOwner}.PROC_CD PROC,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.CD_MPPNG MAP
WHERE CLM.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
  AND MAP.TRGT_CD = 'FACETS'
  AND CLM.CLM_SK = LN.CLM_SK
  AND LN.PROC_CD_SK = PROC.PROC_CD_SK
  AND (PROC.ALT_KEY_WORD_2 = 'LIVE BIRTH' OR PROC.ALT_KEY_WORD_2 = 'LOST PREG')
  AND CLM.MBR_SK = MBR.MBR_SK
  AND MBR.MBR_UNIQ_KEY <> 0
  AND MBR.MBR_UNIQ_KEY <> 1
  AND MBR.SUB_SK = SUB.SUB_SK
  AND LN.SVC_STRT_DT_SK >= '2009-01-01'
  AND CLM.LAST_UPDT_RUN_CYC_EXCTN_SK > {IDSRunCycleToExtract}"""

df_IDS_Read_Clm_data_life_event_claims = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_life_event_claims)
    .load()
)

extract_query_claim_status_cd = f"""SELECT 
SRC_CD_SK,
CD_MPPNG_SK,
TRGT_CD,
TRGT_CD_NM,
TRGT_DOMAIN_NM
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM = 'CLAIM STATUS'
  AND TRGT_CD_NM IN ('REVERSAL', 'ADJUSTED')"""

df_IDS_Read_Clm_data_claim_status_cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_claim_status_cd)
    .load()
)

extract_query_all_proxy_sub_ids = f"""SELECT 
SUB_UNIQ_KEY,
SUB_ID
FROM {IDSOwner}.SUB
WHERE SUB_ID LIKE 'PRXY%'"""

df_IDS_Read_Clm_data_all_proxy_sub_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_all_proxy_sub_ids)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

jdbc_url_dummy_hf_mbr_life_evt, jdbc_props_dummy_hf_mbr_life_evt = get_db_config(ids_secret_name)
df_dummy_hf_mbr_life_evt_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dummy_hf_mbr_life_evt)
    .options(**jdbc_props_dummy_hf_mbr_life_evt)
    .option("query", "SELECT MBR_UNIQ_KEY, MBR_LIFE_EVT_TYP_CD, CLM_SVC_STRT_DT_SK, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, MBR_LIFE_EVT_SK FROM dummy_hf_mbr_life_evt")
    .load()
)

def deduplicate_df(df_in: DataFrame, partition_cols: list) -> DataFrame:
    return dedup_sort(df_in, partition_cols, [])

df_reversals_adj = deduplicate_df(df_IDS_Read_Clm_data_claim_status_cd, ["SRC_CD_SK","CD_MPPNG_SK"]).select(
    "SRC_CD_SK","CD_MPPNG_SK","TRGT_CD","TRGT_CD_NM","TRGT_DOMAIN_NM"
)

df_proxy_sub_ids = deduplicate_df(df_IDS_Read_Clm_data_all_proxy_sub_ids, ["SUB_UNIQ_KEY"]).select(
    "SUB_UNIQ_KEY","SUB_ID"
)

df_determine_if_keep_clm_joined = (
    df_IDS_Read_Clm_data_life_event_claims.alias("life_event_claims")
    .join(df_reversals_adj.alias("reversals_adj"),
          [(F.col("life_event_claims.SRC_SYS_CD_SK")==F.col("reversals_adj.SRC_CD_SK")),
           (F.col("life_event_claims.CLM_STTUS_CD_SK")==F.col("reversals_adj.CD_MPPNG_SK"))],
          how="left")
    .join(df_hf_etrnl_cd_mppng.alias("Src_Sys_Cd_Lookup"),
          (F.col("life_event_claims.SRC_SYS_CD_SK") == F.col("Src_Sys_Cd_Lookup.CD_MPPNG_SK")),
          how="left")
    .join(df_hf_etrnl_cd_mppng.alias("mbr_relatnshp"),
          (F.col("life_event_claims.MBR_RELSHP_CD_SK") == F.col("mbr_relatnshp.CD_MPPNG_SK")),
          how="left")
    .join(df_hf_etrnl_cd_mppng.alias("clm_typ_cd"),
          (F.col("life_event_claims.CLM_TYP_CD_SK") == F.col("clm_typ_cd.CD_MPPNG_SK")),
          how="left")
    .join(df_hf_etrnl_cd_mppng.alias("clm_sub_typ_cd"),
          (F.col("life_event_claims.CLM_SUBTYP_CD_SK") == F.col("clm_sub_typ_cd.CD_MPPNG_SK")),
          how="left")
    .join(df_proxy_sub_ids.alias("proxy_sub_ids"),
          (F.col("life_event_claims.SUB_UNIQ_KEY") == F.col("proxy_sub_ids.SUB_UNIQ_KEY")),
          how="left")
)

df_determine_if_keep_clm = df_determine_if_keep_clm_joined.select(
    F.col("life_event_claims.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("life_event_claims.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("life_event_claims.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("life_event_claims.PROC_ALT_KEY_WORD_2").alias("PROC_ALT_KEY_WORD_2"),
    F.col("life_event_claims.CLM_ID").alias("CLM_ID"),
    F.col("life_event_claims.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("life_event_claims.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
    F.col("life_event_claims.CLM_SK").alias("CLM_SK"),
    F.col("life_event_claims.MBR_SK").alias("MBR_SK"),
    F.col("life_event_claims.SUB_SK").alias("SUB_SK"),
    F.col("life_event_claims.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("life_event_claims.CLM_SUBTYP_CD_SK").alias("CLM_SUBTYP_CD_SK"),
    F.col("life_event_claims.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    F.col("life_event_claims.LIFE_EVT_SRC_TYP_CD").alias("LIFE_EVT_SRC_TYP_CD"),
    F.col("reversals_adj.CD_MPPNG_SK").alias("reversals_adj_CD_MPPNG_SK"),
    F.col("mbr_relatnshp.TRGT_CD").alias("mbr_relatnshp_TRGT_CD"),
    F.col("clm_typ_cd.TRGT_CD").alias("clm_typ_cd_TRGT_CD"),
    F.col("clm_sub_typ_cd.TRGT_CD").alias("clm_sub_typ_cd_TRGT_CD"),
    F.col("proxy_sub_ids.SUB_UNIQ_KEY").alias("proxy_sub_ids_SUB_UNIQ_KEY")
)

df_determine_if_keep_clm_calc = df_determine_if_keep_clm.withColumn(
    "TestIfReversalClaim",
    F.when(F.col("reversals_adj_CD_MPPNG_SK").isNull(), F.lit("N")).otherwise(F.lit("Y"))
).withColumn(
    "TestIfSubscriberOrSpouse",
    F.when(F.col("mbr_relatnshp_TRGT_CD").isNull(), F.lit("N"))
     .otherwise(
       F.when(trim(F.col("mbr_relatnshp_TRGT_CD")) == F.lit("SUB"), F.lit("Y"))
        .otherwise(
         F.when(trim(F.col("mbr_relatnshp_TRGT_CD")) == F.lit("SPOUSE"), F.lit("Y"))
          .otherwise(F.lit("N"))
        )
     )
).withColumn(
    "TestIfMedical",
    F.when((F.col("clm_typ_cd_TRGT_CD").isNotNull()) & (trim(F.col("clm_typ_cd_TRGT_CD")) == F.lit("MED")), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "TestIfProcedureClm",
    F.when((F.col("clm_sub_typ_cd_TRGT_CD").isNotNull()) & (trim(F.col("clm_sub_typ_cd_TRGT_CD")) == F.lit("PR")), F.lit("Y")).otherwise(F.lit("N"))
).withColumn(
    "TestIfProxyClaim",
    F.when(F.col("proxy_sub_ids_SUB_UNIQ_KEY").isNull(), F.lit("N")).otherwise(F.lit("Y"))
).withColumn(
    "svEventTypeCd",
    F.when(trim(F.col("PROC_ALT_KEY_WORD_2")) == F.lit("LIVE BIRTH"), F.lit("LIVEBRTH")).otherwise(F.lit("LOSTPREGNCY"))
).withColumn(
    "TestIfWriteOutClaim",
    F.when(
        (F.col("TestIfReversalClaim")==F.lit("N")) &
        (F.col("TestIfSubscriberOrSpouse")==F.lit("Y")) &
        (F.col("TestIfMedical")==F.lit("Y")) &
        (F.col("TestIfProcedureClm")==F.lit("Y")) &
        (F.col("TestIfProxyClaim")==F.lit("N")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

df_no_rev_adj = df_determine_if_keep_clm_calc.filter(F.col("TestIfWriteOutClaim")==F.lit("Y")).select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svEventTypeCd").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SUB_SK").alias("SBR_SK"),
    F.col("LIFE_EVT_SRC_TYP_CD").alias("LIFE_EVT_SRC_TYP_CD"),
    F.col("PROC_ALT_KEY_WORD_2").alias("LIFE_EVT_TYP")
)

df_hf_mbrsh_mbr_live_brth_dedup = deduplicate_df(
    df_no_rev_adj,
    ["MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK","CLM_ID","CLM_SK","MBR_SK","SBR_SK","LIFE_EVT_SRC_TYP_CD"]
)

df_build_keys_for_max = df_hf_mbrsh_mbr_live_brth_dedup.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK","CLM_ID","CLM_SK","MBR_SK","SBR_SK","LIFE_EVT_SRC_TYP_CD"
)

df_all_claims_1 = df_hf_mbrsh_mbr_live_brth_dedup.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK","CLM_ID","CLM_SK","MBR_SK","SBR_SK","LIFE_EVT_SRC_TYP_CD","LIFE_EVT_TYP"
)

df_mbr_life_evt_max_srvc_Dt_in = df_build_keys_for_max.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_LIFE_EVT_TYP_CD").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK")
)

df_mbr_life_evt_max_srvc_Dt_out = df_mbr_life_evt_max_srvc_Dt_in.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK"
)

df_hf_ids_mbr_life_evt_mbr_svrc_dt_dedup = deduplicate_df(df_mbr_life_evt_max_srvc_Dt_out,
    ["MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK"]
).select("MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK")

df_agg_svrc_dt = (
    df_hf_ids_mbr_life_evt_mbr_svrc_dt_dedup
    .groupBy("MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK")
    .agg(F.max("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"))
)

df_mbr_last_svrc_dt = df_agg_svrc_dt.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK"
)

df_hf_mbr_life_evt_mbr_svrc_max_dedup = deduplicate_df(df_mbr_last_svrc_dt,
    ["MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK"]
)

df_mbr_mx_srv = df_hf_mbr_life_evt_mbr_svrc_max_dedup.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","SRC_SYS_CD_SK","CLM_SVC_STRT_DT_SK"
)

df_only_load_where_max_all_claims_1 = df_all_claims_1.alias("all_claims_1")
df_only_load_where_max_mbr_mx_srv = df_mbr_mx_srv.alias("mbr_mx_srv")
df_only_load_where_max_join = df_only_load_where_max_all_claims_1.join(
    df_only_load_where_max_mbr_mx_srv,
    [
        F.col("all_claims_1.MBR_UNIQ_KEY")==F.col("mbr_mx_srv.MBR_UNIQ_KEY"),
        F.col("all_claims_1.MBR_LIFE_EVT_TYP_CD")==F.col("mbr_mx_srv.MBR_LIFE_EVT_TYP_CD"),
        F.col("all_claims_1.SRC_SYS_CD_SK")==F.col("mbr_mx_srv.SRC_SYS_CD_SK"),
        F.col("all_claims_1.CLM_SVC_STRT_DT_SK")==F.col("mbr_mx_srv.CLM_SVC_STRT_DT_SK")
    ],
    how="left"
)

df_only_load_where_max = df_only_load_where_max_join.select(
    F.col("all_claims_1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("all_claims_1.MBR_LIFE_EVT_TYP_CD").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("all_claims_1.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("all_claims_1.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("all_claims_1.CLM_SK").alias("CLM_SK"),
    F.col("all_claims_1.MBR_SK").alias("MBR_SK"),
    F.col("all_claims_1.SBR_SK").alias("SBR_SK"),
    F.col("all_claims_1.CLM_ID").alias("CLM_ID"),
    F.col("all_claims_1.LIFE_EVT_SRC_TYP_CD").alias("LIFE_EVT_SRC_TYP_CD"),
    F.col("all_claims_1.LIFE_EVT_TYP").alias("LIFE_EVT_TYP"),
    F.col("mbr_mx_srv.MBR_UNIQ_KEY").alias("mbr_mx_srv_mbr_uniq_key")
)

df_get_max_clm = df_only_load_where_max.filter(F.col("mbr_mx_srv_mbr_uniq_key").isNotNull()).select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_SK","MBR_SK","SBR_SK","CLM_ID","LIFE_EVT_SRC_TYP_CD","LIFE_EVT_TYP"
)

df_hf_dm_mbr_life_evt_get_max_clm_dedup = deduplicate_df(df_get_max_clm,
    ["MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_SK","MBR_SK","SBR_SK","CLM_ID","LIFE_EVT_SRC_TYP_CD","LIFE_EVT_TYP"]
)

df_build_keys_for_max_clm_id = df_hf_dm_mbr_life_evt_get_max_clm_dedup.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_SK","MBR_SK","SBR_SK","CLM_ID","LIFE_EVT_SRC_TYP_CD"
)

df_all_claims_2 = df_hf_dm_mbr_life_evt_get_max_clm_dedup.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_SK","MBR_SK","SBR_SK","CLM_ID","LIFE_EVT_SRC_TYP_CD","LIFE_EVT_TYP"
)

df_mbr_life_evt_max_clm_id_in = df_build_keys_for_max_clm_id.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_LIFE_EVT_TYP_CD").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_mbr_life_evt_max_clm_id_out = df_mbr_life_evt_max_clm_id_in.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_ID"
)

df_hf_ids_mbr_life_evt_clm_id_dedup = deduplicate_df(df_mbr_life_evt_max_clm_id_out,
    ["MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_ID"]
)

df_get_max_clm_id = df_hf_ids_mbr_life_evt_clm_id_dedup.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_ID"
)

df_agg_max_clm_Id = (
    df_get_max_clm_id
    .groupBy("MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK")
    .agg(F.max("CLM_ID").alias("CLM_ID"))
)

df_mbr_max_clm_id = df_agg_max_clm_Id.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_ID"
)

df_hf_mbr_life_evt_mbr_clm_dedup = deduplicate_df(df_mbr_max_clm_id,
    ["MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_ID"]
).select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_ID"
)

df_max_clm_id = df_hf_mbr_life_evt_mbr_clm_dedup.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_ID"
)

df_clm_id_match_all_claims_2 = df_all_claims_2.alias("all_claims_2")
df_clm_id_match_max_clm_id = df_max_clm_id.alias("max_clm_id")
df_clm_id_match_joined = df_clm_id_match_all_claims_2.join(
    df_clm_id_match_max_clm_id,
    [
       F.col("all_claims_2.MBR_UNIQ_KEY")==F.col("max_clm_id.MBR_UNIQ_KEY"),
       F.col("all_claims_2.MBR_LIFE_EVT_TYP_CD")==F.col("max_clm_id.MBR_LIFE_EVT_TYP_CD"),
       F.col("all_claims_2.CLM_SVC_STRT_DT_SK")==F.col("max_clm_id.CLM_SVC_STRT_DT_SK"),
       F.col("all_claims_2.SRC_SYS_CD_SK")==F.col("max_clm_id.SRC_SYS_CD_SK"),
       F.col("all_claims_2.CLM_ID")==F.col("max_clm_id.CLM_ID")
    ],
    how="left"
)

df_clm_id_match = df_clm_id_match_joined.select(
    F.col("all_claims_2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("all_claims_2.MBR_LIFE_EVT_TYP_CD").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("all_claims_2.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("all_claims_2.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("all_claims_2.CLM_SK").alias("CLM_SK"),
    F.col("all_claims_2.MBR_SK").alias("MBR_SK"),
    F.col("all_claims_2.SBR_SK").alias("SBR_SK"),
    F.col("all_claims_2.CLM_ID").alias("CLM_ID"),
    F.col("all_claims_2.LIFE_EVT_SRC_TYP_CD").alias("LIFE_EVT_SRC_TYP_CD"),
    F.col("all_claims_2.LIFE_EVT_TYP").alias("LIFE_EVT_TYP"),
    F.col("max_clm_id.MBR_UNIQ_KEY").alias("max_clm_id_mbr_uniq_key")
)

df_all_members = df_clm_id_match.filter(F.col("max_clm_id_mbr_uniq_key").isNotNull()).select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CLM_SK","MBR_SK","SBR_SK","CLM_ID","LIFE_EVT_SRC_TYP_CD","LIFE_EVT_TYP"
)

df_lkup_dummy_hf_mbr_life_evt = df_dummy_hf_mbr_life_evt_read.alias("lkup")
df_all_members_alias = df_all_members.alias("all_members")

df_apply_name_fields_joined = df_all_members_alias.join(
    df_lkup_dummy_hf_mbr_life_evt,
    [
     F.col("all_members.MBR_UNIQ_KEY")==F.col("lkup.MBR_UNIQ_KEY"),
     F.col("all_members.MBR_LIFE_EVT_TYP_CD")==F.col("lkup.MBR_LIFE_EVT_TYP_CD"),
     F.col("all_members.CLM_SVC_STRT_DT_SK")==F.col("lkup.CLM_SVC_STRT_DT_SK"),
     F.col("all_members.SRC_SYS_CD_SK")==F.col("lkup.SRC_SYS_CD_SK")
    ],
    how="left"
)

df_apply_name_fields_interim = df_apply_name_fields_joined.select(
    F.col("all_members.MBR_UNIQ_KEY").alias("AM_MBR_UNIQ_KEY"),
    F.col("all_members.MBR_LIFE_EVT_TYP_CD").alias("AM_MBR_LIFE_EVT_TYP_CD"),
    F.col("all_members.CLM_SVC_STRT_DT_SK").alias("AM_CLM_SVC_STRT_DT_SK"),
    F.col("all_members.SRC_SYS_CD_SK").alias("AM_SRC_SYS_CD_SK"),
    F.col("lkup.MBR_LIFE_EVT_SK").alias("lkup_MBR_LIFE_EVT_SK"),
    F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
    F.col("all_members.CLM_SK").alias("AM_CLM_SK"),
    F.col("all_members.MBR_SK").alias("AM_MBR_SK"),
    F.col("all_members.SBR_SK").alias("AM_SBR_SK"),
    F.col("all_members.CLM_ID").alias("AM_CLM_ID"),
    F.col("all_members.LIFE_EVT_SRC_TYP_CD").alias("AM_LIFE_EVT_SRC_TYP_CD"),
    F.col("all_members.LIFE_EVT_TYP").alias("AM_LIFE_EVT_TYP")
)

df_apply_name_fields_calc = df_apply_name_fields_interim.withColumn(
    "svMbrLifeEvtSK",
    F.when(F.col("lkup_MBR_LIFE_EVT_SK").isNull(), F.lit(None)).otherwise(F.col("lkup_MBR_LIFE_EVT_SK"))
).withColumn(
    "svCrtRunCyc",
    F.when(F.col("lkup_MBR_LIFE_EVT_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = df_apply_name_fields_calc
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svMbrLifeEvtSK",<schema>,<secret_name>)

df_apply_name_fields_final = df_enriched.withColumn(
    "IsNull_lkup_MBR_LIFE_EVT_SK",
    F.when(F.col("lkup_MBR_LIFE_EVT_SK").isNull(), F.lit(True)).otherwise(F.lit(False))
)

df_updt = df_apply_name_fields_final.filter(
    F.col("IsNull_lkup_MBR_LIFE_EVT_SK") == True
).select(
    F.col("AM_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("AM_MBR_LIFE_EVT_TYP_CD").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("AM_CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("AM_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("svCrtRunCyc").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("svMbrLifeEvtSK").alias("MBR_LIFE_EVT_SK")
)

df_allMemberLifeEvents = df_apply_name_fields_final.select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lpad(F.lit("U"),10," ").alias("INSRT_UPDT_CD"),
    F.lpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.lpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.col("CurrDate").alias("FIRST_RECYC_DT"),
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.lit("IDS").alias("SRC_SYS_CD"),
    F.concat(
        F.col("AM_MBR_UNIQ_KEY"), 
        F.lit(""), 
        F.col("AM_MBR_LIFE_EVT_TYP_CD"),
        F.lit(""),
        F.col("AM_CLM_SVC_STRT_DT_SK"),
        F.lit(""),
        F.col("AM_SRC_SYS_CD_SK")
    ).alias("PRI_KEY_STRING"),
    F.col("svMbrLifeEvtSK").alias("MBR_LIFE_EVT_SK"),
    F.col("AM_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("AM_MBR_LIFE_EVT_TYP_CD").alias("MBR_LIFE_EVT_TYP_CD"),
    F.col("AM_CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("AM_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("svCrtRunCyc").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AM_CLM_SK").alias("CLM_SK"),
    F.col("AM_MBR_SK").alias("MBR_SK"),
    F.col("AM_SBR_SK").alias("SBR_SK"),
    F.col("AM_CLM_ID").alias("CLM_ID"),
    F.col("AM_LIFE_EVT_SRC_TYP_CD").alias("LIFE_EVT_SRC_TYP_CD"),
    F.col("AM_LIFE_EVT_TYP").alias("LIFE_EVT_TYP")
)

df_upsert_hf_mbr_life_evt_keys = df_updt.select(
    "MBR_UNIQ_KEY","MBR_LIFE_EVT_TYP_CD","CLM_SVC_STRT_DT_SK","SRC_SYS_CD_SK","CRT_RUN_CYC_EXCTN_SK","MBR_LIFE_EVT_SK"
)

temp_table_name_b = "STAGING.IdsMbrshMbrLifeEvtExtr_hf_mbr_life_evt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_b}", jdbc_url_dummy_hf_mbr_life_evt, jdbc_props_dummy_hf_mbr_life_evt)
(
    df_upsert_hf_mbr_life_evt_keys
    .write
    .format("jdbc")
    .option("url", jdbc_url_dummy_hf_mbr_life_evt)
    .options(**jdbc_props_dummy_hf_mbr_life_evt)
    .option("dbtable", temp_table_name_b)
    .mode("overwrite")
    .save()
)
merge_sql_b = f"""
MERGE dummy_hf_mbr_life_evt as T
USING {temp_table_name_b} as S
ON 
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.MBR_LIFE_EVT_TYP_CD = S.MBR_LIFE_EVT_TYP_CD
    AND T.CLM_SVC_STRT_DT_SK = S.CLM_SVC_STRT_DT_SK
    AND T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.MBR_LIFE_EVT_SK = S.MBR_LIFE_EVT_SK
WHEN NOT MATCHED THEN
  INSERT (MBR_UNIQ_KEY, MBR_LIFE_EVT_TYP_CD, CLM_SVC_STRT_DT_SK, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, MBR_LIFE_EVT_SK)
  VALUES (S.MBR_UNIQ_KEY, S.MBR_LIFE_EVT_TYP_CD, S.CLM_SVC_STRT_DT_SK, S.SRC_SYS_CD_SK, S.CRT_RUN_CYC_EXCTN_SK, S.MBR_LIFE_EVT_SK);
"""
execute_dml(merge_sql_b, jdbc_url_dummy_hf_mbr_life_evt, jdbc_props_dummy_hf_mbr_life_evt)

df_final = df_allMemberLifeEvents.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "MBR_LIFE_EVT_SK",
    "MBR_UNIQ_KEY",
    "MBR_LIFE_EVT_TYP_CD",
    "CLM_SVC_STRT_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "MBR_SK",
    "SBR_SK",
    "CLM_ID",
    "LIFE_EVT_SRC_TYP_CD",
    "LIFE_EVT_TYP"
)

df_final_padded = df_final \
.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")) \
.withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")) \
.withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")) \
.withColumn("CLM_SVC_STRT_DT_SK", F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " "))

write_files(
    df_final_padded,
    f"{adls_path}/key/IdsMbrLifeEvt.MbrLifeEvt.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\""
)
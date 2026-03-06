# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Aditya   Raju              06/28/2013               5114                              Create Load File for EDW Table PROV_MSG_D                         EnterpriseWrhsDevl    Peter Marshall              8/28/2013

# MAGIC Write PROV_MSG_D Data into a Sequential file for Load Job IdsEdwProv_Msg_CdDLoad.
# MAGIC Read all the Data from IDS PROV_MSG_D Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwProvMsgDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_PROV_MSG_in = f"""
SELECT
  PROV_MSG_SK,
  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
  PROV_ID,
  PROV_MSG_CD,
  EFF_DT_SK,
  PROV_SK,
  PROV_MSG_CD_SK,
  PROV_MSG_TERM_RSN_CD_SK,
  TERM_DT_SK
FROM
  {IDSOwner}.PROV_MSG PROVMSG
  LEFT JOIN {IDSOwner}.CD_MPPNG CD
    ON PROVMSG.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_PROV_MSG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_MSG_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM
  {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Cpy_cd_mpping_ProvMsgEntLkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_ProvMsgCdLkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Prov_Msg_D_Nm_temp1 = df_db2_PROV_MSG_in.alias("lnk_IdsEdwProvMsgDExtr_InABC").join(
    df_Cpy_cd_mpping_ProvMsgEntLkp.alias("ProvMsgEntLkp"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_MSG_CD_SK") == F.col("ProvMsgEntLkp.CD_MPPNG_SK"),
    "left"
)

df_lkp_Prov_Msg_D_Nm = df_lkp_Prov_Msg_D_Nm_temp1.join(
    df_Cpy_cd_mpping_ProvMsgCdLkup.alias("ProvMsgCdLkup"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_MSG_TERM_RSN_CD_SK") == F.col("ProvMsgCdLkup.CD_MPPNG_SK"),
    "left"
).select(
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_MSG_SK").alias("PROV_MSG_SK"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_ID").alias("PROV_ID"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_MSG_CD").alias("PROV_MSG_CD"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_SK").alias("PROV_SK"),
    F.col("ProvMsgEntLkp.TRGT_CD_NM").alias("PROV_MSG_NM"),
    F.col("ProvMsgCdLkup.TRGT_CD").alias("PROV_MSG_TERM_RSN_CD"),
    F.col("ProvMsgCdLkup.TRGT_CD_NM").alias("PROV_MSG_TERM_RSN_NM"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_MSG_CD_SK").alias("PROV_MSG_CD_SK"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.PROV_MSG_TERM_RSN_CD_SK").alias("PROV_MSG_TERM_RSN_CD_SK"),
    F.col("lnk_IdsEdwProvMsgDExtr_InABC.TERM_DT_SK").alias("PROV_MSG_TERM_DT_SK")
)

df_xfrm_BusinessLogic = df_lkp_Prov_Msg_D_Nm.select(
    F.col("PROV_MSG_SK").alias("PROV_MSG_SK"),
    F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_MSG_CD").alias("PROV_MSG_CD"),
    F.col("EFF_DT_SK").alias("PROV_MSG_EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROV_SK").alias("PROV_SK"),
    F.when(F.col("PROV_MSG_NM").isNull(), F.lit("UNK")).otherwise(F.col("PROV_MSG_NM")).alias("PROV_MSG_NM"),
    F.col("PROV_MSG_TERM_DT_SK").alias("PROV_MSG_TERM_DT_SK"),
    F.when(F.col("PROV_MSG_TERM_RSN_CD").isNull(), F.lit("UNK")).otherwise(F.col("PROV_MSG_TERM_RSN_CD")).alias("PROV_MSG_TERM_RSN_CD"),
    F.when(F.col("PROV_MSG_TERM_RSN_NM").isNull(), F.lit("UNKNOWN")).otherwise(F.col("PROV_MSG_TERM_RSN_NM")).alias("PROV_MSG_TERM_RSN_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_MSG_CD_SK").alias("PROV_MSG_CD_SK"),
    F.col("PROV_MSG_TERM_RSN_CD_SK").alias("PROV_MSG_TERM_RSN_CD_SK")
)

df_seq_ProvMsgDExtr_csv_load = df_xfrm_BusinessLogic.select(
    F.col("PROV_MSG_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PROV_ID"),
    F.col("PROV_MSG_CD"),
    F.rpad(F.col("PROV_MSG_EFF_DT_SK"), 10, " ").alias("PROV_MSG_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROV_SK"),
    F.col("PROV_MSG_NM"),
    F.rpad(F.col("PROV_MSG_TERM_DT_SK"), 10, " ").alias("PROV_MSG_TERM_DT_SK"),
    F.col("PROV_MSG_TERM_RSN_CD"),
    F.col("PROV_MSG_TERM_RSN_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_MSG_CD_SK"),
    F.col("PROV_MSG_TERM_RSN_CD_SK")
)

write_files(
    df_seq_ProvMsgDExtr_csv_load,
    f"{adls_path}/load/PROV_MSG_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)
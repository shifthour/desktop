# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:   IdsEdwMbrHedisMesrYrMoSeq
# MAGIC 
# MAGIC PROCESSING:     Populate HEDIS data from GDIT,
# MAGIC                              To generate the load file for EDW table HEDIS_RVW_SET_D from IDS table HEDIS_RVW_SET.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila M\(9)                           2018-04-02\(9)30001\(9)                                                     Original Programming\(9)\(9)\(9)         EntrepriseDev 2                       Jaideep Mankala       04/12/2018
# MAGIC Karthik C                              2021-05-22                   360427                                          Added new fielf VNDR_STD_FMT_IN                                              EntrepriseDev 2                         Jaideep Mankala         05/24/2021

# MAGIC Source extract from IDS HEDIS_RVW_SET
# MAGIC This job generates the load file for EDW table HEDIS_RVW_SET_D from IDS table HEDIS_RVW_SET
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner", "")
EDWRunCycle = get_widget_value("EDWRunCycle", "")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate", "")
IDSRunCycle = get_widget_value("IDSRunCycle", "")
EDWOwner = get_widget_value("EDWOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
edw_secret_name = get_widget_value("edw_secret_name", "")

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
extract_query_EDW = """SELECT DISTINCT
   HEDIS_RVW_SET_SK, 
   CRT_RUN_CYC_EXCTN_DT_SK,
   CRT_RUN_CYC_EXCTN_SK
FROM
   {}.HEDIS_RVW_SET_D""".format(EDWOwner)
df_EDW_HEDIS_RVW_SET_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_EDW)
    .load()
)

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_IDS = """SELECT DISTINCT
   HEDIS_RVW_SET_SK, 
   HEDIS_RVW_SET_NM, 
   HEDIS_RVW_SET_STRT_DT, 
   HEDIS_RVW_SET_END_DT, 
   SRC_SYS_CD, 
   LAST_UPDT_RUN_CYC_EXCTN_SK, 
   HEDIS_RVW_SET_STRT_MO_NO, 
   HEDIS_RVW_SET_STRT_YR_NO, 
   HEDIS_RVW_SET_END_MO_NO, 
   HEDIS_RVW_SET_END_YR_NO,
   VNDR_STD_FMT_IN
FROM
   {}.HEDIS_RVW_SET
WHERE
   SRC_SYS_CD NOT IN ('NA', 'UNK')
   AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {}""".format(IDSOwner, IDSRunCycle)
df_IDS_HEDIS_RVW_SET = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_IDS)
    .load()
)

df_Lkp_Edwupdate_intermediate = df_IDS_HEDIS_RVW_SET.alias("Lnk_HEDIS_RVW_SET").join(
    df_EDW_HEDIS_RVW_SET_D.alias("Lnk_HEDIS_RVW_SET_D"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_SK") == F.col("Lnk_HEDIS_RVW_SET_D.HEDIS_RVW_SET_SK"),
    "left"
)

df_Lkp_Edwupdate = df_Lkp_Edwupdate_intermediate.select(
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("Lnk_HEDIS_RVW_SET.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_HEDIS_RVW_SET_D.CRT_RUN_CYC_EXCTN_DT_SK").alias("in_CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_STRT_MO_NO").alias("HEDIS_RVW_SET_STRT_MO_NO"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_STRT_YR_NO").alias("HEDIS_RVW_SET_STRT_YR_NO"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_END_MO_NO").alias("HEDIS_RVW_SET_END_MO_NO"),
    F.col("Lnk_HEDIS_RVW_SET.HEDIS_RVW_SET_END_YR_NO").alias("HEDIS_RVW_SET_END_YR_NO"),
    F.col("Lnk_HEDIS_RVW_SET_D.CRT_RUN_CYC_EXCTN_SK").alias("in_CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_HEDIS_RVW_SET.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("in_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_HEDIS_RVW_SET.VNDR_STD_FMT_IN").alias("VNDR_STD_FMT_IN")
)

df_Trans_Business_logic = df_Lkp_Edwupdate.select(
    F.col("HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
    F.col("HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT").alias("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT").alias("HEDIS_RVW_SET_END_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(
        trim(
            F.when(F.col("in_CRT_RUN_CYC_EXCTN_DT_SK").isNotNull(), F.col("in_CRT_RUN_CYC_EXCTN_DT_SK")).otherwise("")
        ) == "",
        F.lit(EDWRunCycleDate)
    ).otherwise(F.col("in_CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("HEDIS_RVW_SET_STRT_MO_NO").alias("HEDIS_RVW_SET_STRT_MO_NO"),
    F.col("HEDIS_RVW_SET_STRT_YR_NO").alias("HEDIS_RVW_SET_STRT_YR_NO"),
    F.col("HEDIS_RVW_SET_END_MO_NO").alias("HEDIS_RVW_SET_END_MO_NO"),
    F.col("HEDIS_RVW_SET_END_YR_NO").alias("HEDIS_RVW_SET_END_YR_NO"),
    F.when(
        trim(
            F.when(F.col("in_CRT_RUN_CYC_EXCTN_SK").isNotNull(), F.col("in_CRT_RUN_CYC_EXCTN_SK")).otherwise("0")
        ) == "0",
        F.lit(EDWRunCycle)
    ).otherwise(F.col("in_CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("in_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VNDR_STD_FMT_IN").alias("VNDR_STD_FMT_IN")
)

df_final = df_Trans_Business_logic.select(
    F.col("HEDIS_RVW_SET_SK"),
    F.col("HEDIS_RVW_SET_NM"),
    F.col("HEDIS_RVW_SET_STRT_DT"),
    F.col("HEDIS_RVW_SET_END_DT"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("HEDIS_RVW_SET_STRT_MO_NO"),
    F.col("HEDIS_RVW_SET_STRT_YR_NO"),
    F.col("HEDIS_RVW_SET_END_MO_NO"),
    F.col("HEDIS_RVW_SET_END_YR_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("VNDR_STD_FMT_IN"), 1, " ").alias("VNDR_STD_FMT_IN")
)

write_files(
    df_final,
    f"{adls_path}/load/HEDIS_RVW_SET_D.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)
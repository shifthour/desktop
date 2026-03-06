# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                01/30/2009     IAD Supp/15                  Originally programmed                          devlEDW                         Steph Goddard          05/02/2009
# MAGIC SAndrew                        2010-06-29      TTR-853                         added hash clear                                 EnterpriseNewDevl          Steph Goddard          06/30/2010
# MAGIC 
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               05/31/2013        5114                              Create Load File for EDW Table PROD_SRC_SYS_HIST_D              EnterpriseWhseDevl    Pete Marshall              2013-08-08

# MAGIC Read from source table PROD_SRC_SYS_HIST from IDS.  Apply Join  when applicable to get just the needed rows forward
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC PROD_LOB_CD_SK
# MAGIC PROD_PCKG_CD_SK
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling.
# MAGIC Write PROD_SRC_SYS_HIST_D Data into a Sequential file for Load Job IdsEdwProdSrcSysHistDLoad.
# MAGIC Job Name: IdsProdSrcSysHistDDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# Obtain DB configuration
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: db2_PROD_SRC_SYS_HIST_In
extract_query_1 = f"""
SELECT 
 S.PROD_SK,
 S.PROD_EFF_DT_SK,
 COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
 S.PROD_ID,
 S.PROD_SH_NM_SK,
 PSN.PROD_SH_NM,
 S.PROD_LOB_CD_SK,
 S.PROD_PCKG_CD_SK,
 S.PROD_TERM_DT_SK,
 S.LOB_NO,
 S.PROD_ABBR,
 S.PROD_DESC
FROM {IDSOwner}.PROD_SRC_SYS_HIST S,
     {IDSOwner}.PROD_SH_NM PSN,
     {IDSOwner}.CD_MPPNG CD
WHERE
 S.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
 AND
 S.PROD_SH_NM_SK = PSN.PROD_SH_NM_SK
"""
df_db2_PROD_SRC_SYS_HIST_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_1)
    .load()
)

# Stage: db2_CD_MPPNG_In
extract_query_2 = f"""
SELECT 
 CD_MPPNG_SK,
 COALESCE(TRGT_CD,'UNK') TRGT_CD,
 COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_2)
    .load()
)

# Stage: cpy_cd_mppng
df_Ref_PckgCd_Lkp = df_db2_CD_MPPNG_In.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")
df_Ref_LobCode_Lkp = df_db2_CD_MPPNG_In.select("CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM")

# Stage: lkp_Codes
p = df_db2_PROD_SRC_SYS_HIST_In.alias("p")
rlc = df_Ref_LobCode_Lkp.alias("rlc")
rpc = df_Ref_PckgCd_Lkp.alias("rpc")

df_lkp_Codes = (
    p.join(rlc, col("p.PROD_LOB_CD_SK") == col("rlc.CD_MPPNG_SK"), "left")
     .join(rpc, col("p.PROD_PCKG_CD_SK") == col("rpc.CD_MPPNG_SK"), "left")
     .select(
         col("p.PROD_SK").alias("PROD_SK"),
         col("p.PROD_EFF_DT_SK").alias("PROD_EFF_DT_SK"),
         col("p.SRC_SYS_CD").alias("SRC_SYS_CD"),
         col("p.PROD_ID").alias("PROD_ID"),
         col("p.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
         col("p.PROD_ABBR").alias("PROD_ABBR"),
         col("p.PROD_DESC").alias("PROD_DESC"),
         col("rlc.TRGT_CD").alias("PROD_LOB_CD"),
         col("rlc.TRGT_CD_NM").alias("PROD_LOB_NM"),
         col("p.LOB_NO").alias("LOB_NO"),
         col("rpc.TRGT_CD").alias("PROD_PCKG_CD"),
         col("rpc.TRGT_CD_NM").alias("PROD_PCKG_DESC"),
         col("p.PROD_SH_NM").alias("PROD_SH_NM"),
         col("p.PROD_TERM_DT_SK").alias("PROD_TERM_DT_SK"),
         col("p.PROD_LOB_CD_SK").alias("PROD_LOB_CD_SK"),
         col("p.PROD_PCKG_CD_SK").alias("PROD_PCKG_CD_SK")
     )
)

# Stage: xfm_BusinessLogic
df_xfm_BusinessLogic = (
    df_lkp_Codes
    .withColumn("SRC_SYS_CD", when(trim(col("SRC_SYS_CD")) == "0", lit("UNK")).otherwise(trim(col("SRC_SYS_CD"))))
    .withColumn("PROD_LOB_CD", when(trim(col("PROD_LOB_CD")) == "", lit("UNK")).otherwise(trim(col("PROD_LOB_CD"))))
    .withColumn("PROD_LOB_NM", when(trim(col("PROD_LOB_NM")) == "", lit("UNK")).otherwise(trim(col("PROD_LOB_NM"))))
    .withColumn("PROD_PCKG_CD", when(trim(col("PROD_PCKG_CD")) == "", lit("UNK")).otherwise(trim(col("PROD_PckG_CD"))))
    .withColumn("PROD_PCKG_DESC", when(trim(col("PROD_PCKG_DESC")) == "", lit("UNK")).otherwise(trim(col("PROD_PCKG_DESC"))))
    .withColumn("PROD_SH_NM", when(col("PROD_SH_NM_SK").isNull(), lit("UNK")).otherwise(trim(col("PROD_SH_NM"))))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
)

# Stage: seq_PROD_SRC_SYS_HIST_D_Load
df_seq_PROD_SRC_SYS_HIST_D_Load = df_xfm_BusinessLogic.select(
    col("PROD_SK"),
    rpad(col("PROD_EFF_DT_SK"), 10, " ").alias("PROD_EFF_DT_SK"),
    col("SRC_SYS_CD"),
    rpad(col("PROD_ID"), 8, " ").alias("PROD_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PROD_SH_NM_SK"),
    rpad(col("PROD_ABBR"), 2, " ").alias("PROD_ABBR"),
    col("PROD_DESC"),
    col("PROD_LOB_CD"),
    col("PROD_LOB_NM"),
    rpad(col("LOB_NO"), 4, " ").alias("PROD_LOB_NO"),
    col("PROD_PCKG_CD"),
    col("PROD_PCKG_DESC"),
    col("PROD_SH_NM"),
    rpad(col("PROD_TERM_DT_SK"), 10, " ").alias("PROD_TERM_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROD_LOB_CD_SK"),
    col("PROD_PCKG_CD_SK")
)

write_files(
    df_seq_PROD_SRC_SYS_HIST_D_Load,
    f"{adls_path}/load/PROD_SRC_SYS_HIST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)
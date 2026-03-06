# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Aditya   Raju              08/22/2013               5114                      Create Load File for EDW Table FEE_DSCNT_D                         EnterpriseWrhsDevl          Jag Yelavarthi               2013-12-11

# MAGIC Write FEE_DSCNT_D Data into a Sequential file for Load Job IdsEdwFeeDscntDLoad.
# MAGIC Read all the Data from IDS FEE_DSCNT Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwFeeDscntDExtr
# MAGIC Write B_FEE_DSCNT_D.dat for the IdsEdwBFeeDscntDLoad Job.
# MAGIC Write FEE_DSCNT_D.dat for the IdsEdwFeeDscntDLoad Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
EdwRuncycleDate = get_widget_value("EdwRuncycleDate", "")
BeginCycle = get_widget_value("BeginCycle", "")
IDSOwner = get_widget_value("IDSOwner", "")
EDWRunCycle = get_widget_value("EDWRunCycle", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# Database config
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from db2_FEE_DSCNT_in
extract_query_db2_FEE_DSCNT_in = f"""
SELECT 
FEE_DSCNT.FEE_DSCNT_SK,
COALESCE(CD.TRGT_CD,'UNK') AS SRC_SYS_CD,
FEE_DSCNT.FEE_DSCNT_ID,
FEE_DSCNT.FNCL_LOB_SK,
FEE_DSCNT.FEE_DSCNT_CD_SK,
FEE_DSCNT.FEE_DSCNT_LOB_CD_SK
FROM {IDSOwner}.FEE_DSCNT FEE_DSCNT
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON FEE_DSCNT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
FEE_DSCNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginCycle}
"""
df_db2_FEE_DSCNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_FEE_DSCNT_in)
    .load()
)

# Read from db2_CD_MPPNG_Extr
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

# cpy_cd_mppng outputs (Ref_DiscountCode_Lkup and Ref_DiscountLOBCode_Lkup)
df_cpy_cd_mppng_Ref_DiscountCode_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_cpy_cd_mppng_Ref_DiscountLOBCode_Lkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# lkp_Codes (join df_db2_FEE_DSCNT_in with the two lookup DataFrames)
df_lkp_Codes = (
    df_db2_FEE_DSCNT_in.alias("lnk_IdsEdwFeeDscntDExtr_InABC")
    .join(
        df_cpy_cd_mppng_Ref_DiscountCode_Lkup.alias("Ref_DiscountCode_Lkup"),
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.FEE_DSCNT_CD_SK")
        == F.col("Ref_DiscountCode_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_cpy_cd_mppng_Ref_DiscountLOBCode_Lkup.alias("Ref_DiscountLOBCode_Lkup"),
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.FEE_DSCNT_LOB_CD_SK")
        == F.col("Ref_DiscountLOBCode_Lkup.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.col("Ref_DiscountCode_Lkup.TRGT_CD").alias("FEE_DSCNT_CD"),
        F.col("Ref_DiscountCode_Lkup.TRGT_CD_NM").alias("FEE_DSCNT_NM"),
        F.col("Ref_DiscountLOBCode_Lkup.TRGT_CD").alias("FEE_DSCNT_LOB_CD"),
        F.col("Ref_DiscountLOBCode_Lkup.TRGT_CD_NM").alias("FEE_DSCNT_LOB_NM"),
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.FEE_DSCNT_CD_SK").alias("FEE_DSCNT_CD_SK"),
        F.col("lnk_IdsEdwFeeDscntDExtr_InABC.FEE_DSCNT_LOB_CD_SK").alias("FEE_DSCNT_LOB_CD_SK"),
    )
)

# xfrm_BusinessLogic
df_xfrm_BusinessLogic_lnk_MainData_in = (
    df_lkp_Codes.filter((F.col("FEE_DSCNT_SK") != 0) & (F.col("FEE_DSCNT_SK") != 1))
    .select(
        F.col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        F.when(F.col("SRC_SYS_CD").isNull(), F.lit("UNK"))
        .otherwise(trim(F.col("SRC_SYS_CD")))
        .alias("SRC_SYS_CD"),
        F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        F.lit(EdwRuncycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EdwRuncycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        F.when(F.trim(F.col("FEE_DSCNT_CD")) == "", F.lit("UNK"))
        .otherwise(F.col("FEE_DSCNT_CD"))
        .alias("FEE_DSCNT_CD"),
        F.when(F.trim(F.col("FEE_DSCNT_NM")) == "", F.lit("UNK"))
        .otherwise(F.col("FEE_DSCNT_NM"))
        .alias("FEE_DSCNT_NM"),
        F.when(F.trim(F.col("FEE_DSCNT_LOB_CD")) == "", F.lit("UNK"))
        .otherwise(F.col("FEE_DSCNT_LOB_CD"))
        .alias("FEE_DSCNT_LOB_CD"),
        F.when(F.trim(F.col("FEE_DSCNT_LOB_NM")) == "", F.lit("UNK"))
        .otherwise(F.col("FEE_DSCNT_LOB_NM"))
        .alias("FEE_DSCNT_LOB_NM"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("FEE_DSCNT_CD_SK").alias("FEE_DSCNT_CD_SK"),
        F.col("FEE_DSCNT_LOB_CD_SK").alias("FEE_DSCNT_LOB_CD_SK"),
    )
)

df_xfrm_BusinessLogic_NA = (
    df_lkp_Codes.limit(1)
    .select(
        F.lit(1).alias("FEE_DSCNT_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("NA").alias("FEE_DSCNT_ID"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("FNCL_LOB_SK"),
        F.lit("NA").alias("FEE_DSCNT_CD"),
        F.lit("NA").alias("FEE_DSCNT_NM"),
        F.lit("NA").alias("FEE_DSCNT_LOB_CD"),
        F.lit("NA").alias("FEE_DSCNT_LOB_NM"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("FEE_DSCNT_CD_SK"),
        F.lit(1).alias("FEE_DSCNT_LOB_CD_SK"),
    )
)

df_xfrm_BusinessLogic_UNK = (
    df_lkp_Codes.limit(1)
    .select(
        F.lit(0).alias("FEE_DSCNT_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("UNK").alias("FEE_DSCNT_ID"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("FNCL_LOB_SK"),
        F.lit("UNK").alias("FEE_DSCNT_CD"),
        F.lit("UNK").alias("FEE_DSCNT_NM"),
        F.lit("UNK").alias("FEE_DSCNT_LOB_CD"),
        F.lit("UNK").alias("FEE_DSCNT_LOB_NM"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("FEE_DSCNT_CD_SK"),
        F.lit(0).alias("FEE_DSCNT_LOB_CD_SK"),
    )
)

# Fnl_Fee_Dscnt_D (PxFunnel)
df_Fnl_Fee_Dscnt_D = (
    df_xfrm_BusinessLogic_lnk_MainData_in.unionByName(df_xfrm_BusinessLogic_UNK)
    .unionByName(df_xfrm_BusinessLogic_NA)
)

# cpy_Main_Data
df_cpy_Main_Data_lnk_IdsEdwFeeDscntDExtr_OutABC = df_Fnl_Fee_Dscnt_D.select(
    "FEE_DSCNT_SK",
    "SRC_SYS_CD",
    "FEE_DSCNT_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "FNCL_LOB_SK",
    "FEE_DSCNT_CD",
    "FEE_DSCNT_NM",
    "FEE_DSCNT_LOB_CD",
    "FEE_DSCNT_LOB_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FEE_DSCNT_CD_SK",
    "FEE_DSCNT_LOB_CD_SK",
)
df_cpy_Main_Data_lnk_IdsEdwBFeeDscntDExtr_OutABC = df_Fnl_Fee_Dscnt_D.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
)

# seq_FEE_DSCNT_D_csv_load (PxSequentialFile)
df_seq_FEE_DSCNT_D_csv_load = df_cpy_Main_Data_lnk_IdsEdwFeeDscntDExtr_OutABC.select(
    F.col("FEE_DSCNT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("FEE_DSCNT_ID"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("FEE_DSCNT_CD"),
    F.col("FEE_DSCNT_NM"),
    F.col("FEE_DSCNT_LOB_CD"),
    F.col("FEE_DSCNT_LOB_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FEE_DSCNT_CD_SK"),
    F.col("FEE_DSCNT_LOB_CD_SK"),
)
write_files(
    df_seq_FEE_DSCNT_D_csv_load,
    f"{adls_path}/load/FEE_DSCNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# seq_B_FEE_DSCNT_D_csv_load (PxSequentialFile)
df_seq_B_FEE_DSCNT_D_csv_load = df_cpy_Main_Data_lnk_IdsEdwBFeeDscntDExtr_OutABC.select(
    F.col("SRC_SYS_CD"),
    F.col("FEE_DSCNT_ID"),
    F.col("FNCL_LOB_SK"),
)
write_files(
    df_seq_B_FEE_DSCNT_D_csv_load,
    f"{adls_path}/load/B_FEE_DSCNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)
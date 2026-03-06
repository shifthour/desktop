# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  EyeMedProvLocXfrm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC     Transformation rules applied on Extracted data information that is used to populate the IDS ProvLoc table 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham K              2020-09-23       US-261580              Original Programming.                                                        IntegrateDev2                Kalyan Neelam             2020-10-26                         
# MAGIC 
# MAGIC Goutham K               2021-05-15         US-366403            New Provider file Change to include Loc and Svc loc id    IntegrateDev1              Jeyaprasanna               2021-05-24

# MAGIC JobName: EyeMedProvLocXfrm
# MAGIC Using the PROV_ADDR Xfrm dataset to use PROV_ADDR_TYP_CD population instead of doing the same logic
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')

# Read ds_PROV_LOC_Extr (Dataset -> parquet)
df_ds_PROV_LOC_Extr = spark.read.parquet(f"{adls_path}/ds/PROV_ADDRLOC.{SrcSysCd}.xfrm.{RunID}.parquet")

# Xfrm_BusinessLogic: produce two output DataFrames
df_lnk_IdsProvLocXfrm_Out = df_ds_PROV_LOC_Extr.select(
    concat(
        col("PROV_ADDR_ID"), lit(";"),
        col("PROV_ADDR_ID"), lit(";"),
        col("PROV_ADDR_TYP_CD"), lit(";"),
        col("PROV_ADDR_EFF_DT"), lit(";"),
        col("SRC_SYS_CD")
    ).alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    lit(0).alias("PROV_LOC_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    lit(0).alias("PROV_ADDR_SK"),
    lit(0).alias("PROV_SK"),
    lit("N").alias("PRI_ADDR_IN"),
    lit("N").alias("REMIT_ADDR_IN")
)

df_lnk_ToBalLkup = df_ds_PROV_LOC_Extr.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT").alias("EFF_DT_SK")
)

# ds_PROV_LOC_Xfrm (write Dataset -> parquet)
df_ds_PROV_LOC_Xfrm = (
    df_lnk_IdsProvLocXfrm_Out
    .withColumn("PROV_ADDR_EFF_DT", rpad(col("PROV_ADDR_EFF_DT"), 10, " "))
    .withColumn("PRI_ADDR_IN", rpad(col("PRI_ADDR_IN"), 1, " "))
    .withColumn("REMIT_ADDR_IN", rpad(col("REMIT_ADDR_IN"), 1, " "))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PROV_LOC_SK",
        "PROV_ID",
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD",
        "PROV_ADDR_EFF_DT",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "PROV_ADDR_SK",
        "PROV_SK",
        "PRI_ADDR_IN",
        "REMIT_ADDR_IN"
    )
)
write_files(
    df_ds_PROV_LOC_Xfrm,
    f"{adls_path}/ds/PROV_LOC.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ds_CD_MPPNG_Lkp_Data (Dataset -> parquet)
df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# fltr_FilterData
df_fltr_FilterData = df_ds_CD_MPPNG_Lkp_Data.filter(
    (col("SRC_SYS_CD") == "FACETS") &
    (col("SRC_CLCTN_CD") == "FACETS DBO") &
    (col("TRGT_CLCTN_CD") == "IDS") &
    (col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE") &
    (col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)
df_lnkProvAddrTyp = df_fltr_FilterData.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Lookup_Fkey_Bal
df_Lookup_Fkey_Bal = df_lnk_ToBalLkup.alias("lnk_ToBalLkup").join(
    df_lnkProvAddrTyp.alias("lnkProvAddrTyp"),
    col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == col("lnkProvAddrTyp.SRC_CD"),
    "left"
).select(
    col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
)

# Xfrm_B_PROV_ADDR
df_Lnk_BProvLoc_Out = df_Lookup_Fkey_Bal.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROV_ADDR_ID").alias("PROV_ID"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    col("EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

# B_PROV_LOC (SequentialFile -> .dat)
df_B_PROV_LOC = (
    df_Lnk_BProvLoc_Out
    .withColumn("PROV_ADDR_EFF_DT_SK", rpad(col("PROV_ADDR_EFF_DT_SK"), 10, " "))
    .select("SRC_SYS_CD_SK", "PROV_ID", "PROV_ADDR_ID", "PROV_ADDR_TYP_CD_SK", "PROV_ADDR_EFF_DT_SK")
)
write_files(
    df_B_PROV_LOC,
    f"{adls_path}/load/B_PROV_LOC.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)
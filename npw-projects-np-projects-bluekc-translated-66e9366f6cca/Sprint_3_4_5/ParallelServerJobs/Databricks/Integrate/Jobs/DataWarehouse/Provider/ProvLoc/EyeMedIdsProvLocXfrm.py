# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  EyeMedIdsProvLocXfrm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC     Transformation rules applied on Extracted data information that is used to populate the IDS ProvLoc table 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Madhavan B                   2018-03-20         5744                                Initial Programming                                             IntegrateDev2              Kalyan Neelam              2018-04-05
# MAGIC 
# MAGIC Goutham K                     2021-05-21         US-366403            New Provider file Change to include Loc and Svc loc id     IntegrateDev1       Jeyaprasanna               2021-05-24

# MAGIC JobName: EyeMedIdsProvLocXfrm
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
SrcSysCd = get_widget_value("SrcSysCd","")

df_ds_PROV_ADDR_Extr = spark.read.parquet(
    f"{adls_path}/ds/PROV_LOC_CLM.{SrcSysCd}.xfrm.{RunID}.parquet"
)
df_ds_PROV_ADDR_Extr = df_ds_PROV_ADDR_Extr.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "PROV_ADDR_SK",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SRC_SYS_CD_SK",
    "PROV_ADDR_CNTY_CLS_CD",
    "PROV_ADDR_GEO_ACES_RTRN_CD",
    "PROV_ADDR_METRORURAL_COV_CD",
    "PROV_ADDR_TERM_RSN_CD",
    "HCAP_IN",
    "PDX_24_HR_IN",
    "PRCTC_LOC_IN",
    "PROV_ADDR_DIR_IN",
    "TERM_DT",
    "ADDR_LN_1",
    "ADDR_LN_2",
    "ADDR_LN_3",
    "CITY_NM",
    "PROV_ADDR_ST_CD",
    "POSTAL_CD",
    "CNTY_NM",
    "PHN_NO",
    "PHN_NO_EXT",
    "FAX_NO",
    "FAX_NO_EXT",
    "EMAIL_ADDR_TX",
    "LAT_TX",
    "LONG_TX",
    "PRAD_TYPE_MAIL",
    "PROV2_PRAD_EFF_DT",
    "PROV_ADDR_TYP_CD_ORIG",
    "PROV_ID"
)

df_Xfrm_BusinessLogic_out_lnk_IdsProvLocXfrm = df_ds_PROV_ADDR_Extr.select(
    (
        F.col("PROV_ADDR_ID")
        + F.lit(";")
        + F.col("PROV_ADDR_ID")
        + F.lit(";")
        + F.col("PROV_ADDR_TYP_CD")
        + F.lit(";")
        + F.col("PROV_ADDR_EFF_DT")
        + F.lit(";")
        + F.col("SRC_SYS_CD")
    ).alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.lit(0).alias("PROV_LOC_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("PROV_ADDR_SK"),
    F.lit(0).alias("PROV_SK"),
    F.lit("N").alias("PRI_ADDR_IN"),
    F.lit("N").alias("REMIT_ADDR_IN")
)

df_Xfrm_BusinessLogic_out_lnk_ToBalLkup = df_ds_PROV_ADDR_Extr.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT").alias("EFF_DT_SK")
)

df_idsprovlocxfrm_temp = df_Xfrm_BusinessLogic_out_lnk_IdsProvLocXfrm.withColumn(
    "PROV_ADDR_EFF_DT",
    F.rpad(F.col("PROV_ADDR_EFF_DT"), 10, " ")
).withColumn(
    "PRI_ADDR_IN",
    F.rpad(F.col("PRI_ADDR_IN"), 1, " ")
).withColumn(
    "REMIT_ADDR_IN",
    F.rpad(F.col("REMIT_ADDR_IN"), 1, " ")
)

df_idsprovlocxfrm_temp = df_idsprovlocxfrm_temp.select(
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

write_files(
    df_idsprovlocxfrm_temp,
    f"{adls_path}/ds/PROV_LOC.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    '"',
    None
)

df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(
    f"{adls_path}/ds/CD_MPPNG.parquet"
)
df_ds_CD_MPPNG_Lkp_Data = df_ds_CD_MPPNG_Lkp_Data.select(
    "CD_MPPNG_SK",
    "SRC_CD",
    "SRC_CD_NM",
    "SRC_CLCTN_CD",
    "SRC_DRVD_LKUP_VAL",
    "SRC_DOMAIN_NM",
    "SRC_SYS_CD",
    "TRGT_CD",
    "TRGT_CD_NM",
    "TRGT_CLCTN_CD",
    "TRGT_DOMAIN_NM"
)

df_fltr_FilterData_out_lnkProvAddrTyp = df_ds_CD_MPPNG_Lkp_Data.filter(
    (F.col("SRC_SYS_CD") == "FACETS")
    & (F.col("SRC_CLCTN_CD") == "FACETS DBO")
    & (F.col("TRGT_CLCTN_CD") == "IDS")
    & (F.col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
    & (F.col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
).select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_Lookup_Fkey_Bal_out_Snapshot = df_Xfrm_BusinessLogic_out_lnk_ToBalLkup.alias("lnk_ToBalLkup").join(
    df_fltr_FilterData_out_lnkProvAddrTyp.alias("lnkProvAddrTyp"),
    F.col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == F.col("lnkProvAddrTyp.SRC_CD"),
    "left"
)

df_Lookup_Fkey_Bal_out_Snapshot = df_Lookup_Fkey_Bal_out_Snapshot.select(
    F.col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
)

df_Xfrm_B_PROV_ADDR_out_Lnk_BProvLoc_Out = df_Lookup_Fkey_Bal_out_Snapshot.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

df_B_PROV_LOC_final = df_Xfrm_B_PROV_ADDR_out_Lnk_BProvLoc_Out.withColumn(
    "PROV_ADDR_EFF_DT_SK",
    F.rpad(F.col("PROV_ADDR_EFF_DT_SK"), 10, " ")
)

df_B_PROV_LOC_final = df_B_PROV_LOC_final.select(
    "SRC_SYS_CD_SK",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK"
)

write_files(
    df_B_PROV_LOC_final,
    f"{adls_path}/load/B_PROV_LOC.{SrcSysCd}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)
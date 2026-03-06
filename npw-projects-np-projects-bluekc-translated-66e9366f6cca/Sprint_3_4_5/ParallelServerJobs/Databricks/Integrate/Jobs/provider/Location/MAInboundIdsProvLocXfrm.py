# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  MAInboundIdsProvLocXfrm
# MAGIC Callimg job Name: MAInboundProvExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Job to process MA Inbound Encounter Claims into IDS PROV LOC
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           	 Date                	User Story #        	Change Description                                               Project                    		Reviewed By                           Reviewed Date
# MAGIC -------------------------      ---------------------   	----------------   	--------------------------------------------------------             -----------------------------------------     	-------------------------  		-------------------
# MAGIC Lokesh K                 2021-11-29               US 404552                Initial programming                                     IntegrateDev2                                     Jeyaprasanna                         2022-02-05

# MAGIC JobName: MA InboundIdsProvLocXfrm
# MAGIC Using the PROV_ADDR Xfrm dataset to use PROV_ADDR_TYP_CD population instead of doing the same logic
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')

df_ds_PROV_ADDR_Extr = spark.read.parquet(f"{adls_path}/ds/PROV_ADDR.{SrcSysCd}.xfrm.{RunID}.parquet")

df_lnk_IdsProvLocXfrm_Out = (
    df_ds_PROV_ADDR_Extr
    .withColumn(
        "PRI_NAT_KEY_STRING",
        concat(
            col("PROV_ADDR_ID"), lit(";"),
            col("PROV_ADDR_ID"), lit(";"),
            col("PROV_ADDR_TYP_CD"), lit(";"),
            col("PROV_ADDR_EFF_DT"), lit(";"),
            col("SRC_SYS_CD")
        )
    )
    .withColumn("FIRST_RECYC_TS", col("FIRST_RECYC_TS"))
    .withColumn("PROV_LOC_SK", lit(0))
    .withColumn("PROV_ID", col("PROV_ADDR_ID"))
    .withColumn("PROV_ADDR_ID", col("PROV_ADDR_ID"))
    .withColumn("PROV_ADDR_TYP_CD", col("PROV_ADDR_TYP_CD"))
    .withColumn("PROV_ADDR_EFF_DT", col("PROV_ADDR_EFF_DT"))
    .withColumn("SRC_SYS_CD", col("SRC_SYS_CD"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("SRC_SYS_CD_SK", col("SRC_SYS_CD_SK"))
    .withColumn("PROV_ADDR_SK", lit(0))
    .withColumn("PROV_SK", lit(0))
    .withColumn("PRI_ADDR_IN", col("Primary_Location_Indicator"))
    .withColumn("REMIT_ADDR_IN", lit("N"))
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

df_lnk_ToBalLkup = df_ds_PROV_ADDR_Extr.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    col("PROV_ADDR_EFF_DT").alias("EFF_DT_SK")
)

write_files(
    df_lnk_IdsProvLocXfrm_Out.select(
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
    ),
    f"{adls_path}/ds/PROV_LOC.{SrcSysCd}.xfrm.{RunID}.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

df_ds_CD_MPPNG_Lkp_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_Lnk_Codes_Lkp = df_ds_CD_MPPNG_Lkp_Data.select(
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

df_lnkProvAddrTyp = (
    df_Lnk_Codes_Lkp
    .filter(
        (col("SRC_SYS_CD") == "FACETS") &
        (col("SRC_CLCTN_CD") == "FACETS DBO") &
        (col("TRGT_CLCTN_CD") == "IDS") &
        (col("SRC_DOMAIN_NM") == "PROVIDER ADDRESS TYPE") &
        (col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
    )
    .select(
        col("SRC_CD").alias("SRC_CD"),
        col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_Lookup_Fkey_Bal = (
    df_lnk_ToBalLkup.alias("lnk_ToBalLkup")
    .join(
        df_lnkProvAddrTyp.alias("lnkProvAddrTyp"),
        on=[col("lnk_ToBalLkup.PROV_ADDR_TYP_CD") == col("lnkProvAddrTyp.SRC_CD")],
        how="left"
    )
)

df_Snapshot = df_Lookup_Fkey_Bal.select(
    col("lnk_ToBalLkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_ToBalLkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("lnkProvAddrTyp.CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    col("lnk_ToBalLkup.EFF_DT_SK").alias("EFF_DT_SK")
)

df_Lnk_BProvLoc_Out = df_Snapshot.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("PROV_ADDR_ID").alias("PROV_ID"),
    col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    col("EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK")
)

df_final_B_PROV_LOC = df_Lnk_BProvLoc_Out.withColumn(
    "PROV_ADDR_EFF_DT_SK",
    rpad("PROV_ADDR_EFF_DT_SK", 10, " ")
).select(
    "SRC_SYS_CD_SK",
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD_SK",
    "PROV_ADDR_EFF_DT_SK"
)

write_files(
    df_final_B_PROV_LOC,
    f"{adls_path}/load/B_PROV_LOC.{SrcSysCd}.dat.{RunID}",
    ',',
    'overwrite',
    False,
    False,
    '^',
    None
)
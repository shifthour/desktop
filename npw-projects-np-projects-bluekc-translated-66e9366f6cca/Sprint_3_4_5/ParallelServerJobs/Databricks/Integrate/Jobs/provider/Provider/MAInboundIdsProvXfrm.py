# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT2021 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  MAInboundIdsProvXfrm
# MAGIC Calling Job:  MAInboundProvExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Control job to process MA Inbound Encounter Claims into IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           	 Date                	User Story #        	Change Description                                               Project                    		Reviewed By                           Reviewed Date
# MAGIC -------------------------      ---------------------   	----------------   	--------------------------------------------------------             -----------------------------------------     	-------------------------  		-------------------
# MAGIC Lokesh K                 2021-11-29               US 404552                Initial programming                                     IntegrateDev2                                     Jeyaprasanna                          2020-02-05

# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC JobName: MA InboundIdsProvXfrm
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
from pyspark.sql.functions import col, lit, when, rpad, trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# COMMAND ----------

SrcSysCd = get_widget_value("SrcSysCd", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")
IDSOwner = get_widget_value("$IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
RunID = get_widget_value("RunID", "")
RunIDTimeStamp = get_widget_value("RunIDTimeStamp", "")
# COMMAND ----------

df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_Fltr_CdMppngExtr = (
    df_ds_CD_MPPNG_Data
    .filter(
        (col("SRC_CD") == "P")
        & (col("SRC_SYS_CD") == "FACETS")
        & (col("SRC_CLCTN_CD") == "FACETS DBO")
        & (col("SRC_DOMAIN_NM") == "PROVIDER ENTITY")
        & (col("TRGT_CLCTN_CD") == "IDS")
        & (col("TRGT_DOMAIN_NM") == "PROVIDER ENTITY")
    )
    .select(
        col("SRC_CD").alias("SRC_CD"),
        col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.lit(1).alias("LKP_VAL")
    )
)
# COMMAND ----------

df_ds_PROV_Inbound = spark.read.parquet(f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet")
df_Xfrm_BusinessLogic = (
    df_ds_PROV_Inbound
    .select(
        col("PROVIDER_ID").alias("PROVIDER_ID"),
        col("PROVIDER_NAME").alias("PROVIDER_NAME"),
        col("TYPE_OF_PROV").alias("TYPE_OF_PROV"),
        F.lit(1).alias("LKP_VAL"),
        col("PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
        col("PROV_TAX_ID").alias("PROV_TAX_ID"),
        col("PROV_TXNMY_CD").alias("PROV_TXNMY_CD")
    )
)
# COMMAND ----------

df_Xfrm_BusinessLogic_with_col = df_Xfrm_BusinessLogic.withColumn("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", F.lit(None).cast("int"))
df_Lkup_Codes = (
    df_Xfrm_BusinessLogic_with_col.alias("lnk_Inbound_ProvLkup_in")
    .join(
        df_Fltr_CdMppngExtr.alias("ref_ProvEntyCdSK"),
        (
            (col("lnk_Inbound_ProvLkup_in.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK") == col("ref_ProvEntyCdSK.CD_MPPNG_SK"))
            & (col("lnk_Inbound_ProvLkup_in.LKP_VAL") == col("ref_ProvEntyCdSK.LKP_VAL"))
        ),
        "left"
    )
    .select(
        col("lnk_Inbound_ProvLkup_in.PROVIDER_ID").alias("PROVIDER_ID"),
        col("lnk_Inbound_ProvLkup_in.PROVIDER_NAME").alias("PROVIDER_NAME"),
        col("lnk_Inbound_ProvLkup_in.TYPE_OF_PROV").alias("TYPE_OF_PROV"),
        col("ref_ProvEntyCdSK.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        col("lnk_Inbound_ProvLkup_in.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
        col("lnk_Inbound_ProvLkup_in.PROV_TAX_ID").alias("PROV_TAX_ID"),
        col("lnk_Inbound_ProvLkup_in.PROV_TXNMY_CD").alias("PROV_TXNMY_CD")
    )
)
# COMMAND ----------

df_Xfrm_BusinessRules_out1 = (
    df_Lkup_Codes
    .select(
        F.concat(col("PROVIDER_ID"), F.lit(";"), F.lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
        F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
        F.lit(0).alias("PROV_SK"),
        col("PROVIDER_ID").alias("PROV_ID"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.lit("").alias("CMN_PRCT"),
        F.lit("NA").alias("REL_GRP_PROV"),
        F.lit("NA").alias("REL_IPA_PROV"),
        F.lit("NA").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
        F.lit("NA").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
        F.lit("NA").alias("PROV_CLM_PAYMT_METH_CD"),
        F.lit("PRCTR").alias("PROV_ENTY_CD"),
        F.lit("NA").alias("PROV_FCLTY_TYP_CD"),
        F.lit("NA").alias("PROV_PRCTC_TYP_CD"),
        F.lit("NA").alias("PROV_SVC_CAT_CD"),
        F.lit("NA").alias("PROV_SPEC_CD"),
        F.lit("NA").alias("PROV_STTUS_CD"),
        F.lit("NA").alias("PROV_TERM_RSN_CD"),
        F.lit("NA").alias("PROV_TYP_CD"),
        F.rpad(F.lit("2199-12-31"), 10, " ").alias("TERM_DT"),
        F.rpad(F.lit("2199-12-31"), 10, " ").alias("PAYMT_HOLD_DT"),
        F.lit("NA").alias("CLRNGHOUSE_ID"),
        F.lit("NA").alias("EDI_DEST_ID"),
        F.lit("").alias("EDI_DEST_QUAL"),
        col("PROV_NTNL_PROV_ID").alias("NTNL_PROV_ID"),
        col("PROVIDER_ID").alias("PROV_ADDR_ID"),
        when(trim(col("PROVIDER_NAME")) == "", F.lit("NA")).otherwise(col("PROVIDER_NAME")).alias("PROV_NM"),
        col("PROV_TAX_ID").alias("TAX_ID"),
        col("PROV_TXNMY_CD").alias("TXNMY_CD")
    )
)

df_Xfrm_BusinessRules_out2 = (
    df_Lkup_Codes
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("PROVIDER_ID").alias("PROV_ID"),
        F.lit(1).alias("CMN_PRCT_SK"),
        F.lit(1).alias("REL_GRP_PROV_SK"),
        F.lit(1).alias("REL_IPA_PROV_SK"),
        when(col("CD_MPPNG_SK").isNull(), F.lit(0)).otherwise(col("CD_MPPNG_SK")).alias("PROV_ENTY_CD_SK")
    )
)
# COMMAND ----------

write_files(
    df_Xfrm_BusinessRules_out1.select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PROV_SK",
        "PROV_ID",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "CMN_PRCT",
        "REL_GRP_PROV",
        "REL_IPA_PROV",
        "PROV_CAP_PAYMT_EFT_METH_CD",
        "PROV_CLM_PAYMT_EFT_METH_CD",
        "PROV_CLM_PAYMT_METH_CD",
        "PROV_ENTY_CD",
        "PROV_FCLTY_TYP_CD",
        "PROV_PRCTC_TYP_CD",
        "PROV_SVC_CAT_CD",
        "PROV_SPEC_CD",
        "PROV_STTUS_CD",
        "PROV_TERM_RSN_CD",
        "PROV_TYP_CD",
        "TERM_DT",
        "PAYMT_HOLD_DT",
        "CLRNGHOUSE_ID",
        "EDI_DEST_ID",
        "EDI_DEST_QUAL",
        "NTNL_PROV_ID",
        "PROV_ADDR_ID",
        "PROV_NM",
        "TAX_ID",
        "TXNMY_CD"
    ),
    f"{adls_path}/ds/PROV.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_Xfrm_BusinessRules_out2.select(
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "CMN_PRCT_SK",
        "REL_GRP_PROV_SK",
        "REL_IPA_PROV_SK",
        "PROV_ENTY_CD_SK"
    ),
    f"{adls_path}/load/B_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)
# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Update Web data mart with paid date from Argus monthly file
# MAGIC       
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Update Data Mart with paid date from Argus monthly file
# MAGIC                Update Data Mart CLM_DM_CLM, CLM_DM_CLM_LN, and CLM_DM_INIT_CLM 
# MAGIC               If claim on input does not have matching claim in IDS, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                                                                     Project #                 Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------                                           ----------------               ------------------------------------       ----------------------------           ----------------
# MAGIC Sharon Andrew     11/01/2008                 original programming                                                                                      3784 PBM               devlIDSnew                       Steph Goddard               11/07/2008
# MAGIC Brent Leland         11-25-2008                   Removed writing to unmatched exception file.  Done in IDS updt                3567 Primary Key     devlIDS
# MAGIC Dan Long              4/19/2013                   Changed the Performance parameters to the new standard values 
# MAGIC                                                                   of 512 for the buffer size and 300 for the timeout value. No code logic        TTR-1492                IntegrateNewDevl
# MAGIC                                                                   was modified.

# MAGIC Claim IDs for missing records in EDW
# MAGIC Read weekly Invoice file from ESI  ESIDrugClmInvoiceLand
# MAGIC Direct update of data mart tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SourceSys = get_widget_value("SourceSys","ESI")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","100")
RunCycleDate = get_widget_value("RunCycleDate","")
CurrentDate = get_widget_value("CurrentDate","")
ClmMartOwner = get_widget_value("ClmMartOwner","")
clmmart_secret_name = get_widget_value("clmmart_secret_name","")

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

schema_ESI_Invoice = StructType([
    StructField("RCRD_ID", DecimalType(38,10), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("PRCSR_NO", DecimalType(38,10), False),
    StructField("MEM_CK_KEY", IntegerType(), False),
    StructField("BTCH_NO", DecimalType(38,10), False),
    StructField("PDX_NO", StringType(), False),
    StructField("RX_NO", DecimalType(38,10), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("NDC_NO", DecimalType(38,10), False),
    StructField("DRUG_DESC", StringType(), False),
    StructField("NEW_RFL_CD", DecimalType(38,10), False),
    StructField("METRIC_QTY", DecimalType(38,10), False),
    StructField("DAYS_SUPL", DecimalType(38,10), False),
    StructField("BSS_OF_CST_DTRM", StringType(), False),
    StructField("INGR_CST", DecimalType(38,10), False),
    StructField("DISPNS_FEE", DecimalType(38,10), False),
    StructField("COPAY_AMT", DecimalType(38,10), False),
    StructField("SLS_TAX", DecimalType(38,10), False),
    StructField("AMT_BILL", DecimalType(38,10), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("DOB", DecimalType(38,10), False),
    StructField("SEX_CD", DecimalType(38,10), False),
    StructField("CARDHLDR_ID_NO", StringType(), False),
    StructField("RELSHP_CD", DecimalType(38,10), False),
    StructField("GRP_NO", StringType(), False),
    StructField("HOME_PLN", StringType(), False),
    StructField("HOST_PLN", DecimalType(38,10), False),
    StructField("PRESCRIBER_ID", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("CARDHLDR_FIRST_NM", StringType(), False),
    StructField("CARDHLDR_LAST_NM", StringType(), False),
    StructField("PRAUTH_NO", DecimalType(38,10), False),
    StructField("PA_MC_SC_NO", StringType(), False),
    StructField("CUST_LOC", DecimalType(38,10), False),
    StructField("RESUB_CYC_CT", DecimalType(38,10), False),
    StructField("DT_RX_WRTN", DecimalType(38,10), False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), False),
    StructField("PRSN_CD", StringType(), False),
    StructField("OTHR_COV_CD", DecimalType(38,10), False),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), False),
    StructField("CMPND_CD", DecimalType(38,10), False),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), False),
    StructField("LVL_OF_SVC", DecimalType(38,10), False),
    StructField("RX_ORIG_CD", DecimalType(38,10), False),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), False),
    StructField("PRI_PRESCRIBER", StringType(), False),
    StructField("CLNC_ID_NO", DecimalType(38,10), False),
    StructField("DRUG_TYP", DecimalType(38,10), False),
    StructField("PRESCRIBER_LAST_NM", StringType(), False),
    StructField("POSTAGE_AMT_CLMED", DecimalType(38,10), False),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), False),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), False),
    StructField("FULL_AWP", DecimalType(38,10), False),
    StructField("EXPNSN_AREA", StringType(), False),
    StructField("MSTR_CAR", StringType(), False),
    StructField("SUB_CAR", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ESI_SUB_GRP", StringType(), False),
    StructField("PLN_DSGNR", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("ADMIN_FEE", DecimalType(38,10), False),
    StructField("CAP_AMT", DecimalType(38,10), False),
    StructField("INGR_CST_SUB", DecimalType(38,10), False),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), False),
    StructField("MBR_PAY_CD", StringType(), False),
    StructField("INCNTV_FEE", DecimalType(38,10), False),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), False),
    StructField("CLM_ADJ_CD", StringType(), False),
    StructField("FRMLRY_FLAG", StringType(), False),
    StructField("GNRC_CLS_NO", StringType(), False),
    StructField("THRPTC_CLS_AHFS", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("BILL_BSS_CD", StringType(), False),
    StructField("USL_AND_CUST_CHRG", DecimalType(38,10), False),
    StructField("PD_DT", DecimalType(38,10), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", DecimalType(38,10), False),
    StructField("FEE_AMT", DecimalType(38,10), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", DecimalType(38,10), False),
    StructField("ESI_ANCLRY_AMT", DecimalType(38,10), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", DecimalType(38,10), False),
    StructField("FSA_VNDR_CD", StringType(), False),
    StructField("PICA_DRUG_CD", StringType(), False),
    StructField("AMT_CLMED", DecimalType(38,10), False),
    StructField("AMT_DSALW", DecimalType(38,10), False),
    StructField("FED_DRUG_CLS_CD", StringType(), False),
    StructField("DEDCT_AMT", DecimalType(38,10), False),
    StructField("BNF_COPAY_100", StringType(), False),
    StructField("CLM_PRCS_TYP", StringType(), False),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), False),
    StructField("FLR", StringType(), False),
    StructField("MCARE_D_COV_DRUG", StringType(), False),
    StructField("RETRO_LICS_CD", StringType(), False),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), False),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), False),
    StructField("MED_B_DRUG", StringType(), False),
    StructField("MED_B_CLM", StringType(), False),
    StructField("PRESCRIBER_QLFR", StringType(), False),
    StructField("PRESCRIBER_ID_NPI", StringType(), False),
    StructField("PDX_QLFR", StringType(), False),
    StructField("PDX_ID_NPI", StringType(), False),
    StructField("HRA_APLD_AMT", DecimalType(38,10), False),
    StructField("ESI_THER_CLS", DecimalType(38,10), False),
    StructField("HIC_NO", StringType(), False),
    StructField("HRA_FLAG", StringType(), False),
    StructField("DOSE_CD", DecimalType(38,10), False),
    StructField("LOW_INCM", StringType(), False),
    StructField("RTE_OF_ADMIN", StringType(), False),
    StructField("DEA_SCHD", DecimalType(38,10), False),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), False),
    StructField("GNRC_PROD_IN_GPI", DecimalType(38,10), False),
    StructField("PRESCRIBER_SPEC", StringType(), False),
    StructField("VAL_CD", StringType(), False),
    StructField("PRI_CARE_PDX", StringType(), False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False)
])

df_ESI_Invoice = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_ESI_Invoice)
    .load(f"{adls_path}/verified/ESIDrugClmInvoicePaidUpdt.dat.{RunID}")
)

df_DataMart = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_clmmart)
    .options(**jdbc_props_clmmart)
    .option("query", f"SELECT SRC_SYS_CD, CLM_ID FROM {ClmMartOwner}.CLM_DM_CLM")
    .load()
)

df_TrnsGetSrcCd = df_ESI_Invoice.select(
    F.col("CLAIM_ID").alias("CLAIM_ID"),
    F.lit(SourceSys).alias("SRC_CD"),
    F.col("ESI_BILL_DT").alias("PAID_DATE"),
    F.col("AMT_BILL").alias("AMT_BILL"),
    F.col("OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    F.col("ADMIN_FEE").alias("ADMIN_FEE"),
    F.col("BILL_BSS_CD").alias("BILL_BSS_CD")
)

df_join = df_TrnsGetSrcCd.alias("PaidClm").join(
    df_DataMart.alias("ClmMart"),
    (F.col("PaidClm.SRC_CD") == F.col("ClmMart.SRC_SYS_CD")) &
    (F.col("PaidClm.CLAIM_ID") == F.col("ClmMart.CLM_ID")),
    "left"
)

df_join_matched = df_join.filter(F.col("ClmMart.CLM_ID").isNotNull())
df_join_unmatched = df_join.filter(F.col("ClmMart.CLM_ID").isNull())

df_GetPaidYrMo = df_join_matched.select(
    F.col("ClmMart.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ClmMart.CLM_ID").alias("CLM_ID"),
    F.col("PaidClm.PAID_DATE").alias("CLM_PD_DT"),
    F.col("PaidClm.PAID_DATE").alias("PAID_DATE"),
    F.col("PaidClm.AMT_BILL").alias("AMT_BILL"),
    F.col("PaidClm.OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("PaidClm.OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    F.col("PaidClm.ADMIN_FEE").alias("ADMIN_FEE"),
    F.col("PaidClm.BILL_BSS_CD").alias("BILL_BSS_CD")
)

df_Unmatched = df_join_unmatched.select(
    F.col("PaidClm.CLAIM_ID").alias("CLAIM_ID")
)

write_files(
    df_Unmatched,
    f"{adls_path_publish}/external/processed/ESI_WDM_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_ClmDMClmUpdt = df_GetPaidYrMo.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.rpad(F.col("CLM_PD_DT"), 10, " ").alias("CLM_PD_DT"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.col("AMT_BILL").alias("CLM_ACTL_PD_AMT"),
    F.col("AMT_BILL").alias("CLM_PAYBL_AMT"),
    F.col("OTHR_PAYOR_AMT").alias("CLM_COB_PD_AMT"),
    F.lit(CurrentDate).alias("DM_LAST_UPDT_DT")
)

df_ClmDMClmInit = df_GetPaidYrMo.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("AMT_BILL").alias("CLM_PAYBL_AMT")
)

df_ClmDmClmLn = df_GetPaidYrMo.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.col("AMT_BILL").alias("CLM_LN_PAYBL_AMT")
)

spark.sql(f"DROP TABLE IF EXISTS STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDMClmUpdt_temp")
df_ClmDMClmUpdt.write.format("jdbc") \
  .option("url", jdbc_url_clmmart) \
  .options(**jdbc_props_clmmart) \
  .option("dbtable", "STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDMClmUpdt_temp") \
  .mode("overwrite") \
  .save()

merge_sql_ClmDMClmUpdt = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM AS T
USING STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDMClmUpdt_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN UPDATE SET
  T.CLM_PD_DT = S.CLM_PD_DT,
  T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
  T.CLM_ACTL_PD_AMT = S.CLM_ACTL_PD_AMT,
  T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
  T.CLM_COB_PD_AMT = S.CLM_COB_PD_AMT,
  T.DM_LAST_UPDT_DT = S.DM_LAST_UPDT_DT
WHEN NOT MATCHED THEN INSERT
  (SRC_SYS_CD, CLM_ID, CLM_PD_DT, LAST_UPDT_RUN_CYC_NO, CLM_ACTL_PD_AMT, CLM_PAYBL_AMT, CLM_COB_PD_AMT, DM_LAST_UPDT_DT)
  VALUES
  (S.SRC_SYS_CD, S.CLM_ID, S.CLM_PD_DT, S.LAST_UPDT_RUN_CYC_NO, S.CLM_ACTL_PD_AMT, S.CLM_PAYBL_AMT, S.CLM_COB_PD_AMT, S.DM_LAST_UPDT_DT);
"""

execute_dml(merge_sql_ClmDMClmUpdt, jdbc_url_clmmart, jdbc_props_clmmart)

spark.sql(f"DROP TABLE IF EXISTS STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDMClmInit_temp")
df_ClmDMClmInit.write.format("jdbc") \
  .option("url", jdbc_url_clmmart) \
  .options(**jdbc_props_clmmart) \
  .option("dbtable", "STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDMClmInit_temp") \
  .mode("overwrite") \
  .save()

merge_sql_ClmDMClmInit = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_INIT_CLM AS T
USING STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDMClmInit_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN UPDATE SET
  T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT
WHEN NOT MATCHED THEN INSERT
  (SRC_SYS_CD, CLM_ID, CLM_PAYBL_AMT)
  VALUES
  (S.SRC_SYS_CD, S.CLM_ID, S.CLM_PAYBL_AMT);
"""

execute_dml(merge_sql_ClmDMClmInit, jdbc_url_clmmart, jdbc_props_clmmart)

spark.sql(f"DROP TABLE IF EXISTS STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDmClmLn_temp")
df_ClmDmClmLn.write.format("jdbc") \
  .option("url", jdbc_url_clmmart) \
  .options(**jdbc_props_clmmart) \
  .option("dbtable", "STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDmClmLn_temp") \
  .mode("overwrite") \
  .save()

merge_sql_ClmDmClmLn = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_LN AS T
USING STAGING.ESIWebDMUpd_CLM_DM_CLM_ClmDmClmLn_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO)
WHEN MATCHED THEN UPDATE SET
  T.CLM_LN_PAYBL_AMT = S.CLM_LN_PAYBL_AMT
WHEN NOT MATCHED THEN INSERT
  (SRC_SYS_CD, CLM_ID, CLM_LN_SEQ_NO, CLM_LN_PAYBL_AMT)
  VALUES
  (S.SRC_SYS_CD, S.CLM_ID, S.CLM_LN_SEQ_NO, S.CLM_LN_PAYBL_AMT);
"""

execute_dml(merge_sql_ClmDmClmLn, jdbc_url_clmmart, jdbc_props_clmmart)
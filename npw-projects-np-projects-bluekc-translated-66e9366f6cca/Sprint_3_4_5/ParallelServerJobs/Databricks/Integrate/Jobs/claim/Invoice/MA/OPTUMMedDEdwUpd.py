# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Update EDW CLM_F  with paid date from OPTUMRX Bi Monthly Invoice file
# MAGIC     
# MAGIC PROCESSING:
# MAGIC                Update EDW CLM_F with paid date from OPTUMRX Bi Monthly file
# MAGIC                 If claim on input does not have matching claim in EDW, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project / TTR                    Change Description                                                                     Development                     Code Reviewer                 Date Reviewed      
# MAGIC ------------------              --------------------    -----------------------                    ------------------------------------------------------------------------------------               -------------------------------                        -------------------------------          ----------------------------   
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs                      Initial Development             IntegrateDev2	Abhiram Dasarathy	2020-12-11

# MAGIC Claim IDs for missing records in EDW
# MAGIC Read OPTUMRX File from the job OPTUMDrugClmInvoiceLand (../verified)
# MAGIC Directly updates the EDW Claim tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SourceSys = get_widget_value('SourceSys','OPTUMRX')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
RunCycleDate = get_widget_value('RunCycleDate','')
CurrentDate = get_widget_value('CurrentDate','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

df_EDW_EdwClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", f"SELECT CLM_SK,SRC_SYS_CD,CLM_ID FROM {EDWOwner}.CLM_F")
    .load()
)

df_EDW_date = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", f"SELECT CLNDR_DT_SK,YR_MO_SK FROM {EDWOwner}.CLNDR_DT_D")
    .load()
)

df_hf_optumrx_edw_invoice_date = dedup_sort(
    df_EDW_date,
    ["CLNDR_DT_SK"],
    []
)

schema_OPTUMRX_Invoice = StructType([
    StructField("CLAIM_ID", StringType(), False),
    StructField("INVC_DT", DecimalType(38,10), False),
    StructField("TOT_CST", DecimalType(38,10), False),
    StructField("CLM_ADM_FEE", DecimalType(38,10), False),
    StructField("BILL_CLM_CST", DecimalType(38,10), False),
    StructField("MBR_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("GRP_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True)
])

df_OPTUMRX_Invoice = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_OPTUMRX_Invoice)
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

df_TrnsGetSrcCd_PaidClm = df_OPTUMRX_Invoice.selectExpr(
    f"'{SourceSys}' as SRC_CD",
    "CLAIM_ID as CLM_ID",
    "INVC_DT",
    "TOT_CST",
    "CLM_ADM_FEE",
    "BILL_CLM_CST",
    "MBR_ID",
    "GRP_ID",
    "DT_FILLED",
    "CLM_STTUS"
)

df_Trns1_pre = (
    df_TrnsGetSrcCd_PaidClm.alias("PaidClm")
    .join(
        df_EDW_EdwClm.alias("EdwClm"),
        (F.col("PaidClm.SRC_CD") == F.col("EdwClm.SRC_SYS_CD")) & (F.col("PaidClm.CLM_ID") == F.col("EdwClm.CLM_ID")),
        "left"
    )
)

df_Trns1_pre = df_Trns1_pre.withColumn(
    "PdDtSk",
    GetFkeyDate(F.lit("IDS"), F.col("EdwClm.CLM_SK"), F.col("PaidClm.INVC_DT"), F.lit("X"))
)

df_Trns1_GetPaidYrMo = (
    df_Trns1_pre
    .filter(F.col("EdwClm.CLM_SK").isNotNull())
    .select(
        F.col("EdwClm.CLM_SK").alias("CLM_SK"),
        F.col("EdwClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("EdwClm.CLM_ID").alias("CLM_ID"),
        F.lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("PdDtSk").alias("CLM_PD_DT_SK"),
        F.col("PaidClm.BILL_CLM_CST").alias("AMT_BILL"),
        F.col("PaidClm.CLM_ADM_FEE").alias("ADMIN_FEE")
    )
)

df_Trns1_Unmatched = (
    df_Trns1_pre
    .filter(F.col("EdwClm.CLM_SK").isNull())
    .select(
        F.col("PaidClm.CLM_ID").alias("CLAIM_ID")
    )
)

df_Trns1_Unmatched = df_Trns1_Unmatched.withColumn("CLAIM_ID", F.rpad("CLAIM_ID", 20, " "))
write_files(
    df_Trns1_Unmatched.select("CLAIM_ID"),
    f"{adls_path_publish}/processed/OPTUMRX_EDW_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_PaidYrMo_pre = (
    df_Trns1_GetPaidYrMo.alias("GetPaidYrMo")
    .join(
        df_hf_optumrx_edw_invoice_date.alias("Date_lkup"),
        F.col("GetPaidYrMo.CLM_PD_DT_SK") == F.col("Date_lkup.CLNDR_DT_SK"),
        "left"
    )
)

df_PaidYrMo_pre = df_PaidYrMo_pre.withColumn(
    "ClmPdYrMoSk",
    F.when(
        F.col("Date_lkup.YR_MO_SK").isNull(),
        F.col("GetPaidYrMo.CLM_PD_DT_SK").substr(F.lit(1), F.lit(4)).concat(F.col("GetPaidYrMo.CLM_PD_DT_SK").substr(F.lit(6), F.lit(2)))
    ).otherwise(F.col("Date_lkup.YR_MO_SK"))
)

df_PaidYrMo_ClmFactUpdt = (
    df_PaidYrMo_pre
    .select(
        F.col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
        F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("GetPaidYrMo.CLM_PD_DT_SK").alias("CLM_PD_DT_SK"),
        F.col("ClmPdYrMoSk").alias("CLM_PD_YR_MO_SK"),
        F.col("GetPaidYrMo.ADMIN_FEE").alias("DRUG_CLM_ADM_FEE_AMT")
    )
)

df_PaidYrMo_ClmF2Updt = (
    df_PaidYrMo_pre
    .select(
        F.col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
        F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
    )
)

df_PaidYrMo_DrugClmPriceFUpdt = (
    df_PaidYrMo_pre
    .select(
        F.col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
        F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("GetPaidYrMo.AMT_BILL").alias("INVC_TOT_DUE_AMT")
    )
)

df_PaidYrMo_ClmFactUpdt = df_PaidYrMo_ClmFactUpdt.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
).withColumn(
    "CLM_PD_DT_SK", F.rpad("CLM_PD_DT_SK", 10, " ")
).withColumn(
    "CLM_PD_YR_MO_SK", F.rpad("CLM_PD_YR_MO_SK", 6, " ")
)

df_PaidYrMo_ClmF2Updt = df_PaidYrMo_ClmF2Updt.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
)

df_PaidYrMo_DrugClmPriceFUpdt = df_PaidYrMo_DrugClmPriceFUpdt.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ")
)

spark.sql("DROP TABLE IF EXISTS STAGING.OPTUMMedDEdwUpd_Update_Clm_F_ClmFactUpdt_temp")
df_PaidYrMo_ClmFactUpdt.write.format("jdbc") \
    .option("url", jdbc_url_edw) \
    .options(**jdbc_props_edw) \
    .option("dbtable", "STAGING.OPTUMMedDEdwUpd_Update_Clm_F_ClmFactUpdt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_ClmFactUpdt = f"""MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.OPTUMMedDEdwUpd_Update_Clm_F_ClmFactUpdt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
  T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  T.CLM_PD_DT_SK = S.CLM_PD_DT_SK,
  T.CLM_PD_YR_MO_SK = S.CLM_PD_YR_MO_SK,
  T.DRUG_CLM_ADM_FEE_AMT = S.DRUG_CLM_ADM_FEE_AMT
WHEN NOT MATCHED THEN INSERT
  (CLM_SK,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,CLM_PD_DT_SK,CLM_PD_YR_MO_SK,DRUG_CLM_ADM_FEE_AMT)
  VALUES
  (S.CLM_SK,S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,S.CLM_PD_DT_SK,S.CLM_PD_YR_MO_SK,S.DRUG_CLM_ADM_FEE_AMT);"""
execute_dml(merge_sql_ClmFactUpdt, jdbc_url_edw, jdbc_props_edw)

spark.sql("DROP TABLE IF EXISTS STAGING.OPTUMMedDEdwUpd_Update_Clm_F_ClmF2Updt_temp")
df_PaidYrMo_ClmF2Updt.write.format("jdbc") \
    .option("url", jdbc_url_edw) \
    .options(**jdbc_props_edw) \
    .option("dbtable", "STAGING.OPTUMMedDEdwUpd_Update_Clm_F_ClmF2Updt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_ClmF2Updt = f"""MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.OPTUMMedDEdwUpd_Update_Clm_F_ClmF2Updt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
  T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
WHEN NOT MATCHED THEN INSERT
  (CLM_SK,LAST_UPDT_RUN_CYC_EXCTN_DT_SK)
  VALUES
  (S.CLM_SK,S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK);"""
execute_dml(merge_sql_ClmF2Updt, jdbc_url_edw, jdbc_props_edw)

spark.sql("DROP TABLE IF EXISTS STAGING.OPTUMMedDEdwUpd_Update_Clm_F_DrugClmPriceFUpdt_temp")
df_PaidYrMo_DrugClmPriceFUpdt.write.format("jdbc") \
    .option("url", jdbc_url_edw) \
    .options(**jdbc_props_edw) \
    .option("dbtable", "STAGING.OPTUMMedDEdwUpd_Update_Clm_F_DrugClmPriceFUpdt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_DrugClmPriceFUpdt = f"""MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.OPTUMMedDEdwUpd_Update_Clm_F_DrugClmPriceFUpdt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
  T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  T.INVC_TOT_DUE_AMT = S.INVC_TOT_DUE_AMT
WHEN NOT MATCHED THEN INSERT
  (CLM_SK,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,INVC_TOT_DUE_AMT)
  VALUES
  (S.CLM_SK,S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,S.INVC_TOT_DUE_AMT);"""
execute_dml(merge_sql_DrugClmPriceFUpdt, jdbc_url_edw, jdbc_props_edw)
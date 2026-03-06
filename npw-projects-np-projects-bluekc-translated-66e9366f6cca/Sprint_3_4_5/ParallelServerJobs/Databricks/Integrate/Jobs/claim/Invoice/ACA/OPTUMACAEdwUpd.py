# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Update EDW CLM_F  with paid date from OPTUMRX ACA Bi Monthly Invoice file
# MAGIC Called by:  OPTUMACADrugInvoiceUpdateSeq
# MAGIC     
# MAGIC PROCESSING:
# MAGIC                Update EDW CLM_F with paid date from OPTUMRX Bi Monthly file
# MAGIC                 If claim on input does not have matching claim in EDW, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                      Date                 Prjoect / TTR                                                                           Change Description                                                                                     Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------                 --------------------      -----------------------                                                             -----------------------------------------------------------------------------------------------------                             --------------------------------                   -----------------------------     ----------------------------   
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs             Initial Development                                                                                                    IntegrateDev2                            Reddy Sanam            2020-12-10

# MAGIC Claim IDs for missing records in EDW
# MAGIC Read OPTUMRX File from the job OPTUMDrugClmInvoiceLand (../verified)
# MAGIC Called by OPTUMACADrugInvoiceUpdateSeq
# MAGIC Directly updates the EDW Claim tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, lit, when, concat, rpad, substring
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

SourceSys = get_widget_value('SourceSys', '')
RunID = get_widget_value('RunID', '')
RunCycle = get_widget_value('RunCycle', '')
RunCycleDate = get_widget_value('RunCycleDate', '')
CurrentDate = get_widget_value('CurrentDate', '')
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query = f"SELECT CLM_SK,SRC_SYS_CD,CLM_ID FROM {EDWOwner}.CLM_F"
df_EDW_EdwClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"SELECT CLNDR_DT_SK,YR_MO_SK FROM {EDWOwner}.CLNDR_DT_D"
df_EDW_date = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_optumrx_edw_invoice_date = dedup_sort(df_EDW_date, ["CLNDR_DT_SK"], [])

schema_OPTUMRX_Invoice = StructType([
    StructField("CLAIM_ID", StringType(), True),
    StructField("INVC_DT", DoubleType(), True),
    StructField("TOT_CST", DoubleType(), True),
    StructField("CLM_ADM_FEE", DoubleType(), True),
    StructField("BILL_CLM_CST", DoubleType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("DT_FILLED", StringType(), True),
    StructField("CLM_STTUS", StringType(), True),
    StructField("GRP_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True)
])

df_OPTUMRX_Invoice = (
    spark.read
    .option("quote", '"')
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_OPTUMRX_Invoice)
    .csv(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

df_PaidClm = (
    df_OPTUMRX_Invoice.select(
        lit(SourceSys).alias("SRC_CD"),
        col("CLAIM_ID").alias("CLM_ID"),
        col("INVC_DT").alias("INVC_DT"),
        col("TOT_CST").alias("TOT_CST"),
        col("CLM_ADM_FEE").alias("CLM_ADM_FEE"),
        col("BILL_CLM_CST").alias("BILL_CLM_CST"),
        col("MBR_ID").alias("MBR_ID"),
        col("GRP_ID").alias("GRP_ID"),
        col("DT_FILLED").alias("DT_FILLED"),
        col("CLM_STTUS").alias("CLM_STTUS")
    )
)

df_join = df_PaidClm.alias("PaidClm").join(
    df_EDW_EdwClm.alias("EdwClm"),
    [
        col("PaidClm.SRC_CD") == col("EdwClm.SRC_SYS_CD"),
        col("PaidClm.CLM_ID") == col("EdwClm.CLM_ID")
    ],
    "left"
)

df_withvars = df_join.withColumn(
    "PdDtSk",
    GetFkeyDate('IDS', col("EdwClm.CLM_SK"), col("PaidClm.INVC_DT"), lit("X"))
)

df_GetPaidYrMo = df_withvars.filter(col("EdwClm.CLM_SK").isNotNull()).select(
    col("EdwClm.CLM_SK").alias("CLM_SK"),
    col("EdwClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("EdwClm.CLM_ID").alias("CLM_ID"),
    lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PdDtSk").alias("CLM_PD_DT_SK"),
    col("PaidClm.BILL_CLM_CST").alias("AMT_BILL"),
    col("PaidClm.CLM_ADM_FEE").alias("ADMIN_FEE")
)

df_Unmatched = df_withvars.filter(col("EdwClm.CLM_SK").isNull()).select(
    col("PaidClm.CLM_ID").alias("CLAIM_ID")
)

df_unmatched_final = df_Unmatched.withColumn(
    "CLAIM_ID",
    rpad("CLAIM_ID", 20, " ")
)

write_files(
    df_unmatched_final.select("CLAIM_ID"),
    f"{adls_path_publish}/external/processed/OPTUMRX_EDW_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_join2 = df_GetPaidYrMo.alias("GetPaidYrMo").join(
    df_hf_optumrx_edw_invoice_date.alias("Date_lkup"),
    col("GetPaidYrMo.CLM_PD_DT_SK") == col("Date_lkup.CLNDR_DT_SK"),
    "left"
)

df_withVars2 = df_join2.withColumn(
    "ClmPdYrMoSk",
    when(
        col("Date_lkup.YR_MO_SK").isNull(),
        concat(
            substring(col("GetPaidYrMo.CLM_PD_DT_SK"), 1, 4),
            substring(col("GetPaidYrMo.CLM_PD_DT_SK"), 6, 2)
        )
    ).otherwise(col("Date_lkup.YR_MO_SK"))
)

df_ClmFactUpdt = df_withVars2.select(
    col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
    lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GetPaidYrMo.CLM_PD_DT_SK").alias("CLM_PD_DT_SK"),
    col("ClmPdYrMoSk").alias("CLM_PD_YR_MO_SK"),
    col("GetPaidYrMo.ADMIN_FEE").alias("DRUG_CLM_ADM_FEE_AMT")
)

df_ClmF2Updt = df_withVars2.select(
    col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
    lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
)

df_DrugClmPriceFUpdt = df_withVars2.select(
    col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
    lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GetPaidYrMo.AMT_BILL").alias("INVC_TOT_DUE_AMT")
)

df_ClmFactUpdt_final = (
    df_ClmFactUpdt
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("CLM_PD_DT_SK", rpad("CLM_PD_DT_SK", 10, " "))
    .withColumn("CLM_PD_YR_MO_SK", rpad("CLM_PD_YR_MO_SK", 6, " "))
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp", jdbc_url, jdbc_props)
df_ClmFactUpdt_final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp") \
    .mode("overwrite") \
    .save()
merge_sql_1 = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
  T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  T.CLM_PD_DT_SK = S.CLM_PD_DT_SK,
  T.CLM_PD_YR_MO_SK = S.CLM_PD_YR_MO_SK,
  T.DRUG_CLM_ADM_FEE_AMT = S.DRUG_CLM_ADM_FEE_AMT
WHEN NOT MATCHED THEN INSERT
  (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK, CLM_PD_DT_SK, CLM_PD_YR_MO_SK, DRUG_CLM_ADM_FEE_AMT)
  VALUES
  (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, S.CLM_PD_DT_SK, S.CLM_PD_YR_MO_SK, S.DRUG_CLM_ADM_FEE_AMT);
"""
execute_dml(merge_sql_1, jdbc_url, jdbc_props)

df_ClmF2Updt_final = (
    df_ClmF2Updt
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " "))
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp", jdbc_url, jdbc_props)
df_ClmF2Updt_final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp") \
    .mode("overwrite") \
    .save()
merge_sql_2 = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
  T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
WHEN NOT MATCHED THEN INSERT
  (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK)
  VALUES
  (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK);
"""
execute_dml(merge_sql_2, jdbc_url, jdbc_props)

df_DrugClmPriceFUpdt_final = (
    df_DrugClmPriceFUpdt
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " "))
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp", jdbc_url, jdbc_props)
df_DrugClmPriceFUpdt_final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp") \
    .mode("overwrite") \
    .save()
merge_sql_3 = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.OPTUMACAEdwUpd_Update_Clm_F_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
  T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  T.INVC_TOT_DUE_AMT = S.INVC_TOT_DUE_AMT
WHEN NOT MATCHED THEN INSERT
  (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK, INVC_TOT_DUE_AMT)
  VALUES
  (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, S.INVC_TOT_DUE_AMT);
"""
execute_dml(merge_sql_3, jdbc_url, jdbc_props)
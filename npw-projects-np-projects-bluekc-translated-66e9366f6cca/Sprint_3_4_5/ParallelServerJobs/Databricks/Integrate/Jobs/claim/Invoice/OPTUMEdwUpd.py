# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
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
# MAGIC Ramu                    10/24/2019        6131- PBM Replacement     original programming                                                                  IntegrateDev2                                Kalyan Neelam                 2019-11-27

# MAGIC Claim IDs for missing records in EDW
# MAGIC Read OPTUMRX File from the job OPTUMDrugClmInvoiceLand (../verified)
# MAGIC Directly updates the EDW Claim tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, lit, rpad, substring, when, isnull
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = ""
jdbc_props = {}

# Parameters
SourceSys = get_widget_value("SourceSys","OPTUMRX")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
RunCycleDate = get_widget_value("RunCycleDate","")
CurrentDate = get_widget_value("CurrentDate","")
EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")

# Obtain DB connection for EDW
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# Read date dimension for "date" link (EDW -> hf_optumrx_edw_invoice_date), then deduplicate on CLNDR_DT_SK
df_dateQuery = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CLNDR_DT_SK, YR_MO_SK FROM {EDWOwner}.CLNDR_DT_D")
    .load()
)
df_date_lkup = dedup_sort(df_dateQuery, ["CLNDR_DT_SK"], [])

# Read file from OPTUMRX_Invoice (CSeqFileStage)
schema_OPTUMRX_Invoice = StructType([
    StructField("CLAIM_ID", StringType(), False),
    StructField("INVC_DT", DoubleType(), False),
    StructField("TOT_CST", DoubleType(), False),
    StructField("CLM_ADM_FEE", DoubleType(), False),
    StructField("BILL_CLM_CST", DoubleType(), False),
    StructField("MBR_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("GRP_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True),
])
df_OPTUMRX_Invoice = (
    spark.read.format("csv")
    .schema(schema_OPTUMRX_Invoice)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

# TrnsGetSrcCd (CTransformerStage)
df_TrnsGetSrcCd = (
    df_OPTUMRX_Invoice
    .withColumn("SRC_CD", lit(SourceSys))
    .withColumn("CLM_ID", col("CLAIM_ID"))
    .withColumn("INVC_DT", col("INVC_DT"))
    .withColumn("TOT_CST", col("TOT_CST"))
    .withColumn("CLM_ADM_FEE", col("CLM_ADM_FEE"))
    .withColumn("BILL_CLM_CST", col("BILL_CLM_CST"))
    .withColumn("MBR_ID", col("MBR_ID"))
    .withColumn("GRP_ID", col("GRP_ID"))
    .withColumn("DT_FILLED", col("DT_FILLED"))
    .withColumn("CLM_STTUS", col("CLM_STTUS"))
)
df_PaidClm = df_TrnsGetSrcCd.select(
    "SRC_CD",
    "CLM_ID",
    "INVC_DT",
    "TOT_CST",
    "CLM_ADM_FEE",
    "BILL_CLM_CST",
    "MBR_ID",
    "GRP_ID",
    "DT_FILLED",
    "CLM_STTUS"
)

# Trns1 (CTransformerStage) with lookup on EdwClm (DB2Connector) via left join
# Write df_PaidClm into a staging table, then left join with {EDWOwner}.CLM_F
execute_dml(f"DROP TABLE IF EXISTS STAGING.OPTUMEdwUpd_Trns1_temp", jdbc_url, jdbc_props)
df_PaidClm.write.jdbc(
    url=jdbc_url,
    table="STAGING.OPTUMEdwUpd_Trns1_temp",
    mode="overwrite",
    properties=jdbc_props
)
df_Trns1Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", 
        f"SELECT t.SRC_CD, t.CLM_ID, t.INVC_DT, t.TOT_CST, t.CLM_ADM_FEE, t.BILL_CLM_CST, "
        f"t.MBR_ID, t.GRP_ID, t.DT_FILLED, t.CLM_STTUS, e.CLM_SK, e.SRC_SYS_CD as EDW_SRC_SYS_CD, e.CLM_ID as EDW_CLM_ID "
        f"FROM STAGING.OPTUMEdwUpd_Trns1_temp t "
        f"LEFT JOIN {EDWOwner}.CLM_F e ON e.SRC_SYS_CD = t.SRC_CD AND e.CLM_ID = t.CLM_ID"
    )
    .load()
)
df_Trns1Input = df_Trns1Input.withColumn(
    "PdDtSk",
    GetFkeyDate(lit("IDS"), col("CLM_SK"), col("INVC_DT"), lit("X"))
)

df_GetPaidYrMo = (
    df_Trns1Input
    .filter(col("CLM_SK").isNotNull())
    .select(
        col("CLM_SK").alias("CLM_SK"),
        col("EDW_SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("EDW_CLM_ID").alias("CLM_ID"),
        lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("PdDtSk").alias("CLM_PD_DT_SK"),
        col("BILL_CLM_CST").alias("AMT_BILL"),
        col("CLM_ADM_FEE").alias("ADMIN_FEE")
    )
)

df_Unmatched = df_Trns1Input.filter(col("CLM_SK").isNull()).select(col("CLM_ID"))

# Write Unmatched (OPTUMRX_EDW_Unmatched)
df_Unmatched_final = df_Unmatched.select(
    rpad(col("CLM_ID"), 20, " ").alias("CLAIM_ID")
)
write_files(
    df_Unmatched_final,
    f"{adls_path_publish}/external/processed/OPTUMRX_EDW_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# PaidYrMo (CTransformerStage) with lookup on df_date_lkup
df_PaidYrMoInput = df_GetPaidYrMo.join(
    df_date_lkup,
    [df_GetPaidYrMo["CLM_PD_DT_SK"] == df_date_lkup["CLNDR_DT_SK"]],
    "left"
)
df_PaidYrMo = df_PaidYrMoInput.withColumn(
    "ClmPdYrMoSk",
    when(
        isnull(col("YR_MO_SK")),
        substring(col("CLM_PD_DT_SK"), 1, 4).concat(substring(col("CLM_PD_DT_SK"), 6, 2))
    ).otherwise(col("YR_MO_SK"))
)

df_ClmFactUpdt = df_PaidYrMo.select(
    col("CLM_SK").alias("CLM_SK"),
    lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_PD_DT_SK").alias("CLM_PD_DT_SK"),
    col("ClmPdYrMoSk").alias("CLM_PD_YR_MO_SK"),
    col("ADMIN_FEE").alias("DRUG_CLM_ADM_FEE_AMT")
)

df_ClmF2Updt = df_PaidYrMo.select(
    col("CLM_SK").alias("CLM_SK"),
    lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
)

df_DrugClmPriceFUpdt = df_PaidYrMo.select(
    col("CLM_SK").alias("CLM_SK"),
    lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("AMT_BILL").alias("INVC_TOT_DUE_AMT")
)

# Update_Clm_F (DB2Connector) merges
# 1) ClmFactUpdt
df_ClmFactUpdt_final = df_ClmFactUpdt.select(
    "CLM_SK",
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("CLM_PD_DT_SK"), 10, " ").alias("CLM_PD_DT_SK"),
    rpad(col("CLM_PD_YR_MO_SK"), 6, " ").alias("CLM_PD_YR_MO_SK"),
    col("DRUG_CLM_ADM_FEE_AMT").alias("DRUG_CLM_ADM_FEE_AMT")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.OPTUMEdwUpd_Update_Clm_F_temp_ClmFactUpdt", jdbc_url, jdbc_props)
df_ClmFactUpdt_final.write.jdbc(
    url=jdbc_url,
    table="STAGING.OPTUMEdwUpd_Update_Clm_F_temp_ClmFactUpdt",
    mode="overwrite",
    properties=jdbc_props
)
merge_sql_ClmFactUpdt = (
    f"MERGE {EDWOwner}.CLM_F AS T "
    f"USING STAGING.OPTUMEdwUpd_Update_Clm_F_temp_ClmFactUpdt AS S "
    f"ON T.CLM_SK = S.CLM_SK "
    f"WHEN MATCHED THEN "
    f"UPDATE SET T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, "
    f"T.CLM_PD_DT_SK = S.CLM_PD_DT_SK, "
    f"T.CLM_PD_YR_MO_SK = S.CLM_PD_YR_MO_SK, "
    f"T.DRUG_CLM_ADM_FEE_AMT = S.DRUG_CLM_ADM_FEE_AMT "
    f"WHEN NOT MATCHED THEN "
    f"INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK, CLM_PD_DT_SK, CLM_PD_YR_MO_SK, DRUG_CLM_ADM_FEE_AMT) "
    f"VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, S.CLM_PD_DT_SK, S.CLM_PD_YR_MO_SK, S.DRUG_CLM_ADM_FEE_AMT);"
)
execute_dml(merge_sql_ClmFactUpdt, jdbc_url, jdbc_props)

# 2) ClmF2Updt
df_ClmF2Updt_final = df_ClmF2Updt.select(
    "CLM_SK",
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.OPTUMEdwUpd_Update_Clm_F_temp_ClmF2Updt", jdbc_url, jdbc_props)
df_ClmF2Updt_final.write.jdbc(
    url=jdbc_url,
    table="STAGING.OPTUMEdwUpd_Update_Clm_F_temp_ClmF2Updt",
    mode="overwrite",
    properties=jdbc_props
)
merge_sql_ClmF2Updt = (
    f"MERGE {EDWOwner}.CLM_F AS T "
    f"USING STAGING.OPTUMEdwUpd_Update_Clm_F_temp_ClmF2Updt AS S "
    f"ON T.CLM_SK = S.CLM_SK "
    f"WHEN MATCHED THEN "
    f"UPDATE SET T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK "
    f"WHEN NOT MATCHED THEN "
    f"INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK) "
    f"VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK);"
)
execute_dml(merge_sql_ClmF2Updt, jdbc_url, jdbc_props)

# 3) DrugClmPriceFUpdt
df_DrugClmPriceFUpdt_final = df_DrugClmPriceFUpdt.select(
    "CLM_SK",
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("INVC_TOT_DUE_AMT").alias("INVC_TOT_DUE_AMT")
)
execute_dml(f"DROP TABLE IF EXISTS STAGING.OPTUMEdwUpd_Update_Clm_F_temp_DrugClmPriceFUpdt", jdbc_url, jdbc_props)
df_DrugClmPriceFUpdt_final.write.jdbc(
    url=jdbc_url,
    table="STAGING.OPTUMEdwUpd_Update_Clm_F_temp_DrugClmPriceFUpdt",
    mode="overwrite",
    properties=jdbc_props
)
merge_sql_DrugClmPriceFUpdt = (
    f"MERGE {EDWOwner}.CLM_F AS T "
    f"USING STAGING.OPTUMEdwUpd_Update_Clm_F_temp_DrugClmPriceFUpdt AS S "
    f"ON T.CLM_SK = S.CLM_SK "
    f"WHEN MATCHED THEN "
    f"UPDATE SET T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, "
    f"T.INVC_TOT_DUE_AMT = S.INVC_TOT_DUE_AMT "
    f"WHEN NOT MATCHED THEN "
    f"INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK, INVC_TOT_DUE_AMT) "
    f"VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, S.INVC_TOT_DUE_AMT);"
)
execute_dml(merge_sql_DrugClmPriceFUpdt, jdbc_url, jdbc_props)
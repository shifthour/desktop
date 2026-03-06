# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Update Web data mart with paid date from OptumRx Bi monthly file
# MAGIC Called by :OPTUMACADrugInvoiceUpdateSeq
# MAGIC       
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Update Data Mart with paid date from Argus monthly file
# MAGIC                Update Data Mart CLM_DM_CLM, CLM_DM_CLM_LN, and CLM_DM_INIT_CLM 
# MAGIC               If claim on input does not have matching claim in IDS, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                            Project #                                                          Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ---------------------------------------------------------   ----------------                                                      ------------------------------------       ----------------------------           ----------------
# MAGIC Ramu Avula            2020-10-20               original programming                          6264 - PBM PHASE II - Government Programs     IntegrateDev2               Reddy Sanam                 12-11-2020

# MAGIC Claim IDs for missing records in EDW
# MAGIC Read weekly Invoice file from OPTUMRX  OPTUMRXDrugClmInvoiceLand
# MAGIC Direct update of data mart tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

SourceSys = get_widget_value('SourceSys','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
RunCycleDate = get_widget_value('RunCycleDate','')
CurrentDate = get_widget_value('CurrentDate','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)

extract_query = f"SELECT SRC_SYS_CD, CLM_ID FROM {ClmMartOwner}.CLM_DM_CLM"
df_datamart = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_optumrx_invoice = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("INVC_DT", DecimalType(10, 0), False),
    StructField("TOT_CST", DecimalType(10, 2), False),
    StructField("CLM_ADM_FEE", DecimalType(10, 0), False),
    StructField("BILL_CLM_CST", DecimalType(10, 2), False),
    StructField("MBR_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("GRP_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True)
])

df_Optumrx_Invoice = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", '"')
    .schema(schema_optumrx_invoice)
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

df_paidclm = df_Optumrx_Invoice.select(
    F.col("CLM_ID").alias("CLAIM_ID"),
    F.lit(SourceSys).alias("SRC_CD"),
    F.col("INVC_DT").alias("PAID_DATE"),
    F.col("TOT_CST").alias("AMT_BILL"),
    F.col("CLM_ADM_FEE").alias("ADMIN_FEE")
)

df_join = df_paidclm.alias("PaidClm").join(
    df_datamart.alias("ClmMart"),
    (F.col("PaidClm.SRC_CD") == F.col("ClmMart.SRC_SYS_CD")) & (F.col("PaidClm.CLAIM_ID") == F.col("ClmMart.CLM_ID")),
    "left"
).withColumn(
    "PdDtSk",
    F.when(
        F.col("PaidClm.PAID_DATE").isNull() | (F.length(trim(F.col("PaidClm.PAID_DATE"))) == 0),
        F.lit("UNK")
    ).otherwise(
        F.substring(trim(F.col("PaidClm.PAID_DATE")), 1, 10)
    )
)

df_getpaidyrmo = df_join.filter(F.col("ClmMart.CLM_ID").isNotNull()).select(
    F.col("ClmMart.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ClmMart.CLM_ID").alias("CLM_ID"),
    F.col("PaidClm.PAID_DATE").alias("CLM_PD_DT"),
    F.col("PaidClm.PAID_DATE").alias("PAID_DATE"),
    F.col("PaidClm.AMT_BILL").alias("AMT_BILL"),
    F.col("PaidClm.ADMIN_FEE").alias("ADMIN_FEE")
)

df_unmatched = df_join.filter(F.col("ClmMart.CLM_ID").isNull()).select(
    F.col("PaidClm.CLAIM_ID").alias("CLAIM_ID")
)

df_unmatched = df_unmatched.withColumn("CLAIM_ID", F.rpad(F.col("CLAIM_ID"), 20, " "))
df_unmatched = df_unmatched.select("CLAIM_ID")

write_files(
    df_unmatched,
    f"{adls_path_publish}/processed/OPTUMRX_WDM_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_clmdmclmupdt = df_getpaidyrmo.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_PD_DT").alias("CLM_PD_DT"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.lit(CurrentDate).alias("DM_LAST_UPDT_DT")
)

df_clmdmclmupdt = df_clmdmclmupdt.withColumn("CLM_PD_DT", F.rpad(F.col("CLM_PD_DT"), 10, " "))
temp_table = "STAGING.OPTUMACAWebDMUpd_CLM_DM_CLM_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

df_clmdmclmupdt.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM AS T
USING {temp_table} AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN UPDATE SET
    T.CLM_PD_DT = S.CLM_PD_DT,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.DM_LAST_UPDT_DT = S.DM_LAST_UPDT_DT
WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, CLM_ID, CLM_PD_DT, LAST_UPDT_RUN_CYC_NO, DM_LAST_UPDT_DT)
VALUES (S.SRC_SYS_CD, S.CLM_ID, S.CLM_PD_DT, S.LAST_UPDT_RUN_CYC_NO, S.DM_LAST_UPDT_DT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)
# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Copyright 2019 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  OPTUMRXDrugExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after OPTUMRXDrugClmInvoiceLand
# MAGIC          *  UNIX file K_CLM.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM_COB
# MAGIC             
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)          Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)          -----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC  Ramu\(9)                2019-10-23\(9)6131- PBM Replacement   Originally Programmed                                                     IntegtateDev2                        Kalyan Neelam        2019-11-27

# MAGIC OPTUMRX Claim Invoice Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records with out keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
from pyspark.sql.functions import col, lit, expr, substring, length
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd", "")
SrcSysCdSK = get_widget_value("SrcSysCdSK", "")
CurrRunCycle = get_widget_value("CurrRunCycle", "")
RunID = get_widget_value("RunID", "")
IDSOwner = get_widget_value("$IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

schema_OPTUMRXDrugClmInvoicePaidUpdt = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("INVC_DT", DecimalType(38, 10), False),
    StructField("TOT_CST", DecimalType(38, 10), False),
    StructField("CLM_ADM_FEE", DecimalType(38, 10), False),
    StructField("BILL_CLM_CST", DecimalType(38, 10), False),
    StructField("MBR_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("GRP_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True)
])

df_OPTUMRXDrugClmInvoicePaidUpdt = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_OPTUMRXDrugClmInvoicePaidUpdt)
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

df_Trans2_Adjustments = (
    df_OPTUMRXDrugClmInvoicePaidUpdt
    .filter(expr("substring(CLM_STTUS,1,1) = 'X'"))
    .select(
        lit(SrcSysCdSK).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        expr("substring(trim(CLM_ID),1,length(trim(CLM_ID))-1)").alias("CLM_ID")
    )
)

df_Trans2_Regular = (
    df_OPTUMRXDrugClmInvoicePaidUpdt
    .filter(expr("substring(CLM_STTUS,1,1) <> 'X'"))
    .select(
        lit(SrcSysCdSK).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID")
    )
)

df_Collector = df_Trans2_Adjustments.union(df_Trans2_Regular)

df_deduped = dedup_sort(
    df_Collector,
    ["SRC_SYS_CD_SK", "CLM_ID"],
    [("SRC_SYS_CD_SK", "A"), ("CLM_ID", "A")]
)

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner
}

df_ClmLoadPK_output = ClmLoadPK(df_deduped, params_ClmLoadPK)

df_final = df_ClmLoadPK_output.select("SRC_SYS_CD", "CLM_ID", "CRT_RUN_CYC_EXCTN_SK", "CLM_SK")

write_files(
    df_final,
    "hf_clm_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Copyright 2020 Blue Cross/Blue Shield of Kansas City
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
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs                      Initial Development       IntegrateDev2\(9)\(9)Abhiram Dasarathy\(9)2020-12-11

# MAGIC OPTUMRX Claim Invoice Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records with out keys
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


SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

schema_OPTUMRXDrugClmInvoicePaidUpdt = StructType([
    StructField("CLM_ID", StringType(), False),
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

df_OPTUMRXDrugClmInvoicePaidUpdt = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", "|")
    .schema(schema_OPTUMRXDrugClmInvoicePaidUpdt)
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

df_Regular = df_OPTUMRXDrugClmInvoicePaidUpdt.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_Adjustments = (
    df_OPTUMRXDrugClmInvoicePaidUpdt
    .filter(F.substring(F.col("CLM_STTUS"), 1, 1) == 'X')
    .select(
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.substring(F.trim(F.col("CLM_ID")), 1, F.length(F.trim(F.col("CLM_ID"))) - 1).alias("CLM_ID")
    )
)

df_Collector = df_Regular.select("SRC_SYS_CD_SK","CLM_ID").union(
    df_Adjustments.select("SRC_SYS_CD_SK","CLM_ID")
)

df_deduped = dedup_sort(df_Collector, ["SRC_SYS_CD_SK","CLM_ID"], [])

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner
}
df_hf_clm_pk_lkup = ClmLoadPK(df_deduped, params_ClmLoadPK)

df_final = df_hf_clm_pk_lkup.select(
    F.rpad(F.col("SRC_SYS_CD"), 10, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), 20, " ").alias("CLM_ID"),
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_SK"
)

write_files(
    df_final,
    "hf_clm_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
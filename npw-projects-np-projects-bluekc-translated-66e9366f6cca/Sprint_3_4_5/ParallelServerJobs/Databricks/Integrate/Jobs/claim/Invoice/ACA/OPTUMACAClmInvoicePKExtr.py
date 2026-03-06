# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Copyright 2019 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  OPTUMACADrugInvoiceUpdateSeq
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
# MAGIC MODIFICATIONS
# MAGIC Developer                      Date                 Prjoect / TTR                                                                           Change Description                                                                                     Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------                 --------------------      -----------------------                                                             -----------------------------------------------------------------------------------------------------                             --------------------------------                   -----------------------------     ----------------------------   
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs              Initial Development                                                                                                    IntegrateDev2                           Reddy Sanam            2020-12-10

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
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_OPTUMRXDrugClmInvoicePaidUpdt = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("INVC_DT", DecimalType(10, 0), False),
    StructField("TOT_CST", DecimalType(10, 2), False),
    StructField("CLM_ADM_FEE", DecimalType(10, 2), False),
    StructField("BILL_CLM_CST", DecimalType(10, 2), False),
    StructField("MBR_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("GRP_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True)
])

df_OPTUMRX = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_OPTUMRXDrugClmInvoicePaidUpdt)
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

df_Trans2_Regular = df_OPTUMRX.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_Trans2_Adjustments = (
    df_OPTUMRX.filter(F.substring(F.col("CLM_STTUS"), 1, 1) == 'X')
    .select(
        F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        F.substring(F.trim(F.col("CLM_ID")), 1, F.length(F.trim(F.col("CLM_ID"))) - 1).alias("CLM_ID")
    )
)

df_Collector = df_Trans2_Regular.union(df_Trans2_Adjustments)

df_hf_optumrx_clminvoice_pkey_extr_dedup = dedup_sort(df_Collector, ["SRC_SYS_CD_SK", "CLM_ID"], [])

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}

df_ClmLoadPK_out = ClmLoadPK(df_hf_optumrx_clminvoice_pkey_extr_dedup, params_ClmLoadPK)

write_files(
    df_ClmLoadPK_out.select("SRC_SYS_CD", "CLM_ID", "CRT_RUN_CYC_EXCTN_SK", "CLM_SK"),
    f"{adls_path}/hf_clm_pk_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
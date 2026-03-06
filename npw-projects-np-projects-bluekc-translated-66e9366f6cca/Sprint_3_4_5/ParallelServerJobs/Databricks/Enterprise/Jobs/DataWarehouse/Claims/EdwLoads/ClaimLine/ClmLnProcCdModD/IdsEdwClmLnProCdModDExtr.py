# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwClmLnProcCdModDimExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from IDS and EDW table CLM_LN_PROC_CD_MOD to compare
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_LN_PROC_CD_MOD
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC                 EDW - CLM_LN_PROC_CD_MOD_D
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd_nm from cd_sk
# MAGIC                 hf_CLM_LN_PROC_CD_MOD_D_ids - hash file from ids to create edw file
# MAGIC                 hf_CLM_LN_PROC_CD_MOD_D_edw - hash file from edw to compare
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC                   /dev/null - no records, just used to re-init the IUD hf
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                Developer            Date            Description                                                              Project                       Environment                                  Code Reviewer                     Date  Reviewed
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Brent leland     03/24/2006    Changed parameters to evironment variables.
# MAGIC                                                              Took out trim() on SK values.
# MAGIC               Brent Leland    05/11/2006    Removed trim() on codes and IDs
# MAGIC 
# MAGIC               Siva Devagiri    09-16-2013     Rewrite in Parallel                                                           5114                      EnterpriseWrhsDevl                 Peter Marshall                         12/19/2013

# MAGIC Job Name:   IdsEdwClmLnProcCdModDExtr
# MAGIC Add Defaults and Null Handling
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write CLM_LN_PROC_CD_MOD_D Data into a Sequential file for Load Job.
# MAGIC Read from source table 
# MAGIC CLM_LN_PROC_CD_MOD
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# Get JDBC connectivity
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: db2_CLM_LN_PROC_CD_MOD_in
extract_query_db2_CLM_LN_PROC_CD_MOD_in = f"""
SELECT 
CLM_LN_PROC_CD_MOD_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
MOD.CLM_ID,
CLM_LN_SEQ_NO,
CLM_LN_PROC_CD_MOD_ORDNL_CD_SK,
CLM_LN_SK,
PROC_CD_MOD_TX
FROM 
{IDSOwner}.CLM_LN_PROC_CD_MOD MOD
INNER JOIN {IDSOwner}.W_EDW_ETL_DRVR DRVR 
  ON MOD.CLM_ID = DRVR.CLM_ID
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON MOD.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_CLM_LN_PROC_CD_MOD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_LN_PROC_CD_MOD_in)
    .load()
)

# Stage: db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM
{IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# Stage: Lkp_Cds (PxLookup)
df_Lkp_Cds_pre = df_db2_CLM_LN_PROC_CD_MOD_in.alias("A").join(
    df_db2_CD_MPPNG_in.alias("B"),
    F.col("A.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK") == F.col("B.CD_MPPNG_SK"),
    "left"
).select(
    F.col("A.CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
    F.col("A.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("A.CLM_ID").alias("CLM_ID"),
    F.col("A.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("B.TRGT_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.col("B.TRGT_CD_NM").alias("CLM_LN_PROC_CD_MOD_ORDNL_NM"),
    F.col("A.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    F.col("A.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("A.PROC_CD_MOD_TX").alias("PROC_CD_MOD_TX")
)
# Apply rpad for PROC_CD_MOD_TX since it is char(2)
df_Lkp_Cds = df_Lkp_Cds_pre.withColumn(
    "PROC_CD_MOD_TX", rpad("PROC_CD_MOD_TX", 2, " ")
)

# Stage: xfrm_BusinessLogic (CTransformerStage) - Output 1 (lnk_IdsEdwClmLnProcCdModDExtr_Main)
df_xfrm_BusinessLogic_Main = df_Lkp_Cds.filter(
    (F.col("CLM_LN_PROC_CD_MOD_SK") != 0) & (F.col("CLM_LN_PROC_CD_MOD_SK") != 1)
).select(
    F.col("CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_NM").alias("CLM_LN_PROC_CD_MOD_ORDNL_NM"),
    F.col("PROC_CD_MOD_TX").alias("CLM_LN_PROC_CD_MOD_TX"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK")
)

# Stage: xfrm_BusinessLogic (CTransformerStage) - Output 2 (In_Na)
# Emulate constraint: only 1 row
df_xfrm_BusinessLogic_In_Na = df_Lkp_Cds.withColumn(
    "rn", F.row_number().over(Window.orderBy(F.lit(1)))
).filter(
    F.col("rn") == 1
).select(
    F.lit(1).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit("NA").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("NA").alias("CLM_LN_PROC_CD_MOD_ORDNL_NM"),
    F.lit(None).alias("CLM_LN_PROC_CD_MOD_TX"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    F.lit(1).alias("CLM_LN_SK")
)

# Stage: xfrm_BusinessLogic (CTransformerStage) - Output 3 (In_Unk)
# Emulate constraint: only 1 row
df_xfrm_BusinessLogic_In_Unk = df_Lkp_Cds.withColumn(
    "rn", F.row_number().over(Window.orderBy(F.lit(1)))
).filter(
    F.col("rn") == 1
).select(
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit("UNK").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("UNK").alias("CLM_LN_PROC_CD_MOD_ORDNL_NM"),
    F.lit(None).alias("CLM_LN_PROC_CD_MOD_TX"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    F.lit(0).alias("CLM_LN_SK")
)

# Stage: Funnel_8 (PxFunnel)
common_cols = [
    "CLM_LN_PROC_CD_MOD_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_PROC_CD_MOD_ORDNL_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_LN_PROC_CD_MOD_ORDNL_NM",
    "CLM_LN_PROC_CD_MOD_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
    "CLM_LN_SK"
]

df_funnel_8_main = df_xfrm_BusinessLogic_Main.select(*common_cols)
df_funnel_8_na = df_xfrm_BusinessLogic_In_Na.select(*common_cols)
df_funnel_8_unk = df_xfrm_BusinessLogic_In_Unk.select(*common_cols)

df_Funnel_8 = df_funnel_8_main.unionByName(df_funnel_8_na, allowMissingColumns=True).unionByName(df_funnel_8_unk, allowMissingColumns=True)

# Final select ensuring column order and applying rpad on char columns
df_final = df_Funnel_8.select(
    F.col("CLM_LN_PROC_CD_MOD_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_NM"),
    rpad(F.col("CLM_LN_PROC_CD_MOD_TX"), 2, " ").alias("CLM_LN_PROC_CD_MOD_TX"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"),
    F.col("CLM_LN_SK")
)

# Stage: seq_CLM_LN_PROC_CD_MOD_D_csv_load (PxSequentialFile)
write_files(
    df_final,
    f"{adls_path}/load/CLM_LN_PROC_CD_MOD_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)
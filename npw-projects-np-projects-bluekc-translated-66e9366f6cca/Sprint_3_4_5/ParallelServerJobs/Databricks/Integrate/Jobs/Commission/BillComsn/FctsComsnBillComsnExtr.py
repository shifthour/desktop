# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnBillComsnExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from FACETS CMC_BLCO_COMM_ITEM
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	CMC_BLCO_COMM_ITEM
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_comsn_bill_comsn
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                             FORMAT.DATE
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 2005-10-26     O. Nielsen   Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                               ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/1/2007          3264                              Added Balancing process to the overall                         devlIDS30                 Steph Goddard             09/17/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/05/2008       3567                             Added new primay key contianer and SrcSysCdsk          devlIDS                     Steph Goddard             09/22/2008              
# MAGIC                                                                                                       and SrcSysCd  
# MAGIC Dan Long                         4/19/2013        TTR-1492                   Changed the Performance parameters to the new             IntegrateNewDevl    Bhoomi Dasari               4/30/2013
# MAGIC                                                                                                      standard values of 512 for the buffer size and 300
# MAGIC                                                                                                      for the timeout value. No code logic TTR-1492                
# MAGIC                                                                                                      was modified.            
# MAGIC 
# MAGIC Prabhu ES                        2022-03-01      S2S Remediation          MSSQL connection parameters added                            IntegrateDev5

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Extract Facets Member Eligibility Data
# MAGIC Hash file hf_bill_comsn_allcol cleared
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, lit, when, length, substring, concat_ws, trim as pyspark_trim, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/BillComsnPK
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: CMC_BLCO_COMM_ITEM (ODBCConnector)
extract_query_1 = f"SELECT * FROM {FacetsOwner}.CMC_BLCO_COMM_ITEM"
jdbc_url_1, jdbc_props_1 = get_db_config(facets_secret_name)
df_CMC_BLCO_COMM_ITEM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_1)
    .options(**jdbc_props_1)
    .option("query", extract_query_1)
    .load()
)

# Stage: StripField (CTransformerStage)
df_StripField = df_CMC_BLCO_COMM_ITEM.select(
    col("BLEI_CK").alias("BLEI_CK"),
    strip_field(col("CSPI_ID")).alias("CSPI_ID"),
    strip_field(col("PMFA_ID")).alias("PMFA_ID"),
    strip_field(col("BLCO_SEQ_NO")).alias("BLCO_SEQ_NO"),
    strip_field(col("COAR_ID")).alias("COAR_ID"),
    col("BLCO_EFF_DT").alias("BLCO_EFF_DT"),
    col("BLCO_TERM_DT").alias("BLCO_TERM_DT"),
    col("BLCO_PCT").alias("BLCO_PCT"),
    col("BLCO_LOCK_TOKEN").alias("BLCO_LOCK_TOKEN"),
    col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID")
)

# Stage: BusinessRules (CTransformerStage) → two output links
df_BusinessRules_AllCol = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("BLEI_CK").alias("BLEI_CK"),
    col("CSPI_ID").alias("CSPI_ID"),
    when(
        (length(pyspark_trim(col("PMFA_ID"))) == 0) | (col("PMFA_ID").isNull()),
        lit("NA")
    ).otherwise(col("PMFA_ID")).alias("PMFA_ID"),
    col("BLCO_SEQ_NO").alias("BLCO_SEQ_NO"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    concat_ws(";", lit("FACETS"), col("BLEI_CK"), col("CSPI_ID"), col("PMFA_ID"), col("BLCO_SEQ_NO")).alias("PRI_KEY_STRING"),
    col("COAR_ID").alias("COAR_ID"),
    substring(col("BLCO_EFF_DT"), 1, 10).alias("BLCO_EFF_DT"),
    substring(col("BLCO_TERM_DT"), 1, 10).alias("BLCO_TERM_DT"),
    (col("BLCO_PCT") / 10000).alias("BLCO_PCT"),
    col("BLCO_LOCK_TOKEN").alias("BLCO_LOCK_TOKEN"),
    col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID")
)

df_BusinessRules_Transform = df_StripField.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("BLEI_CK").alias("BILL_ENTY_UNIQ_KEY"),
    col("CSPI_ID").alias("CLS_PLN_ID"),
    when(
        (length(pyspark_trim(col("PMFA_ID"))) == 0) | (col("PMFA_ID").isNull()),
        lit("NA")
    ).otherwise(col("PMFA_ID")).alias("FEE_DSCNT_ID"),
    col("BLCO_SEQ_NO").alias("SEQ_NO")
)

# Stage: BillComsnPK (CContainerStage / Shared Container)
params_BillComsnPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_BillComsnPK = BillComsnPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_BillComsnPK)

# Stage: IdsComsnBillComsnExtr (CSeqFileStage) - final select + rpad
df_final_IdsComsnBillComsnExtr = df_BillComsnPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("BILL_COMSN_SK").alias("BILL_COMSN_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    rpad(col("CLS_PLN_ID"), 20, " ").alias("CLS_PLN_ID"),
    col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("SEQ_NO").alias("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    rpad(col("COMSN_ARGMT"), 12, " ").alias("COMSN_ARGMT"),
    col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    rpad(col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    col("PRM_PCT").alias("PRM_PCT")
)

write_files(
    df_final_IdsComsnBillComsnExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: Facets_Source (ODBCConnector)
extract_query_2 = f"SELECT * FROM {FacetsOwner}.CMC_BLCO_COMM_ITEM"
jdbc_url_2, jdbc_props_2 = get_db_config(facets_secret_name)
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option("query", extract_query_2)
    .load()
)

# Stage: Transform (CTransformerStage) for Snapshot
df_Transform = df_Facets_Source.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("BLEI_CK").alias("BILL_ENTY_UNIQ_KEY"),
    pyspark_trim(strip_field(col("CSPI_ID"))).alias("CLS_PLN_ID"),
    when(
        (length(pyspark_trim(col("PMFA_ID"))) == 0) | (col("PMFA_ID").isNull()),
        lit("NA")
    ).otherwise(pyspark_trim(strip_field(col("PMFA_ID")))).alias("FEE_DSCNT_ID"),
    col("BLCO_SEQ_NO").alias("SEQ_NO")
)

# Stage: Snapshot_File (CSeqFileStage) - final select + rpad
df_final_Snapshot_File = df_Transform.select(
    col("SRC_SYS_CD_SK"),
    col("BILL_ENTY_UNIQ_KEY"),
    rpad(col("CLS_PLN_ID"), 8, " ").alias("CLS_PLN_ID"),
    col("FEE_DSCNT_ID"),
    col("SEQ_NO")
)

write_files(
    df_final_Snapshot_File,
    f"{adls_path}/load/B_BILL_COMSN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
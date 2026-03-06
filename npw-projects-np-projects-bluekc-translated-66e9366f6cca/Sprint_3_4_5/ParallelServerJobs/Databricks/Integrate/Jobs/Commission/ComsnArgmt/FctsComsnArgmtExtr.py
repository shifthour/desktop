# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsComsnArgmtExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_COAR_ARANGEMNT for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:     Facets -  CMC_COAR_ARANGEMNT 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                              Trim
# MAGIC                              GetFkeyCodes
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Commision Arrangement subject area.
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Hugh Sisson    10/26/2005  -  Originally Program
# MAGIC 
# MAGIC 
# MAGIC Developer                 Date               Project/Altiris #         Change Description                                                                    Development Project    Code Reviewer       Date Reviewed
# MAGIC ------------------------------   -------------------    -----------------------------    ------------------------------------------------------------------------------------------------   ----------------------------------   ----------------------------   -------------------------   
# MAGIC Parikshith Chada      5/30/2007      3264                         Added Balancing process to the overall                                      devlIDS30                    Steph Goddard       09/17/2007
# MAGIC                                                                                          job that takes a snapshot of the source data                        
# MAGIC Bhoomi Dasari          09/09/2008    3567                         Added new primay key contianer and SrcSysCdsk  devlIDS     
# MAGIC                                                                                          and SrcSysCd 
# MAGIC Manasa Andru          2015-03-05     TFS - 10619             Updated the business rule to drop the record for spaces/blank  IntegrateNewDevl        Kalyan Neelam       2015-03-11
# MAGIC                                                                                          COAR_ID field in the StripFields transformer. 
# MAGIC Prabhu ES                2022-03-01     S2S Remediation      MSSQL connection parameters added                                       IntegrateDev5      	Ken Bradmon	2022-06-12        
# MAGIC                                                                                          Added 2 Dedup stages to remove rows with 
# MAGIC                                                                                          duplicate COAR_ID value

# MAGIC There are 4 pairs of rows where the COAR_ID values are the same except the one of each pair has a leading space.
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Trim all string variables except for COAR_ID.  (TrimB is used on COAR_ID as some values have a leading space character)
# MAGIC Extract Facets Commission Arrangement Data
# MAGIC Apply business logic
# MAGIC Hash file hf_comsn_argmt_allcol cleared
# MAGIC Balancing snapshot of source table
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnArgmtPK
# COMMAND ----------

from pyspark.sql.functions import col, lit, length, concat, substring, rpad
from pyspark.sql.functions import expr  # Needed if any SQL expressions are required
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT * FROM {FacetsOwner}.CMC_COAR_ARANGEMNT"
df_CMC_COAR_ARANGEMNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripFields_temp = df_CMC_COAR_ARANGEMNT.filter(length(trim(col("COAR_ID"))) > 0)
df_StripFields = df_StripFields_temp.select(
    trim(col("COAR_ID")).alias("COAR_ID"),
    trim(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", col("COAR_DESC"))).alias("COAR_DESC"),
    col("COAR_PER_BEG_DT").alias("COAR_PER_BEG_DT")
)

df_Dedup_Main = df_StripFields.dropDuplicates(["COAR_ID"])

df_BusinessRules_allcol = df_Dedup_Main.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("COAR_ID").alias("COMSN_ARGMT_ID"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    concat(lit("FACETS"), lit(";"), col("COAR_ID")).alias("PRI_KEY_STRING"),
    lit(0).alias("COMSN_ARGMT_SK"),
    lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    substring(col("COAR_PER_BEG_DT"), 1, 10).alias("BEG_DT"),
    col("COAR_DESC").alias("ARGMT_DESC")
)

df_BusinessRules_transform = df_Dedup_Main.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(col("COAR_ID")).alias("COMSN_ARGMT_ID")
)

params_ComsnArgmtPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}
df_IdsComsnArgmtExtr = ComsnArgmtPK(df_BusinessRules_allcol, df_BusinessRules_transform, params_ComsnArgmtPK)

df_IdsComsnArgmtExtr_out = df_IdsComsnArgmtExtr.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("COMSN_ARGMT_SK"),
    rpad(col("COMSN_ARGMT_ID"), 12, " ").alias("COMSN_ARGMT_ID"),
    col("CRT_RUN_CYC_EXTCN_SK"),
    col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    rpad(col("BEG_DT"), 10, " ").alias("BEG_DT"),
    rpad(col("ARGMT_DESC"), 70, " ").alias("ARGMT_DESC")
)

write_files(
    df_IdsComsnArgmtExtr_out,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

extract_query_2 = f"SELECT * FROM {FacetsOwner}.CMC_COAR_ARANGEMNT"
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_Transform_2 = df_Facets_Source.select(
    GetFkeyCodes("IDS", 1, "SOURCE SYSTEM", "FACETS", "X").alias("SRC_SYS_CD_SK"),
    trim(col("COAR_ID")).alias("COMSN_ARGMT_ID")
)

df_Dedup_Balancing = df_Transform_2.dropDuplicates(["SRC_SYS_CD_SK", "COMSN_ARGMT_ID"])

df_Snapshot_File_out = df_Dedup_Balancing.select(
    col("SRC_SYS_CD_SK"),
    rpad(col("COMSN_ARGMT_ID"), <...>, " ").alias("COMSN_ARGMT_ID")
)

write_files(
    df_Snapshot_File_out,
    f"{adls_path}/load/B_COMSN_ARGMT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
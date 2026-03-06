# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     ScorecardEmplProdItemDailyCntl
# MAGIC 
# MAGIC 
# MAGIC Modifications:
# MAGIC =====================================================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Altiris #\(9)                    Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)             Date Reviewed
# MAGIC =====================================================================================================================================================================
# MAGIC Sruthi M    \(9)07/09/2018\(9)5236-Indigo Replacement\(9)     Original Development\(9)\(9)\(9)\(9)EnterpriseDev1\(9)\(9)Hugh Sisson                         2018-07-20
# MAGIC Sruthi M    \(9)07/09/2018\(9)5236-Indigo Replacement\(9)     Changed the size of INTRNL_FUNC_CD                           EnterpriseDev2\(9)\(9)Abhiram Dasarathy\(9)\(9)2018-11-14
# MAGIC                                                                                                                       field in the source layout
# MAGIC                                                                                                                    that are not in the employees list.
# MAGIC Karthik C                   2019-06-14           Bug-114191                  Added the substring to pull only thw 13 characters                                 Enterprise Dev1\(9)\(9) Kalyan Neelam                      2019-06-14
# MAGIC                                                                                                      for B2_USER_ID column in  xfm_HPA transformer

# MAGIC HPA Extract
# MAGIC The sequential file is used by the ScrcrdEmplProdItemLoad  job to Load Table SCRCRD_EMPL_PROD_ITEM table in ScoreCard Database
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, upper, substring, when, to_timestamp, concat, lit, startswith
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve all job parameters
APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS = get_widget_value('APT_IMPORT_NOWARN_STRING_FIELD_OVERRUNS','1')
ScoreCardOwner = get_widget_value('ScoreCardOwner','')
CurrentRunDtm = get_widget_value('CurrentRunDtm','')
Source = get_widget_value('Source','HPA')

# Because this job reads from a database using $ScoreCardOwner, treat it as a generic secret name
scorecard_secret_name = get_widget_value('scorecard_secret_name','')

# 1) --------------------------------------------------------------------------------
# ScrCrd_EMPL (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(scorecard_secret_name)
extract_query = f"SELECT UPPER(EMPL_BLUE_SQRD_ID) as EMPL_BLUE_SQRD_ID, EMPL_ID FROM {ScoreCardOwner}.EMPL"
df_ScrCrd_EMPL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# 2) --------------------------------------------------------------------------------
# Seq_HPA (PxSequentialFile) - reading a file
schema_Seq_HPA = StructType([
    StructField("SCCF_NO", StringType(), True),
    StructField("B2_USER_ID", StringType(), True),
    StructField("FLR", StringType(), True),
    StructField("RCRD_DT", StringType(), True),
    StructField("INTRNL_FUNC_CD", StringType(), True)
])
# (No explicit path given in JSON, using a generic path under adls_path)
file_path_Seq_HPA = f"{adls_path}/Seq_HPA.dat"
df_Seq_HPA = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_Seq_HPA)
    .load(file_path_Seq_HPA)
)

# 3) --------------------------------------------------------------------------------
# xfm_HPA (CTransformerStage)
df_xfm_HPA = df_Seq_HPA.select(
    upper(trim(substring(col("B2_USER_ID"), 1, 13))).alias("B2_USER_ID"),
    trim(col("SCCF_NO")).alias("SCCF_NO"),
    trim(col("FLR")).alias("FLR"),
    trim(col("RCRD_DT")).alias("RCRD_DT"),
    trim(col("INTRNL_FUNC_CD")).alias("INTRNL_FUNC_CD")
)

# 4) --------------------------------------------------------------------------------
# lkp_Empl_HPA_id (PxLookup)
# Left join on B2_USER_ID = EMPL_BLUE_SQRD_ID (inferred from usage)
df_lkp_Empl_HPA_id_joined = df_xfm_HPA.alias("Lnk_HPA").join(
    df_ScrCrd_EMPL.alias("Lnk_ScrCrd_Empl"),
    col("Lnk_HPA.B2_USER_ID") == col("Lnk_ScrCrd_Empl.EMPL_BLUE_SQRD_ID"),
    how="left"
)

# Primary link output (matched rows)
df_lkp_Empl_HPA_id_out = df_lkp_Empl_HPA_id_joined.select(
    col("Lnk_HPA.B2_USER_ID").alias("B2_USER_ID"),
    col("Lnk_HPA.SCCF_NO").alias("SCCF_NO"),
    col("Lnk_ScrCrd_Empl.EMPL_ID").alias("EMPL_ID"),
    col("Lnk_HPA.FLR").alias("FLR"),
    col("Lnk_HPA.RCRD_DT").alias("RCRD_DT"),
    col("Lnk_HPA.INTRNL_FUNC_CD").alias("INTRNL_FUNC_CD")
)

# Reject link output (null EMPL_ID)
df_rej_hpa_id = df_lkp_Empl_HPA_id_out.filter(col("EMPL_ID").isNull())
df_lkp_Empl_HPA_id_out = df_lkp_Empl_HPA_id_out.filter(col("EMPL_ID").isNotNull())

# 5) --------------------------------------------------------------------------------
# Seq_Scrcrd_Empl_Id_Rej (PxSequentialFile) - write out rejects
file_path_rej = f"{adls_path_publish}/external/Scrcrd_Empl_{Source}_Empl_Id_Rej.dat"
write_files(
    df_rej_hpa_id,
    file_path_rej,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# 6) --------------------------------------------------------------------------------
# Xfm_HPA_Info_Business_Logic (CTransformerStage)
df_Xfm_HPA_Info_Business_Logic = df_lkp_Empl_HPA_id_out.withColumn(
    "RcrdDt",
    concat(col("RCRD_DT"), lit(" 00:00:00"))
).select(
    when(col("EMPL_ID").isNull(), lit("NA")).otherwise(col("EMPL_ID")).alias("EMPL_ID"),
    col("SCCF_NO").alias("SCRCRD_ITEM_ID"),
    current_timestamp().alias("SCRCRD_EXTR_DTM"),
    lit("PLANX_HPA_FILE").alias("SRC_SYS_CD"),
    lit("HPA").alias("SCRCRD_ITEM_CAT_ID"),
    col("INTRNL_FUNC_CD").alias("SCRCRD_ITEM_TYP_ID"),
    lit("HPA").alias("SCRCRD_WORK_TYP_ID"),
    lit("NA").alias("SRC_SYS_INPT_METH_CD"),
    when(trim(col("INTRNL_FUNC_CD")).startswith("SF"), lit("SUBMISSION")).otherwise(lit("DISPOSITION")).alias("SRC_SYS_STATUS_CD"),
    lit("NA").alias("GRP_ID"),
    lit("NA").alias("MBR_ID"),
    lit("NA").alias("PROD_ABBR"),
    lit("NA").alias("PROD_LOB_NO"),
    col("SCCF_NO").alias("SCCF_NO"),
    col("RcrdDt").alias("SRC_SYS_RCVD_DTM"),
    col("RcrdDt").alias("SRC_SYS_CMPLD_DTM"),
    col("RcrdDt").alias("SRC_SYS_TRANS_DTM"),
    lit(1).alias("ACTVTY_CT"),
    lit("HPA EXTRACT").alias("CRT_USER_ID"),
    lit(CurrentRunDtm).alias("LAST_UPDT_DTM"),
    lit("HPA EXTRACT").alias("LAST_UPDT_USER_ID")
)

# 7) --------------------------------------------------------------------------------
# Xfm_HPA_Info (CTransformerStage)
df_Xfm_HPA_Info = df_Xfm_HPA_Info_Business_Logic.select(
    col("EMPL_ID").alias("EMPL_ID"),
    col("SCRCRD_ITEM_ID").alias("SCRCRD_ITEM_ID"),
    to_timestamp(col("SCRCRD_EXTR_DTM"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("RCRD_EXTR_DTM"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SCRCRD_ITEM_CAT_ID").alias("SCRCRD_ITEM_CAT_ID"),
    col("SCRCRD_ITEM_TYP_ID").alias("SCRCRD_ITEM_TYP_ID"),
    col("SCRCRD_WORK_TYP_ID").alias("SCRCRD_WORK_TYP_ID"),
    col("SRC_SYS_INPT_METH_CD").alias("SRC_SYS_INPT_METH_CD"),
    col("SRC_SYS_STATUS_CD").alias("SRC_SYS_STTUS_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("MBR_ID").alias("MBR_ID"),
    col("PROD_ABBR").alias("PROD_ABBR"),
    col("PROD_LOB_NO").alias("PROD_LOB_NO"),
    col("SCCF_NO").alias("SCCF_NO"),
    col("SRC_SYS_RCVD_DTM").alias("SRC_SYS_RCVD_DTM"),
    col("SRC_SYS_CMPLD_DTM").alias("SRC_SYS_CMPLD_DTM"),
    col("SRC_SYS_TRANS_DTM").alias("SRC_SYS_TRANS_DTM"),
    col("ACTVTY_CT").alias("ACTVTY_CT"),
    lit(CurrentRunDtm).alias("CRT_DTM"),
    col("CRT_USER_ID").alias("CRT_USER_ID"),
    col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

# 8) --------------------------------------------------------------------------------
# Scrcrd_Empl_Prod_Item_Load_Ready_File (PxSequentialFile) - final write
df_Scrcrd_Empl_Prod_Item_Load_Ready_File = df_Xfm_HPA_Info.select(
    "EMPL_ID",
    "SCRCRD_ITEM_ID",
    "RCRD_EXTR_DTM",
    "SRC_SYS_CD",
    "SCRCRD_ITEM_CAT_ID",
    "SCRCRD_ITEM_TYP_ID",
    "SCRCRD_WORK_TYP_ID",
    "SRC_SYS_INPT_METH_CD",
    "SRC_SYS_STTUS_CD",
    "GRP_ID",
    "MBR_ID",
    "PROD_ABBR",
    "PROD_LOB_NO",
    "SCCF_NO",
    "SRC_SYS_RCVD_DTM",
    "SRC_SYS_CMPLD_DTM",
    "SRC_SYS_TRANS_DTM",
    "ACTVTY_CT",
    "CRT_DTM",
    "CRT_USER_ID",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID"
)

file_path_final = f"{adls_path}/load/Scrcrd_Prod_Item_{Source}.dat"
write_files(
    df_Scrcrd_Empl_Prod_Item_Load_Ready_File,
    file_path_final,
    delimiter="#$",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)
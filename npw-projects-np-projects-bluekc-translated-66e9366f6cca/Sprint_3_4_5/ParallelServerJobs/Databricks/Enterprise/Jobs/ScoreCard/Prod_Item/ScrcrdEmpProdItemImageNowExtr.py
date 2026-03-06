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
# MAGIC Developer \(9)Date\(9)\(9)Project/Altiris #\(9)                    Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)             Date Reviewed
# MAGIC =====================================================================================================================================================================
# MAGIC Sruthi M    \(9)09/01/2018\(9)5236-Indigo Replacement\(9)    Original Development\(9)\(9)\(9)\(9)EnterpriseDev1\(9)\(9)Kalyan Neelam                      2018-09-27

# MAGIC The sequential file is used by the ScrcrdEmplProdItemLoad  job to Load Table SCRCRD_EMPL_PROD_ITEM table in ScoreCard Database
# MAGIC The file is TAB delimited.and received from LexMark. for the process of load to ScoreCards
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


ScoreCardOwner = get_widget_value('ScoreCardOwner','')
CurrentRunDtm = get_widget_value('CurrentRunDtm','')
Source = get_widget_value('Source','ImageNow')
scorecard_secret_name = get_widget_value('scorecard_secret_name','')

jdbc_url, jdbc_props = get_db_config(scorecard_secret_name)
extract_query = f"SELECT UPPER(EMPL_ACTV_DIR_ID) as EMPL_ACTV_DIR_ID, EMPL_ID FROM {ScoreCardOwner}.EMPL"
df_ScrCrd_EMPL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

schema_WrtnCorresFile = StructType([
    StructField("BLUE_KC_DOC_ID", StringType(), False),
    StructField("QUEUE_NM", StringType(), False),
    StructField("CMPL_DT", StringType(), True),
    StructField("LAST_RTE_BY_USER_ID", StringType(), True),
    StructField("CRSPNDNC_STRT_TM", StringType(), True)
])
file_path_WrtnCorresFile = "<...>"
df_WrtnCorresFile = (
    spark.read
    .option("sep", "\t")
    .option("header", "false")
    .schema(schema_WrtnCorresFile)
    .csv(file_path_WrtnCorresFile)
)

df_lnk_ImageNow = df_WrtnCorresFile.select(
    trim("BLUE_KC_DOC_ID").alias("BLUE_KC_DOC_ID"),
    trim("QUEUE_NM").alias("QUEUE_NM"),
    trim("CMPL_DT").alias("CMPL_DT"),
    trim(upcase("LAST_RTE_BY_USER_ID")).alias("LAST_RTE_BY_USER_ID"),
    trim("CRSPNDNC_STRT_TM").alias("CRSPNDNC_STRT_TM")
)

df_lkp_empl_ImageNow_out = (
    df_lnk_ImageNow.alias("A")
    .join(
        df_ScrCrd_EMPL.alias("B"),
        col("A.LAST_RTE_BY_USER_ID") == col("B.EMPL_ACTV_DIR_ID"),
        "left"
    )
    .select(
        col("B.EMPL_ID").alias("EMPL_ID"),
        col("A.BLUE_KC_DOC_ID").alias("BLUE_KC_DOC_ID"),
        col("A.QUEUE_NM").alias("QUEUE_NM"),
        col("A.CMPL_DT").alias("CMPL_DT"),
        col("A.LAST_RTE_BY_USER_ID").alias("LAST_RTE_BY_USER_ID"),
        col("A.CRSPNDNC_STRT_TM").alias("CRSPNDNC_STRT_TM")
    )
)

df_lkp_empl_ImageNow_info = df_lkp_empl_ImageNow_out.filter(col("EMPL_ID").isNotNull())
df_lkp_empl_ImageNow_rej = df_lkp_empl_ImageNow_out.filter(col("EMPL_ID").isNull())

df_lkp_empl_ImageNow_rej_final = df_lkp_empl_ImageNow_rej.select(
    rpad(col("EMPL_ID"), <...>, " ").alias("EMPL_ID"),
    rpad(col("BLUE_KC_DOC_ID"), <...>, " ").alias("BLUE_KC_DOC_ID"),
    rpad(col("QUEUE_NM"), <...>, " ").alias("QUEUE_NM"),
    rpad(col("CMPL_DT"), <...>, " ").alias("CMPL_DT"),
    rpad(col("LAST_RTE_BY_USER_ID"), <...>, " ").alias("LAST_RTE_BY_USER_ID"),
    rpad(col("CRSPNDNC_STRT_TM"), <...>, " ").alias("CRSPNDNC_STRT_TM")
)
file_path_Seq_Scrcrd_Empl_Id_Rej = f"{adls_path_publish}/external/Scrcrd_Empl_{Source}_Empl_Id_Rej.dat"
write_files(
    df_lkp_empl_ImageNow_rej_final,
    file_path_Seq_Scrcrd_Empl_Id_Rej,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote=None,
    nullValue=None
)

df_Xfm_ImageNow_Info_Business_Logic = df_lkp_empl_ImageNow_info
df_Xfm_ImageNow_Info_Business_Logic = df_Xfm_ImageNow_Info_Business_Logic.withColumn(
    "EMPL_ID",
    when((col("EMPL_ID").isNull()) | (trim(col("EMPL_ID")) == ""), "NA").otherwise(col("EMPL_ID"))
)
df_Xfm_ImageNow_Info_Business_Logic = df_Xfm_ImageNow_Info_Business_Logic.withColumn(
    "SCRCRD_ITEM_ID",
    when(
        (col("BLUE_KC_DOC_ID").isNull()) | (strip_field("BLUE_KC_DOC_ID") == ""),
        convert(' ', '', convert(':', '', convert('-', '', CurrentRunDtm)))
    ).otherwise(strip_field("BLUE_KC_DOC_ID"))
)
df_Xfm_ImageNow_Info_Business_Logic = df_Xfm_ImageNow_Info_Business_Logic.withColumn(
    "RCRD_EXTR_DTM",
    CurrentTimestampMS()
).withColumn(
    "SRC_SYS_CD", lit("IMAGENOW")
).withColumn(
    "SCRCRD_ITEM_CAT_ID", lit("WC")
).withColumn(
    "SCRCRD_ITEM_TYP_ID", lit("WC")
).withColumn(
    "SCRCRD_WORK_TYP_ID", lit("WC")
).withColumn(
    "SRC_SYS_INPT_METH_CD", lit("")
).withColumn(
    "SRC_SYS_STATUS_CD", lit("COMPLETE")
).withColumn(
    "GRP_ID", lit("")
).withColumn(
    "MBR_ID", lit("")
).withColumn(
    "PROD_ABBR", lit("")
).withColumn(
    "PROD_LOB_NO", lit("")
).withColumn(
    "SCCF_NO", lit("")
).withColumn(
    "SRC_SYS_RCVD_DTM", col("CRSPNDNC_STRT_TM")
).withColumn(
    "SRC_SYS_CMPLD_DTM", col("CMPL_DT")
).withColumn(
    "SRC_SYS_TRANS_DTM", col("CMPL_DT")
).withColumn(
    "ACTVTY_CT", lit(1)
).withColumn(
    "CRT_USER_ID", lit("WC EXTRACT")
).withColumn(
    "LAST_UPDT_DTM", lit(CurrentRunDtm)
).withColumn(
    "LAST_UPDT_USER_ID", lit("WC EXTRACT")
)

df_Xfm_Inquiry_Info = df_Xfm_ImageNow_Info_Business_Logic.select(
    col("EMPL_ID").alias("EMPL_ID"),
    col("SCRCRD_ITEM_ID").alias("SCRCRD_ITEM_ID"),
    stringToTimestamp(col("RCRD_EXTR_DTM"), "%yyyy-%mm-%dd %hh:%nn:%ss.6").alias("RCRD_EXTR_DTM"),
    convert('\n','', convert('\r','', col("SRC_SYS_CD"))).alias("SRC_SYS_CD"),
    convert('\n','', convert('\r','', col("SCRCRD_ITEM_CAT_ID"))).alias("SCRCRD_ITEM_CAT_ID"),
    convert('\n','', convert('\r','', col("SCRCRD_ITEM_TYP_ID"))).alias("SCRCRD_ITEM_TYP_ID"),
    convert('\n','', convert('\r','', col("SCRCRD_WORK_TYP_ID"))).alias("SCRCRD_WORK_TYP_ID"),
    convert('\n','', convert('\r','', col("SRC_SYS_INPT_METH_CD"))).alias("SRC_SYS_INPT_METH_CD"),
    convert('\n','', convert('\r','', col("SRC_SYS_STATUS_CD"))).alias("SRC_SYS_STTUS_CD"),
    convert('\n','', convert('\r','', col("GRP_ID"))).alias("GRP_ID"),
    convert('\n','', convert('\r','', col("MBR_ID"))).alias("MBR_ID"),
    convert('\n','', convert('\r','', col("PROD_ABBR"))).alias("PROD_ABBR"),
    convert('\n','', convert('\r','', col("PROD_LOB_NO"))).alias("PROD_LOB_NO"),
    convert('\n','', convert('\r','', col("SCCF_NO"))).alias("SCCF_NO"),
    convert('\n','', convert('\r','', col("SRC_SYS_RCVD_DTM"))).alias("SRC_SYS_RCVD_DTM"),
    convert('\n','', convert('\r','', col("SRC_SYS_CMPLD_DTM"))).alias("SRC_SYS_CMPLD_DTM"),
    convert('\n','', convert('\r','', col("SRC_SYS_TRANS_DTM"))).alias("SRC_SYS_TRANS_DTM"),
    convert('\n','', convert('\r','', col("ACTVTY_CT"))).alias("ACTVTY_CT"),
    lit(CurrentRunDtm).alias("CRT_DTM"),
    col("CRT_USER_ID").alias("CRT_USER_ID"),
    lit(CurrentRunDtm).alias("LAST_UPDT_DTM"),
    col("LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID")
)

df_Scrcrd_Empl_Prod_Item_Load_Ready_File_final = df_Xfm_Inquiry_Info.select(
    rpad(col("EMPL_ID"), <...>, " ").alias("EMPL_ID"),
    rpad(col("SCRCRD_ITEM_ID"), <...>, " ").alias("SCRCRD_ITEM_ID"),
    rpad(col("RCRD_EXTR_DTM"), <...>, " ").alias("RCRD_EXTR_DTM"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("SCRCRD_ITEM_CAT_ID"), <...>, " ").alias("SCRCRD_ITEM_CAT_ID"),
    rpad(col("SCRCRD_ITEM_TYP_ID"), <...>, " ").alias("SCRCRD_ITEM_TYP_ID"),
    rpad(col("SCRCRD_WORK_TYP_ID"), <...>, " ").alias("SCRCRD_WORK_TYP_ID"),
    rpad(col("SRC_SYS_INPT_METH_CD"), <...>, " ").alias("SRC_SYS_INPT_METH_CD"),
    rpad(col("SRC_SYS_STTUS_CD"), <...>, " ").alias("SRC_SYS_STTUS_CD"),
    rpad(col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    rpad(col("MBR_ID"), <...>, " ").alias("MBR_ID"),
    rpad(col("PROD_ABBR"), <...>, " ").alias("PROD_ABBR"),
    rpad(col("PROD_LOB_NO"), <...>, " ").alias("PROD_LOB_NO"),
    rpad(col("SCCF_NO"), <...>, " ").alias("SCCF_NO"),
    rpad(col("SRC_SYS_RCVD_DTM"), <...>, " ").alias("SRC_SYS_RCVD_DTM"),
    rpad(col("SRC_SYS_CMPLD_DTM"), <...>, " ").alias("SRC_SYS_CMPLD_DTM"),
    rpad(col("SRC_SYS_TRANS_DTM"), <...>, " ").alias("SRC_SYS_TRANS_DTM"),
    rpad(col("ACTVTY_CT"), <...>, " ").alias("ACTVTY_CT"),
    rpad(col("CRT_DTM"), <...>, " ").alias("CRT_DTM"),
    rpad(col("CRT_USER_ID"), <...>, " ").alias("CRT_USER_ID"),
    rpad(col("LAST_UPDT_DTM"), <...>, " ").alias("LAST_UPDT_DTM"),
    rpad(col("LAST_UPDT_USER_ID"), <...>, " ").alias("LAST_UPDT_USER_ID")
)

file_path_Scrcrd_Empl_Prod_Item_Load_Ready_File = f"{adls_path}/load/Scrcrd_Prod_Item_{Source}.dat"
write_files(
    df_Scrcrd_Empl_Prod_Item_Load_Ready_File_final,
    file_path_Scrcrd_Empl_Prod_Item_Load_Ready_File,
    delimiter="#$",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
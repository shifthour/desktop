# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 07/13/06 09:01:21 Batch  14074_32499 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_1 07/13/06 08:56:37 Batch  14074_32207 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_7 06/28/06 09:07:27 Batch  14059_32851 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_6 06/28/06 09:02:32 Batch  14059_32558 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_5 06/20/06 16:37:06 Batch  14051_59831 PROMOTE bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_5 06/20/06 16:35:44 Batch  14051_59749 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_4 06/20/06 16:34:07 Batch  14051_59651 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/19/06 11:16:37 Batch  14050_40618 PROMOTE bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/19/06 11:10:31 Batch  14050_40241 PROMOTE bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_3 06/19/06 11:02:44 Batch  14050_39770 INIT bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_2 06/07/06 12:19:56 Batch  14038_44402 PROMOTE bckcett testIDS30 u50871 By Tao Luo
# MAGIC ^1_2 06/07/06 12:18:53 Batch  14038_44336 INIT bckcett devlIDS30 u50871 By Tao Luo
# MAGIC ^1_1 06/01/06 10:52:24 Batch  14032_39155 INIT bckcett devlIDS30 u10913 Ollie moving Med Mgt from Devl to Test
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    IdsDataMartMedMgtDmMedMgtNoteTxExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    IDS Medical Management Data Mart Medical Management Note Text extract from IDS to Data Mart.  Extract is based on BeginRunCycle.
# MAGIC 
# MAGIC INPUTS:
# MAGIC                    IDS:  MED_MGT_NOTE_TX
# MAGIC 
# MAGIC HASH FILES:         
# MAGIC                     hf_etrnl_cd_mppng
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                     Lookups to extract fields
# MAGIC                     FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from IDS and pulls to the Data Marts.  Perform no changes with the data except for formatting the DB2 datetimes into Sybase datetimes.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Data Mart:  MED_MGT_DM_MED_MGT_NOTE
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                    Tao Luo:  Original Programming - 02/23/2006
# MAGIC                    Tao Luo:  06/23/2006  -  Added Run Cycle Logic and Join to UM

# MAGIC Target Code Mapping Lookups
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginRunCycle = get_widget_value('BeginRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query = f"SELECT DISTINCT (MED_MGT_NOTE_TX.MED_MGT_NOTE_TX_SK) as MED_MGT_NOTE_TX_SK,MED_MGT_NOTE_TX.SRC_SYS_CD_SK as SRC_SYS_CD_SK,MED_MGT_NOTE_TX.MED_MGT_NOTE_DTM as MED_MGT_NOTE_DTM,MED_MGT_NOTE_TX.MED_MGT_NOTE_INPT_DTM as MED_MGT_NOTE_INPT_DTM,MED_MGT_NOTE_TX.NOTE_TX_SEQ_NO as NOTE_TX_SEQ_NO,MED_MGT_NOTE_TX.NOTE_TX as NOTE_TX FROM {IDSOwner}.MED_MGT_NOTE_TX MED_MGT_NOTE_TX, {IDSOwner}.MED_MGT_NOTE MED_MGT_NOTE, {IDSOwner}.UM UM WHERE MED_MGT_NOTE_TX.MED_MGT_NOTE_DTM = MED_MGT_NOTE.MED_MGT_NOTE_DTM AND MED_MGT_NOTE_TX.MED_MGT_NOTE_INPT_DTM = MED_MGT_NOTE.MED_MGT_NOTE_INPT_DTM AND MED_MGT_NOTE_TX.SRC_SYS_CD_SK = MED_MGT_NOTE.SRC_SYS_CD_SK AND MED_MGT_NOTE.MED_MGT_NOTE_DTM = UM.MED_MGT_NOTE_DTM AND MED_MGT_NOTE.SRC_SYS_CD_SK = UM.SRC_SYS_CD_SK AND MED_MGT_NOTE_TX.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginRunCycle} AND MED_MGT_NOTE_TX.MED_MGT_NOTE_TX_SK NOT IN (0,1)"
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_Lookup = df_IDS.alias("Extract").join(
    df_hf_etrnl_cd_mppng.alias("Src_Sys_Cd_Lookup"),
    F.trim(F.col("Extract.SRC_SYS_CD_SK")) == F.col("Src_Sys_Cd_Lookup.CD_MPPNG_SK"),
    "left"
)

df_Lookup = df_Lookup.filter(F.col("Extract.NOTE_TX").isNotNull())

df_Load = df_Lookup.select(
    F.col("Src_Sys_Cd_Lookup.TRGT_CD").alias("SRC_SYS_CD"),
    FORMAT.DATE(F.col("Extract.MED_MGT_NOTE_DTM"), 'DB2','TIMESTAMP','SYBTIMESTAMP').alias("MED_MGT_NOTE_DTM"),
    FORMAT.DATE(F.col("Extract.MED_MGT_NOTE_INPT_DTM"), 'DB2','TIMESTAMP','SYBTIMESTAMP').alias("MED_MGT_NOTE_INPT_DTM"),
    F.col("Extract.NOTE_TX_SEQ_NO").alias("MED_MGT_NOTE_TX_SEQ_NO"),
    F.col("Extract.NOTE_TX").alias("MED_MGT_NOTE_TX"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO")
)

df_Final = df_Load.withColumn(
    "SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "MED_MGT_NOTE_TX", F.rpad(F.col("MED_MGT_NOTE_TX"), <...>, " ")
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsDataMartMedMgtDmMedMgtNoteTxExtr_MED_MGT_DM_MED_MGT_NOTE_temp", jdbc_url_clmmart, jdbc_props_clmmart)

df_Final.write.format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", "STAGING.IdsDataMartMedMgtDmMedMgtNoteTxExtr_MED_MGT_DM_MED_MGT_NOTE_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MED_MGT_DM_MED_MGT_NOTE_TX AS T
USING STAGING.IdsDataMartMedMgtDmMedMgtNoteTxExtr_MED_MGT_DM_MED_MGT_NOTE_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MED_MGT_NOTE_DTM = S.MED_MGT_NOTE_DTM
    AND T.MED_MGT_NOTE_INPT_DTM = S.MED_MGT_NOTE_INPT_DTM
    AND T.MED_MGT_NOTE_TX_SEQ_NO = S.MED_MGT_NOTE_TX_SEQ_NO
WHEN MATCHED THEN 
   UPDATE SET T.MED_MGT_NOTE_TX = S.MED_MGT_NOTE_TX,
              T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN 
   INSERT 
   (
    SRC_SYS_CD,
    MED_MGT_NOTE_DTM,
    MED_MGT_NOTE_INPT_DTM,
    MED_MGT_NOTE_TX_SEQ_NO,
    MED_MGT_NOTE_TX,
    LAST_UPDT_RUN_CYC_NO
   )
   VALUES
   (
    S.SRC_SYS_CD,
    S.MED_MGT_NOTE_DTM,
    S.MED_MGT_NOTE_INPT_DTM,
    S.MED_MGT_NOTE_TX_SEQ_NO,
    S.MED_MGT_NOTE_TX,
    S.LAST_UPDT_RUN_CYC_NO
   );
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)
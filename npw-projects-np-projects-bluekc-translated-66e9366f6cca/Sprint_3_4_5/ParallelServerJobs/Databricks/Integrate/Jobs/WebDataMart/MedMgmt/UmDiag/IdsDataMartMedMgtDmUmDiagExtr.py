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
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsDataMartMedMgtDmUmDiagExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                 Pulls data from IDS to create medical management data mart  Extract is based on BeginRunCycle.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - UM_DIAG_SET
# MAGIC                
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC 
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                  
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     direct update of MED_MGT_DM_UM_DIAG_SET_D
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen  03/22/2005 -   Originally Programmed
# MAGIC               Tao Luo          06/23/2006 -   Added Run Cycle logic and join to UM

# MAGIC Use SK's to perform all mappings with any Lookup MappingType.
# MAGIC Transform and output link names are used by job control to get link count.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmartowner_secret_name = get_widget_value('clmmartowner_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
BeginRunCycle = get_widget_value('BeginRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_IDSUmDiagSet = f"""
SELECT 
  UM_DIAG_SET.UM_DIAG_SET_SK as UM_DIAG_SET_SK,
  UM_DIAG_SET.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
  UM_DIAG_SET.UM_REF_ID as UM_REF_ID,
  UM_DIAG_SET.DIAG_SET_CRT_DTM as DIAG_SET_CRT_DTM,
  UM_DIAG_SET.SEQ_NO as SEQ_NO,
  UM_DIAG_SET.ONSET_DT_SK as ONSET_DT_SK,
  DIAG.DIAG_CD as DIAG_CD,
  DIAG.DIAG_CD_DESC as DIAG_CD_DESC
FROM {IDSOwner}.UM_DIAG_SET UM_DIAG_SET, {IDSOwner}.DIAG_CD DIAG, {IDSOwner}.UM UM
WHERE UM_DIAG_SET.DIAG_CD_SK = DIAG.DIAG_CD_SK
  AND UM_DIAG_SET.UM_REF_ID = UM.UM_REF_ID
  AND UM_DIAG_SET.SRC_SYS_CD_SK = UM.SRC_SYS_CD_SK
  AND UM_DIAG_SET.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginRunCycle}
  AND UM_DIAG_SET.UM_DIAG_SET_SK NOT IN (0,1)
"""

df_IDSUmDiagSet = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDSUmDiagSet)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_joined = (
    df_IDSUmDiagSet.alias("lnkUmDiagSet")
    .join(
        df_hf_etrnl_cd_mppng.alias("refSrcSys"),
        trim(F.col("lnkUmDiagSet.SRC_SYS_CD_SK")) == F.col("refSrcSys.CD_MPPNG_SK"),
        "left"
    )
)

df_Trans1 = (
    df_joined
    .withColumn("SRC_SYS_CD", F.col("refSrcSys.TRGT_CD"))
    .withColumn("UM_REF_ID", F.col("lnkUmDiagSet.UM_REF_ID"))
    .withColumn("UM_DIAG_SET_CRT_DTM",
                F.substring(
                    FORMAT_DATE(F.col("lnkUmDiagSet.DIAG_SET_CRT_DTM"), 'DB2','TIMESTAMP','SYBTIMESTAMP'),
                    1,
                    23
                ))
    .withColumn("UM_DIAG_SET_SEQ_NO", F.col("lnkUmDiagSet.SEQ_NO"))
    .withColumn("DIAG_CD", F.col("lnkUmDiagSet.DIAG_CD"))
    .withColumn("DIAG_CD_DESC", F.col("lnkUmDiagSet.DIAG_CD_DESC"))
    .withColumn(
        "UM_DIAG_SET_ONSET_DT",
        F.when(
            (trim(F.col("lnkUmDiagSet.ONSET_DT_SK")) == 'NA') | (trim(F.col("lnkUmDiagSet.ONSET_DT_SK")) == 'UNK'),
            F.lit(None)
        ).otherwise(
            FORMAT_DATE(F.col("lnkUmDiagSet.ONSET_DT_SK"), 'DB2','TIMESTAMP','SYBTIMESTAMP')
        )
    )
    .withColumn("LAST_UPDT_RUN_CYC_NO", F.lit(CurrRunCycle))
)

df_Trans1 = df_Trans1.select(
    "SRC_SYS_CD",
    "UM_REF_ID",
    "UM_DIAG_SET_CRT_DTM",
    "UM_DIAG_SET_SEQ_NO",
    "DIAG_CD",
    "DIAG_CD_DESC",
    "UM_DIAG_SET_ONSET_DT",
    "LAST_UPDT_RUN_CYC_NO"
)

df_Trans1 = (
    df_Trans1
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("DIAG_CD", rpad(F.col("DIAG_CD"), <...>, " "))
    .withColumn("DIAG_CD_DESC", rpad(F.col("DIAG_CD_DESC"), <...>, " "))
)

jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmartowner_secret_name)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDataMartMedMgtDmUmDiagExtr_MED_MGT_DM_UM_DIAG_SET_D_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

(
    df_Trans1.write.format("jdbc")
    .option("url", jdbc_url_clmmart)
    .options(**jdbc_props_clmmart)
    .option("dbtable", "STAGING.IdsDataMartMedMgtDmUmDiagExtr_MED_MGT_DM_UM_DIAG_SET_D_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MED_MGT_DM_UM_DIAG_SET AS T
USING STAGING.IdsDataMartMedMgtDmUmDiagExtr_MED_MGT_DM_UM_DIAG_SET_D_temp AS S
ON 
  T.SRC_SYS_CD = S.SRC_SYS_CD AND
  T.UM_REF_ID = S.UM_REF_ID AND
  T.UM_DIAG_SET_CRT_DTM = S.UM_DIAG_SET_CRT_DTM AND
  T.UM_DIAG_SET_SEQ_NO = S.UM_DIAG_SET_SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    T.DIAG_CD = S.DIAG_CD,
    T.DIAG_CD_DESC = S.DIAG_CD_DESC,
    T.UM_DIAG_SET_ONSET_DT = S.UM_DIAG_SET_ONSET_DT,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    UM_REF_ID,
    UM_DIAG_SET_CRT_DTM,
    UM_DIAG_SET_SEQ_NO,
    DIAG_CD,
    DIAG_CD_DESC,
    UM_DIAG_SET_ONSET_DT,
    LAST_UPDT_RUN_CYC_NO
  )
  VALUES (
    S.SRC_SYS_CD,
    S.UM_REF_ID,
    S.UM_DIAG_SET_CRT_DTM,
    S.UM_DIAG_SET_SEQ_NO,
    S.DIAG_CD,
    S.DIAG_CD_DESC,
    S.UM_DIAG_SET_ONSET_DT,
    S.LAST_UPDT_RUN_CYC_NO
  );
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)
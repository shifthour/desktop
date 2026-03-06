# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : F2FMbrSrvyRspnExtrSeq, NDBHMbrSrvyRspnExtrSeq,HRAMbrSrvyRspnExtrSeq
# MAGIC             
# MAGIC 
# MAGIC PROCESSING:  Job run by mulitiple warehouse processes.  Input files created in specific vendor file validation jobs.
# MAGIC                           Input data is loaded to processing table where surrogate keys are assigned.
# MAGIC                           All rows are extracted and loaded to database tables by calling sequencer.
# MAGIC                           Rows with foreign key errors are left in table to process during next run.  Valid rows are deleted.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Brent Leland\(9)2011-03-24\(9)4673 AYH\(9)Original Programming                                               IntegrateWrhsDevl                  SAndrew               2011-04-20
# MAGIC Kalyan Neelam        2011-05-04              4673                        Changed this job to a multiple instance.                   IntegrateWrhsDevl                  SAnderw               2011-05-10
# MAGIC                                                                                                 Took out the IDS load files and added direct insert 
# MAGIC                                                                                                 update to the IDS & EDW FILE_ERR_DTL and FILE_METRIC tables
# MAGIC                                                                                                  Removed hashed file lookup for File Metric FKey, added database reference lookup    SAnderw       2011-05-12
# MAGIC 
# MAGIC Bhoomi Dasari         2011-11-04               4673                         Added 'newlogic "TransformerIDS"                      IntegrateWrhsDevl                   SAndrew                2011-11-29
# MAGIC                                                                                                  to give time to load EDW table 
# MAGIC 
# MAGIC Manasa Andru        2014-07-25               TFS - 9328               Added 'SourceDomainName' parameter and         IntegrateNewDevl                   Kalyan Neelam         2014-07-30
# MAGIC                                                                                                passed it to the SQL in the Err_Cat_SK step.  
# MAGIC Abhiram Dasarathy\(9)2016-04-23\(9)5414 MEP\(9)Removed VALID_ROW_IN field from the Table\(9)IntegrateDev2                        Kalyan Neelam          2016-04-26
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)structure of FILE_ERR_DTL and  
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)FILE_ERR_DTL_D (moving the job separately as part of a new changeset number)

# MAGIC Insert / Update Rows
# MAGIC Invalid rows will process again with next run of this job
# MAGIC File Error and Metric Primary and Foreign Key
# MAGIC Load EDW FILE_METRIC_D
# MAGIC Insert / Update Rows
# MAGIC Assign primary key for know rows
# MAGIC Assign primary key for know rows
# MAGIC Assign primary key for new rows
# MAGIC Assign primary key for new rows
# MAGIC Assign File Cat CD SK
# MAGIC Assign File Cat CD SK
# MAGIC Assign file metric SK for known exiting values
# MAGIC Assign file metric SK for new value
# MAGIC Load IDS FILE_ERR_DTL
# MAGIC Current File Metric SK
# MAGIC Mark rows with unknown SK values
# MAGIC Mark rows with unknown SK values
# MAGIC Delete rows with valid SK
# MAGIC Delete rows with valid SK
# MAGIC Sequential File Format settings: 
# MAGIC Quote Character 000
# MAGIC Delimiter ;
# MAGIC Load EDW FILE_ERR_DTL_D
# MAGIC Load IDS FILE_METRIC
# MAGIC Sequential File Format settings: 
# MAGIC Quote Character 000
# MAGIC Delimiter ;
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrDate = get_widget_value('CurrDate','')
CurrentTS = get_widget_value('CurrentTS','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ErrorFile = get_widget_value('ErrorFile','')
MetricFile = get_widget_value('MetricFile','')
SourceDomainName = get_widget_value('SourceDomainName','')

schema_MetricFile = StructType([
    StructField("SRC_SYS_CD_SK", StringType(), False),
    StructField("FILE_CAT_CD", StringType(), False),
    StructField("PRCS_DTM", StringType(), False),
    StructField("FILE_METRIC_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("FILE_CAT_CD_SK", IntegerType(), False),
    StructField("NON_RTRN_CT", IntegerType(), False),
    StructField("RTRN_CT", IntegerType(), False),
    StructField("TOT_RCRD_CT", IntegerType(), False),
    StructField("VALID_ROW_IN", StringType(), False)
])

df_MetricFile = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_MetricFile)
    .csv(f"{adls_path}/verified/{MetricFile}")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsFileMetricLoad_P_FILE_METRIC_temp", jdbc_url_ids, jdbc_props_ids)
df_MetricFile.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.IdsFileMetricLoad_P_FILE_METRIC_temp") \
    .mode("overwrite") \
    .save()

merge_sql_P_FILE_METRIC = f"""
MERGE INTO {IDSOwner}.P_FILE_METRIC AS T
USING STAGING.IdsFileMetricLoad_P_FILE_METRIC_temp AS S
ON 
    T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
    AND T.FILE_CAT_CD = S.FILE_CAT_CD
    AND T.PRCS_DTM = S.PRCS_DTM
WHEN MATCHED THEN
    UPDATE SET
        T.FILE_METRIC_SK = S.FILE_METRIC_SK,
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
        T.FILE_CAT_CD_SK = S.FILE_CAT_CD_SK,
        T.NON_RTRN_CT = S.NON_RTRN_CT,
        T.RTRN_CT = S.RTRN_CT,
        T.TOT_RCRD_CT = S.TOT_RCRD_CT,
        T.VALID_ROW_IN = S.VALID_ROW_IN
WHEN NOT MATCHED THEN
    INSERT
    (
     SRC_SYS_CD_SK,
     FILE_CAT_CD,
     PRCS_DTM,
     FILE_METRIC_SK,
     CRT_RUN_CYC_EXCTN_SK,
     LAST_UPDT_RUN_CYC_EXCTN_SK,
     FILE_CAT_CD_SK,
     NON_RTRN_CT,
     RTRN_CT,
     TOT_RCRD_CT,
     VALID_ROW_IN
    )
    VALUES
    (
     S.SRC_SYS_CD_SK,
     S.FILE_CAT_CD,
     S.PRCS_DTM,
     S.FILE_METRIC_SK,
     S.CRT_RUN_CYC_EXCTN_SK,
     S.LAST_UPDT_RUN_CYC_EXCTN_SK,
     S.FILE_CAT_CD_SK,
     S.NON_RTRN_CT,
     S.RTRN_CT,
     S.TOT_RCRD_CT,
     S.VALID_ROW_IN
    );
"""
execute_dml(merge_sql_P_FILE_METRIC, jdbc_url_ids, jdbc_props_ids)

df_Mupd1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

update_sql_Met_Prm_SK = f"""
UPDATE {IDSOwner}.P_FILE_METRIC b
   SET (b.FILE_METRIC_SK, b.CRT_RUN_CYC_EXCTN_SK, b.LAST_UPDT_RUN_CYC_EXCTN_SK) =
       (SELECT e.FILE_METRIC_SK, e.CRT_RUN_CYC_EXCTN_SK, {CurrRunCycle}
          FROM  {IDSOwner}.FILE_METRIC e
         WHERE  e.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
               AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
               AND  e.FILE_CAT_CD = b.FILE_CAT_CD
               AND e.PRCS_DTM = b.PRCS_DTM)
WHERE EXISTS
      (SELECT e.FILE_METRIC_SK
         FROM  {IDSOwner}.FILE_METRIC e
        WHERE  e.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
              AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
              AND  e.FILE_CAT_CD = b.FILE_CAT_CD
               AND e.PRCS_DTM = b.PRCS_DTM)
"""
execute_dml(update_sql_Met_Prm_SK, jdbc_url_ids, jdbc_props_ids)

df_Mupd2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT
    SRC_SYS_CD_SK as SRC_SYS_CD_SK,
    FILE_CAT_CD as FILE_CAT_CD,
    PRCS_DTM as PRCS_DTM,
    CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
    FILE_METRIC_SK as FILE_METRIC_SK
FROM {IDSOwner}.P_FILE_METRIC
WHERE FILE_METRIC_SK = 0
AND SRC_SYS_CD_SK = {SrcSysCdSk}
""")
    .load()
)

df_Met_New_SK = df_Mupd2.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("FILE_CAT_CD").alias("FILE_CAT_CD"),
    F.col("PRCS_DTM").alias("PRCS_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.expr("KeyMgtGetNextValueConcurrent((\"FILE_METRIC_SK\"))").alias("FILE_METRIC_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsFileMetricLoad_Met_New_SK_temp", jdbc_url_ids, jdbc_props_ids)
df_Met_New_SK.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.IdsFileMetricLoad_Met_New_SK_temp") \
    .mode("overwrite") \
    .save()

update_sql_Met_Upd_SK = f"""
MERGE INTO {IDSOwner}.P_FILE_METRIC AS T
USING STAGING.IdsFileMetricLoad_Met_New_SK_temp AS S
ON 
   T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.FILE_CAT_CD = S.FILE_CAT_CD
   AND T.PRCS_DTM = S.PRCS_DTM
WHEN MATCHED THEN
   UPDATE SET
       T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
       T.FILE_METRIC_SK = S.FILE_METRIC_SK;
"""
execute_dml(update_sql_Met_Upd_SK, jdbc_url_ids, jdbc_props_ids)

df_Mupd3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

update_sql_Met_Cat_SK = f"""
UPDATE {IDSOwner}.P_FILE_METRIC b
   SET FILE_CAT_CD_SK =
       (SELECT c.CD_MPPNG_SK
          FROM  {IDSOwner}.CD_MPPNG c
         WHERE  c.TRGT_CD = b.FILE_CAT_CD
           AND  c.SRC_DOMAIN_NM = 'FILE CATEGORY TYPE'
           AND  c.SRC_SYS_CD = 'BCBSKCCOMMON')
WHERE b.FILE_CAT_CD in
      (SELECT d.TRGT_CD
         FROM {IDSOwner}.CD_MPPNG d
        WHERE d.SRC_DOMAIN_NM = 'FILE CATEGORY TYPE')
  AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
"""
execute_dml(update_sql_Met_Cat_SK, jdbc_url_ids, jdbc_props_ids)

df_FinalMetric = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
 FILE_METRIC_SK as FILE_METRIC_SK,
 SRC_SYS_CD_SK as SRC_SYS_CD_SK,
 FILE_CAT_CD as FILE_CAT_CD,
 PRCS_DTM as PRCS_DTM,
 CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
 LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
 FILE_CAT_CD_SK as FILE_CAT_CD_SK,
 NON_RTRN_CT as NON_RTRN_CT,
 RTRN_CT as RTRN_CT,
 TOT_RCRD_CT as TOT_RCRD_CT
FROM {IDSOwner}.P_FILE_METRIC
WHERE SRC_SYS_CD_SK = {SrcSysCdSk}
""")
    .load()
)

df_EDW = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
 METRIC.FILE_METRIC_SK,
 METRIC.SRC_SYS_CD_SK,
 METRIC.FILE_CAT_CD,
 METRIC.PRCS_DTM,
 METRIC.CRT_RUN_CYC_EXCTN_SK,
 METRIC.LAST_UPDT_RUN_CYC_EXCTN_SK,
 METRIC.FILE_CAT_CD_SK,
 METRIC.NON_RTRN_CT,
 METRIC.RTRN_CT,
 METRIC.TOT_RCRD_CT,
 CD.TRGT_CD_NM
FROM {IDSOwner}.P_FILE_METRIC METRIC
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD
  ON METRIC.FILE_CAT_CD_SK = CD.CD_MPPNG_SK
WHERE METRIC.SRC_SYS_CD_SK = {SrcSysCdSk}
""")
    .load()
)

df_Metric_Lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
 SRC_SYS_CD_SK,
 FILE_CAT_CD,
 PRCS_DTM,
 FILE_METRIC_SK
FROM {IDSOwner}.P_FILE_METRIC
WHERE SRC_SYS_CD_SK=? 
AND FILE_CAT_CD=? 
AND PRCS_DTM=? 
""")
    .load()
)

df_Metric_Lkup1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
 SRC_SYS_CD_SK,
 FILE_CAT_CD,
 PRCS_DTM,
 FILE_METRIC_SK
FROM {IDSOwner}.P_FILE_METRIC
WHERE SRC_SYS_CD_SK=? 
AND FILE_CAT_CD=? 
AND PRCS_DTM=? 
""")
    .load()
)

df_EDWTransformer = df_EDW.select(
    F.col("FILE_METRIC_SK").alias("FILE_METRIC_SK"),
    F.lit(SrcSysCd).alias("SrcSysCd"),
    F.col("FILE_CAT_CD").alias("FILE_CAT_CD"),
    F.col("PRCS_DTM").alias("PRCS_DTM"),
    F.lit(CurrDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.expr("CASE WHEN EDW.TRGT_CD_NM IS NULL THEN 'UNK' ELSE EDW.TRGT_CD_NM END").alias("FILE_CAT_NM"),
    F.expr("FORMAT.DATE(PRCS_DTM, 'DATE', 'DATE', 'CCYYMM')").alias("PRCS_YR_MO"),
    F.expr("CASE WHEN (EDW.NON_RTRN_CT IS NULL OR LENGTH(trim(EDW.NON_RTRN_CT))=0) THEN 0 ELSE EDW.NON_RTRN_CT END").alias("NON_RTRN_CT"),
    F.expr("CASE WHEN (EDW.RTRN_CT IS NULL OR LENGTH(trim(EDW.RTRN_CT))=0) THEN 0 ELSE EDW.RTRN_CT END").alias("RTRN_CT"),
    F.expr("CASE WHEN (EDW.TOT_RCRD_CT IS NULL OR LENGTH(trim(EDW.TOT_RCRD_CT))=0) THEN 0 ELSE EDW.TOT_RCRD_CT END").alias("TOT_RCRD_CT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FILE_CAT_CD_SK").alias("FILE_CAT_CD_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsFileMetricLoad_LoadEDW_FILE_METRIC_D_temp", get_db_config(edw_secret_name)[0], get_db_config(edw_secret_name)[1])
df_EDWTransformer.write.format("jdbc") \
    .option("url", get_db_config(edw_secret_name)[0]) \
    .options(**get_db_config(edw_secret_name)[1]) \
    .option("dbtable", "STAGING.IdsFileMetricLoad_LoadEDW_FILE_METRIC_D_temp") \
    .mode("overwrite") \
    .save()

merge_sql_LoadEDW_FILE_METRIC_D = f"""
MERGE INTO {EDWOwner}.FILE_METRIC_D AS T
USING STAGING.IdsFileMetricLoad_LoadEDW_FILE_METRIC_D_temp AS S
ON
   T.FILE_METRIC_SK = S.FILE_METRIC_SK
WHEN MATCHED THEN
   UPDATE SET
       T.SRC_SYS_CD = S.SrcSysCd,
       T.FILE_CAT_CD = S.FILE_CAT_CD,
       T.PRCS_DTM = S.PRCS_DTM,
       T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
       T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
       T.FILE_CAT_NM = S.FILE_CAT_NM,
       T.PRCS_YR_MO = S.PRCS_YR_MO,
       T.NON_RTRN_CT = S.NON_RTRN_CT,
       T.RTRN_CT = S.RTRN_CT,
       T.TOT_RCRD_CT = S.TOT_RCRD_CT,
       T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
       T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
       T.FILE_CAT_CD_SK = S.FILE_CAT_CD_SK
WHEN NOT MATCHED THEN
   INSERT
   (
    FILE_METRIC_SK,
    SRC_SYS_CD,
    FILE_CAT_CD,
    PRCS_DTM,
    CRT_RUN_CYC_EXCTN_DT_SK,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    FILE_CAT_NM,
    PRCS_YR_MO,
    NON_RTRN_CT,
    RTRN_CT,
    TOT_RCRD_CT,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    FILE_CAT_CD_SK
   )
   VALUES
   (
    S.FILE_METRIC_SK,
    S.SrcSysCd,
    S.FILE_CAT_CD,
    S.PRCS_DTM,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    S.FILE_CAT_NM,
    S.PRCS_YR_MO,
    S.NON_RTRN_CT,
    S.RTRN_CT,
    S.TOT_RCRD_CT,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.FILE_CAT_CD_SK
   );
"""
execute_dml(merge_sql_LoadEDW_FILE_METRIC_D, get_db_config(edw_secret_name)[0], get_db_config(edw_secret_name)[1])

############################################################################
# The hashed file stage "hf_medmgt_filemetricload" is Scenario A (intermediate).
# We replace it with a dedup on key column FILE_METRIC_SK and feed it as lookup.
############################################################################

df_EDWTransformer_lkup = df_EDWTransformer.select(
    F.col("FILE_METRIC_SK")
)

df_hf_medmgt_filemetricload = dedup_sort(
    df_EDWTransformer_lkup,
    ["FILE_METRIC_SK"],
    [("FILE_METRIC_SK", "A")]
)

df_TransformerIDS_FinalMetric = df_FinalMetric

df_TransformerIDS_lookup = df_hf_medmgt_filemetricload

df_TransformerIDS_joined = df_TransformerIDS_FinalMetric.alias("FinalMetric").join(
    df_TransformerIDS_lookup.alias("next"),
    on=[F.col("FinalMetric.FILE_METRIC_SK") == F.col("next.FILE_METRIC_SK")],
    how="left"
)

df_TransformerIDS = df_TransformerIDS_joined

df_TransformerIDS_LoadIDS = df_TransformerIDS.select(
    F.col("FinalMetric.FILE_METRIC_SK").alias("FILE_METRIC_SK"),
    F.col("FinalMetric.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("FinalMetric.FILE_CAT_CD").alias("FILE_CAT_CD"),
    F.col("FinalMetric.PRCS_DTM").alias("PRCS_DTM"),
    F.col("FinalMetric.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("FinalMetric.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FinalMetric.FILE_CAT_CD_SK").alias("FILE_CAT_CD_SK"),
    F.col("FinalMetric.NON_RTRN_CT").alias("NON_RTRN_CT"),
    F.col("FinalMetric.RTRN_CT").alias("RTRN_CT"),
    F.col("FinalMetric.TOT_RCRD_CT").alias("TOT_RCRD_CT")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsFileMetricLoad_LoadIDS_FILE_METRIC_temp", jdbc_url_ids, jdbc_props_ids)
df_TransformerIDS_LoadIDS.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.IdsFileMetricLoad_LoadIDS_FILE_METRIC_temp") \
    .mode("overwrite") \
    .save()

merge_sql_LoadIDS_FILE_METRIC = f"""
MERGE INTO {IDSOwner}.FILE_METRIC AS T
USING STAGING.IdsFileMetricLoad_LoadIDS_FILE_METRIC_temp AS S
ON 
    T.FILE_METRIC_SK = S.FILE_METRIC_SK
WHEN MATCHED THEN
    UPDATE SET
        T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK,
        T.FILE_CAT_CD = S.FILE_CAT_CD,
        T.PRCS_DTM = S.PRCS_DTM,
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
        T.FILE_CAT_CD_SK = S.FILE_CAT_CD_SK,
        T.NON_RTRN_CT = S.NON_RTRN_CT,
        T.RTRN_CT = S.RTRN_CT,
        T.TOT_RCRD_CT = S.TOT_RCRD_CT
WHEN NOT MATCHED THEN
    INSERT
    (
      FILE_METRIC_SK,
      SRC_SYS_CD_SK,
      FILE_CAT_CD,
      PRCS_DTM,
      CRT_RUN_CYC_EXCTN_SK,
      LAST_UPDT_RUN_CYC_EXCTN_SK,
      FILE_CAT_CD_SK,
      NON_RTRN_CT,
      RTRN_CT,
      TOT_RCRD_CT
    )
    VALUES
    (
      S.FILE_METRIC_SK,
      S.SRC_SYS_CD_SK,
      S.FILE_CAT_CD,
      S.PRCS_DTM,
      S.CRT_RUN_CYC_EXCTN_SK,
      S.LAST_UPDT_RUN_CYC_EXCTN_SK,
      S.FILE_CAT_CD_SK,
      S.NON_RTRN_CT,
      S.RTRN_CT,
      S.TOT_RCRD_CT
    );
"""
execute_dml(merge_sql_LoadIDS_FILE_METRIC, jdbc_url_ids, jdbc_props_ids)

df_Mupd5 = df_TransformerIDS.filter(F.lit(True)).limit(0)  # Simulate an empty read, actual constraint @INROWNUM=1 triggers once
update_sql_Met_Upd_Val_in = f"""
UPDATE {IDSOwner}.P_FILE_METRIC b
   SET b.VALID_ROW_IN = 'N'
WHERE b.FILE_CAT_CD_SK = 0
  AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
"""
execute_dml(update_sql_Met_Upd_Val_in, jdbc_url_ids, jdbc_props_ids)

df_Mdel = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

delete_sql_Met_Del_Rec = f"""
DELETE FROM {IDSOwner}.P_FILE_METRIC b
WHERE b.VALID_ROW_IN = 'Y'
  AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
"""
execute_dml(delete_sql_Met_Del_Rec, jdbc_url_ids, jdbc_props_ids)

schema_ErrorFile = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("FILE_CAT_CD", StringType(), False),
    StructField("PRCS_DTM", StringType(), False),
    StructField("ROW_SEQ_NO", IntegerType(), False),
    StructField("FILE_ERR_DTL_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("FILE_METRIC_SK", IntegerType(), False),
    StructField("FILE_CAT_CD_SK", IntegerType(), False),
    StructField("BAD_RCRD_STRCT_IN", StringType(), False),
    StructField("DO_NOT_RTRN_IN", StringType(), False),
    StructField("INPT_RCRD_TX", StringType(), False),
    StructField("VALID_ROW_IN", StringType(), True),
    StructField("ERR_ID", StringType(), True),
    StructField("ERR_DESC", StringType(), True)
])

df_ErrorFile = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "000")
    .option("header", "false")
    .schema(schema_ErrorFile)
    .csv(f"{adls_path}/verified/{ErrorFile}")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsFileMetricLoad_P_FILE_ERROR_DTL_temp", jdbc_url_ids, jdbc_props_ids)
df_ErrorFile.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.IdsFileMetricLoad_P_FILE_ERROR_DTL_temp") \
    .mode("overwrite") \
    .save()

merge_sql_P_FILE_ERROR_DTL = f"""
MERGE INTO {IDSOwner}.P_FILE_ERR_DTL AS T
USING STAGING.IdsFileMetricLoad_P_FILE_ERROR_DTL_temp AS S
ON
   T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.FILE_CAT_CD = S.FILE_CAT_CD
   AND T.PRCS_DTM = S.PRCS_DTM
   AND T.ROW_SEQ_NO = S.ROW_SEQ_NO
WHEN MATCHED THEN
   UPDATE SET
       T.FILE_ERR_DTL_SK = S.FILE_ERR_DTL_SK,
       T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
       T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
       T.FILE_METRIC_SK = S.FILE_METRIC_SK,
       T.FILE_CAT_CD_SK = S.FILE_CAT_CD_SK,
       T.BAD_RCRD_STRCT_IN = S.BAD_RCRD_STRCT_IN,
       T.DO_NOT_RTRN_IN = S.DO_NOT_RTRN_IN,
       T.INPT_RCRD_TX = S.INPT_RCRD_TX,
       T.VALID_ROW_IN = S.VALID_ROW_IN,
       T.ERR_ID = S.ERR_ID,
       T.ERR_DESC = S.ERR_DESC
WHEN NOT MATCHED THEN
   INSERT
   (
    SRC_SYS_CD_SK,
    FILE_CAT_CD,
    PRCS_DTM,
    ROW_SEQ_NO,
    FILE_ERR_DTL_SK,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    FILE_METRIC_SK,
    FILE_CAT_CD_SK,
    BAD_RCRD_STRCT_IN,
    DO_NOT_RTRN_IN,
    INPT_RCRD_TX,
    VALID_ROW_IN,
    ERR_ID,
    ERR_DESC
   )
   VALUES
   (
    S.SRC_SYS_CD_SK,
    S.FILE_CAT_CD,
    S.PRCS_DTM,
    S.ROW_SEQ_NO,
    S.FILE_ERR_DTL_SK,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.FILE_METRIC_SK,
    S.FILE_CAT_CD_SK,
    S.BAD_RCRD_STRCT_IN,
    S.DO_NOT_RTRN_IN,
    S.INPT_RCRD_TX,
    S.VALID_ROW_IN,
    S.ERR_ID,
    S.ERR_DESC
   );
"""
execute_dml(merge_sql_P_FILE_ERROR_DTL, jdbc_url_ids, jdbc_props_ids)

df_Eupd1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

update_sql_Err_Prm_SK = f"""
UPDATE {IDSOwner}.P_FILE_ERR_DTL b
   SET (b.FILE_ERR_DTL_SK, b.CRT_RUN_CYC_EXCTN_SK, b.LAST_UPDT_RUN_CYC_EXCTN_SK) =
       (SELECT e.FILE_ERR_DTL_SK, e.CRT_RUN_CYC_EXCTN_SK, {CurrRunCycle}
          FROM {IDSOwner}.FILE_ERR_DTL e
         WHERE e.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
           AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
           AND e.FILE_CAT_CD = b.FILE_CAT_CD
           AND e.PRCS_DTM = b.PRCS_DTM
           AND e.ROW_SEQ_NO = b.ROW_SEQ_NO)
WHERE EXISTS
      (SELECT e.FILE_ERR_DTL_SK
         FROM {IDSOwner}.FILE_ERR_DTL e
        WHERE e.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
          AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
          AND e.FILE_CAT_CD = b.FILE_CAT_CD
          AND e.PRCS_DTM = b.PRCS_DTM
          AND e.ROW_SEQ_NO = b.ROW_SEQ_NO)
"""
execute_dml(update_sql_Err_Prm_SK, jdbc_url_ids, jdbc_props_ids)

df_Eupd2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT 
   SRC_SYS_CD_SK as SRC_SYS_CD_SK,
   FILE_CAT_CD as FILE_CAT_CD,
   PRCS_DTM as PRCS_DTM,
   ROW_SEQ_NO as ROW_SEQ_NO,
   CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
   FILE_ERR_DTL_SK as FILE_ERR_DTL_SK
FROM {IDSOwner}.P_FILE_ERR_DTL
WHERE File_Err_Dtl_SK = 0
  AND SRC_SYS_CD_SK = {SrcSysCdSk}
""")
    .load()
)

df_Err_New_SK = df_Eupd2.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("FILE_CAT_CD").alias("FILE_CAT_CD"),
    F.col("PRCS_DTM").alias("PRCS_DTM"),
    F.col("ROW_SEQ_NO").alias("ROW_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.expr("KeyMgtGetNextValueConcurrent((\"FILE_ERR_SK\"))").alias("FILE_ERR_DTL_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsFileMetricLoad_Err_New_SK_temp", jdbc_url_ids, jdbc_props_ids)
df_Err_New_SK.write.format("jdbc") \
    .option("url", jdbc_url_ids) \
    .options(**jdbc_props_ids) \
    .option("dbtable", "STAGING.IdsFileMetricLoad_Err_New_SK_temp") \
    .mode("overwrite") \
    .save()

update_sql_Err_Upd_SK = f"""
MERGE INTO {IDSOwner}.P_FILE_ERR_DTL AS T
USING STAGING.IdsFileMetricLoad_Err_New_SK_temp AS S
ON
   T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.FILE_CAT_CD = S.FILE_CAT_CD
   AND T.PRCS_DTM = S.PRCS_DTM
   AND T.ROW_SEQ_NO = S.ROW_SEQ_NO
WHEN MATCHED THEN
   UPDATE SET
       T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
       T.FILE_ERR_DTL_SK = S.FILE_ERR_DTL_SK;
"""
execute_dml(update_sql_Err_Upd_SK, jdbc_url_ids, jdbc_props_ids)

df_Eupd3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

update_sql_Err_Cat_SK = f"""
UPDATE {IDSOwner}.P_FILE_ERR_DTL b
   SET FILE_CAT_CD_SK =
       (SELECT c.CD_MPPNG_SK
          FROM {IDSOwner}.CD_MPPNG c
         WHERE c.TRGT_CD = b.FILE_CAT_CD
           AND c.SRC_DOMAIN_NM = '{SourceDomainName}'
           AND c.SRC_SYS_CD = 'BCBSKCCOMMON')
WHERE b.FILE_CAT_CD in
      (SELECT d.TRGT_CD
         FROM {IDSOwner}.CD_MPPNG d
        WHERE d.SRC_DOMAIN_NM = '{SourceDomainName}')
  AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
"""
execute_dml(update_sql_Err_Cat_SK, jdbc_url_ids, jdbc_props_ids)

df_Eupd5 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

update_sql_Err_Met_Upd_SK = f"""
UPDATE {IDSOwner}.P_FILE_ERR_DTL b
   SET (b.FILE_METRIC_SK) =
       (SELECT e.FILE_METRIC_SK
          FROM {IDSOwner}.FILE_METRIC e
         WHERE e.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
           AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
           AND e.FILE_CAT_CD = b.FILE_CAT_CD
           AND e.PRCS_DTM = b.PRCS_DTM)
WHERE EXISTS
      (SELECT e.FILE_METRIC_SK
         FROM {IDSOwner}.FILE_METRIC e
        WHERE e.SRC_SYS_CD_SK = b.SRC_SYS_CD_SK
          AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
          AND e.FILE_CAT_CD = b.FILE_CAT_CD
          AND e.PRCS_DTM = b.PRCS_DTM)
"""
execute_dml(update_sql_Err_Met_Upd_SK, jdbc_url_ids, jdbc_props_ids)

df_Met_Lkup_SK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT
 FILE_ERR_DTL_SK as FILE_ERR_DTL_SK,
 SRC_SYS_CD_SK as SRC_SYS_CD_SK,
 FILE_CAT_CD as FILE_CAT_CD,
 PRCS_DTM as PRCS_DTM,
 ROW_SEQ_NO as ROW_SEQ_NO,
 CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
 LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
 FILE_METRIC_SK as FILE_METRIC_SK,
 BAD_RCRD_STRCT_IN as BAD_RCRD_STRCT_IN,
 DO_NOT_RTRN_IN as DO_NOT_RTRN_IN,
 INPT_RCRD_TX as INPT_RCRD_TX,
 FILE_CAT_CD_SK as FILE_CAT_CD_SK,
 ERR_ID,
 ERR_DESC
FROM {IDSOwner}.P_FILE_ERR_DTL
WHERE SRC_SYS_CD_SK = {SrcSysCdSk}
""")
    .load()
)

df_EDW_Err = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"""
SELECT
 ERR.FILE_ERR_DTL_SK,
 ERR.SRC_SYS_CD_SK,
 ERR.FILE_CAT_CD,
 ERR.PRCS_DTM,
 ERR.ROW_SEQ_NO,
 ERR.CRT_RUN_CYC_EXCTN_SK,
 ERR.LAST_UPDT_RUN_CYC_EXCTN_SK,
 ERR.FILE_METRIC_SK,
 ERR.BAD_RCRD_STRCT_IN,
 ERR.DO_NOT_RTRN_IN,
 ERR.INPT_RCRD_TX,
 ERR.FILE_CAT_CD_SK,
 ERR.ERR_ID,
 ERR.ERR_DESC,
 CD.TRGT_CD_NM
FROM {IDSOwner}.P_FILE_ERR_DTL ERR
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD
  ON ERR.FILE_CAT_CD_SK = CD.CD_MPPNG_SK
WHERE ERR.SRC_SYS_CD_SK = {SrcSysCdSk}
""")
    .load()
)

df_Met_Lkup_SK_primary = df_Met_Lkup_SK.alias("Eupd6")
df_Metric_Lkup_alias = df_Metric_Lkup.alias("Metric_Lkup")

df_Met_Lkup_SK_joined = df_Met_Lkup_SK_primary.join(
    df_Metric_Lkup_alias,
    (F.col("Eupd6.SRC_SYS_CD_SK") == F.col("Metric_Lkup.SRC_SYS_CD_SK")) &
    (F.col("Eupd6.FILE_CAT_CD") == F.col("Metric_Lkup.FILE_CAT_CD")) &
    (F.col("Eupd6.PRCS_DTM") == F.col("Metric_Lkup.PRCS_DTM")),
    how="left"
)

df_Met_Lkup_SK_out = df_Met_Lkup_SK_joined.select(
    F.col("Eupd6.FILE_ERR_DTL_SK").alias("FILE_ERR_DTL_SK"),
    F.col("Eupd6.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Eupd6.FILE_CAT_CD").alias("FILE_CAT_CD"),
    F.col("Eupd6.PRCS_DTM").alias("PRCS_DTM"),
    F.col("Eupd6.ROW_SEQ_NO").alias("ROW_SEQ_NO"),
    F.col("Eupd6.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Eupd6.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("""
CASE WHEN Eupd6.FILE_METRIC_SK = 0 THEN
     CASE WHEN Metric_Lkup.FILE_METRIC_SK IS NULL THEN 0
          ELSE Metric_Lkup.FILE_METRIC_SK END
     ELSE Eupd6.FILE_METRIC_SK END
""").alias("FILE_METRIC_SK"),
    F.col("Eupd6.FILE_CAT_CD_SK").alias("FILE_CAT_CD_SK"),
    F.col("Eupd6.BAD_RCRD_STRCT_IN").alias("BAD_RCRD_STRCT_IN"),
    F.col("Eupd6.DO_NOT_RTRN_IN").alias("DO_NOT_RTRN_IN"),
    F.col("Eupd6.INPT_RCRD_TX").alias("INPT_RCRD_TX"),
    F.col("Eupd6.ERR_ID").alias("ERR_ID"),
    F.col("Eupd6.ERR_DESC").alias("ERR_DESC")
)

df_Eupd7 = df_Met_Lkup_SK_out.filter(F.lit(True)).limit(0)

update_sql_Err_Upd_Val_In = f"""
UPDATE {IDSOwner}.P_FILE_ERR_DTL b
   SET b.VALID_ROW_IN = 'N'
WHERE (b.FILE_CAT_CD_SK = 0
       OR b.FILE_METRIC_SK = 0)
  AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
"""
execute_dml(update_sql_Err_Upd_Val_In, jdbc_url_ids, jdbc_props_ids)

df_Edel = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT IBMREQD FROM sysibm.sysdummy1")
    .load()
)

delete_sql_Err_Del_Rec = f"""
DELETE FROM {IDSOwner}.P_FILE_ERR_DTL b
WHERE b.VALID_ROW_IN = 'Y'
  AND b.SRC_SYS_CD_SK = {SrcSysCdSk}
"""
execute_dml(delete_sql_Err_Del_Rec, jdbc_url_ids, jdbc_props_ids)

df_EDW_Err_primary = df_EDW_Err.alias("EDW")
df_Metric_Lkup1_alias = df_Metric_Lkup1.alias("Metric_Lkup1")

df_EDW_CdLkup_joined = df_EDW_Err_primary.join(
    df_Metric_Lkup1_alias,
    (F.col("EDW.SRC_SYS_CD_SK") == F.col("Metric_Lkup1.SRC_SYS_CD_SK")) &
    (F.col("EDW.FILE_CAT_CD") == F.col("Metric_Lkup1.FILE_CAT_CD")) &
    (F.col("EDW.PRCS_DTM") == F.col("Metric_Lkup1.PRCS_DTM")),
    how="left"
)

df_EDW_CdLkup = df_EDW_CdLkup_joined.select(
    F.col("EDW.FILE_ERR_DTL_SK").alias("FILE_ERR_DTL_SK"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.col("EDW.FILE_CAT_CD").alias("FILE_CAT_CD"),
    F.col("EDW.PRCS_DTM").alias("PRCS_DTM"),
    F.col("EDW.ROW_SEQ_NO").alias("ROW_SEQ_NO"),
    F.lit(CurrDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.expr("""
CASE WHEN EDW.FILE_METRIC_SK = 0 THEN
     CASE WHEN Metric_Lkup1.FILE_METRIC_SK IS NULL THEN 0
          ELSE Metric_Lkup1.FILE_METRIC_SK END
     ELSE EDW.FILE_METRIC_SK END
""").alias("FILE_METRIC_SK"),
    F.expr("CASE WHEN EDW.TRGT_CD_NM IS NULL THEN 'UNK' ELSE EDW.TRGT_CD_NM END").alias("FILE_CAT_NM"),
    F.expr("""
CASE WHEN (EDW.BAD_RCRD_STRCT_IN IS NULL OR LENGTH(TRIM(EDW.BAD_RCRD_STRCT_IN))=0)
     THEN 'N' ELSE EDW.BAD_RCRD_STRCT_IN END
""").alias("BAD_RCRD_STRCT_IN"),
    F.expr("""
CASE WHEN (EDW.DO_NOT_RTRN_IN IS NULL OR LENGTH(TRIM(EDW.DO_NOT_RTRN_IN))=0)
     THEN 'N' ELSE EDW.DO_NOT_RTRN_IN END
""").alias("DO_NOT_RTRN_IN"),
    F.expr("FORMAT.DATE(EDW.PRCS_DTM, 'DATE', 'DATE', 'CCYYMM')").alias("PRCS_YR_MO"),
    F.expr("""
CASE WHEN (EDW.INPT_RCRD_TX IS NULL OR LENGTH(TRIM(EDW.INPT_RCRD_TX))=0)
     THEN 'NA' ELSE EDW.INPT_RCRD_TX END
""").alias("INPT_RCRD_TX"),
    F.col("EDW.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("EDW.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("""
CASE WHEN (EDW.FILE_CAT_CD_SK IS NULL OR LENGTH(TRIM(EDW.FILE_CAT_CD_SK))=0)
     THEN 0 ELSE EDW.FILE_CAT_CD_SK END
""").alias("FILE_CAT_CD_SK"),
    F.expr("""
CASE WHEN (EDW.ERR_ID IS NULL OR LENGTH(TRIM(EDW.ERR_ID))=0)
     THEN 'NA' ELSE EDW.ERR_ID END
""").alias("ERR_ID"),
    F.expr("""
CASE WHEN (EDW.ERR_DESC IS NULL OR LENGTH(TRIM(EDW.ERR_DESC))=0)
     THEN 'NA' ELSE EDW.ERR_DESC END
""").alias("ERR_DESC")
)

execute_dml("DROP TABLE IF EXISTS STAGING.IdsFileMetricLoad_LoadEDW_FILE_ERR_DTL_D_temp", get_db_config(edw_secret_name)[0], get_db_config(edw_secret_name)[1])
df_EDW_CdLkup.write.format("jdbc") \
    .option("url", get_db_config(edw_secret_name)[0]) \
    .options(**get_db_config(edw_secret_name)[1]) \
    .option("dbtable", "STAGING.IdsFileMetricLoad_LoadEDW_FILE_ERR_DTL_D_temp") \
    .mode("overwrite") \
    .save()

merge_sql_LoadEDW_FILE_ERR_DTL_D = f"""
MERGE INTO {EDWOwner}.FILE_ERR_DTL_D AS T
USING STAGING.IdsFileMetricLoad_LoadEDW_FILE_ERR_DTL_D_temp AS S
ON 
   T.FILE_ERR_DTL_SK = S.FILE_ERR_DTL_SK
   AND T.FILE_METRIC_SK = S.FILE_METRIC_SK
WHEN MATCHED THEN
   UPDATE SET
       T.SRC_SYS_CD = S.SRC_SYS_CD,
       T.FILE_CAT_CD = S.FILE_CAT_CD,
       T.PRCS_DTM = S.PRCS_DTM,
       T.ROW_SEQ_NO = S.ROW_SEQ_NO,
       T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
       T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
       T.FILE_CAT_NM = S.FILE_CAT_NM,
       T.BAD_RCRD_STRCT_IN = S.BAD_RCRD_STRCT_IN,
       T.DO_NOT_RTRN_IN = S.DO_NOT_RTRN_IN,
       T.PRCS_YR_MO = S.PRCS_YR_MO,
       T.INPT_RCRD_TX = S.INPT_RCRD_TX,
       T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
       T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
       T.FILE_CAT_CD_SK = S.FILE_CAT_CD_SK,
       T.ERR_ID = S.ERR_ID,
       T.ERR_DESC = S.ERR_DESC
WHEN NOT MATCHED THEN
   INSERT
   (
    FILE_ERR_DTL_SK,
    SRC_SYS_CD,
    FILE_CAT_CD,
    PRCS_DTM,
    ROW_SEQ_NO,
    CRT_RUN_CYC_EXCTN_DT_SK,
    LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    FILE_METRIC_SK,
    FILE_CAT_NM,
    BAD_RCRD_STRCT_IN,
    DO_NOT_RTRN_IN,
    PRCS_YR_MO,
    INPT_RCRD_TX,
    CRT_RUN_CYC_EXCTN_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    FILE_CAT_CD_SK,
    ERR_ID,
    ERR_DESC
   )
   VALUES
   (
    S.FILE_ERR_DTL_SK,
    S.SRC_SYS_CD,
    S.FILE_CAT_CD,
    S.PRCS_DTM,
    S.ROW_SEQ_NO,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    S.FILE_METRIC_SK,
    S.FILE_CAT_NM,
    S.BAD_RCRD_STRCT_IN,
    S.DO_NOT_RTRN_IN,
    S.PRCS_YR_MO,
    S.INPT_RCRD_TX,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.FILE_CAT_CD_SK,
    S.ERR_ID,
    S.ERR_DESC
   );
"""
execute_dml(merge_sql_LoadEDW_FILE_ERR_DTL_D, get_db_config(edw_secret_name)[0], get_db_config(edw_secret_name)[1])
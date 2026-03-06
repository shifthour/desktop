# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/24/09 09:20:30 Batch  15090_33635 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 04/24/09 09:14:25 Batch  15090_33296 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC 
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:  Deletes from CLM_LN_REMIT and CLM_LN_REMIT_DSALW the ABF conversion records that are now out-of-date once the claim line remit data has been reprocessed on facets.  
# MAGIC                            If not deleted, then claim line remit amounts are overstated.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCBSClmLnRemitCntl / BCBSClmLnRemitCleanupSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Run foreign key and database loads for some of the IDS claim tables
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer            Date                 Project/Altiris #                            Change Description                                                                            Development Project      Code Reviewer         Date Reviewed         
# MAGIC ------------------         --------------------     ------------------------                            -----------------------------------------------------------------------                                      --------------------------------       -----------------------------    --------------------------       
# MAGIC  SAndrew            2008-09-22       #3057 Provider Web Resdegin   initial programming.                                                                              devlIDS

# MAGIC Delete claim records that are no longer in the source system.
# MAGIC Claim Line Remit delete process for obsolete records
# MAGIC Read IDS table for any claim lines that records on IDS CLM_LN_REMIT_DSALW that are for claim lines just processed but yet were not updated.  IE their last activity run cycle will not match for that this process is associated with.  This was specifically written to deleted old converted data
# MAGIC IDS CLM_LN_REMIT_DSALW joined with W_CLM_LN_REMIT_DRVR
# MAGIC IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','FACETS')
SourceSK = get_widget_value('SourceSK','35122')
RunCycle = get_widget_value('RunCycle','130')
IDSOwner = get_widget_value('IDSOwner','$PROJDEF')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_IDS_CLM_LN_REMIT_DISALLOW_recs = (
    f"SELECT c.CLM_LN_REMIT_DSALW_SK, c.CLM_LN_DSALW_TYP_CD_SK "
    f"FROM {IDSOwner}.CLM_LN_REMIT_DSALW c, {IDSOwner}.W_CLM_LN_REMIT_DRVR d "
    f"WHERE c.SRC_SYS_CD_SK = {SourceSK} "
    f"AND d.CLM_ID = c.CLM_ID "
    f"AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}"
)

df_IDS_CLM_LN_REMIT_DISALLOW_recs = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_IDS_CLM_LN_REMIT_DISALLOW_recs)
    .load()
)

query_IDS_CD_MPPNG = (
    f"SELECT CD_MPPNG_SK, TRGT_CD, SRC_SYS_CD "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE TRGT_DOMAIN_NM = 'CLAIM LINE DISALLOW TYPE' "
    f"AND SRC_SYS_CD = 'ABF'"
)

df_IDS_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_IDS_CD_MPPNG)
    .load()
)

df_IDS_CD_MPPNG_dedup = dedup_sort(df_IDS_CD_MPPNG, ["CD_MPPNG_SK"], [])

df_joined = df_IDS_CLM_LN_REMIT_DISALLOW_recs.alias("ClmLnRemitDisallow_to_delete").join(
    df_IDS_CD_MPPNG_dedup.alias("has_converted_cd_mppngs"),
    F.col("ClmLnRemitDisallow_to_delete.CLM_LN_DSALW_TYP_CD_SK") == F.col("has_converted_cd_mppngs.CD_MPPNG_SK"),
    "left"
)

df_delete_clm_ln_remit_dsalw = df_joined.filter(
    F.col("has_converted_cd_mppngs.CD_MPPNG_SK").isNotNull()
).select(
    F.col("ClmLnRemitDisallow_to_delete.CLM_LN_REMIT_DSALW_SK")
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsBcbsClmLineRemitDelete_CLM_LN_REMIT_DISALLOW_temp",
    jdbc_url,
    jdbc_props
)

df_delete_clm_ln_remit_dsalw.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsBcbsClmLineRemitDelete_CLM_LN_REMIT_DISALLOW_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.CLM_LN_REMIT_DSALW AS T
USING STAGING.IdsBcbsClmLineRemitDelete_CLM_LN_REMIT_DISALLOW_temp AS S
ON T.CLM_LN_REMIT_DSALW_SK = S.CLM_LN_REMIT_DSALW_SK
WHEN MATCHED THEN DELETE;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)
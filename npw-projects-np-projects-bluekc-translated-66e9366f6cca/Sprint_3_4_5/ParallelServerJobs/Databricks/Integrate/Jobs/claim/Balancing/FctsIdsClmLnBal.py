# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 03/26/07 12:34:37 Batch  14330_45286 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_7 12/18/06 11:16:18 Batch  14232_40614 INIT bckcetl ids20 dsadm Backup for 12/18 install
# MAGIC ^1_6 04/17/06 10:35:58 Batch  13987_38166 PROMOTE bckcetl ids20 dsadm J. mahaffey
# MAGIC ^1_6 04/17/06 10:24:16 Batch  13987_37468 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_4 04/13/06 08:47:39 Batch  13983_31666 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_4 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 04/02/06 15:43:48 Batch  13972_56632 INIT bckcett devlIDS30 dcg01 steffy
# MAGIC ^1_4 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_3 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_2 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 01/06/06 12:42:54 Batch  13886_45778 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC © Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsClmLnBal
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Using data created during processing, this job balances Facets inputs to IDS.  
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsIdsBalSeq
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC               UV LoadDates Entry:  None
# MAGIC               UV RunDate Entry:     None
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  none
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Compare list of Claim Lines created in FctsClmLnExtr to the IDS Claim Line table.  Any rows in the list but not on the IDS CLM_LN table are written to the output file.  The link to the output file is checked in the sequencer job.  If the link has a non-zero value an email notifies the warehouse on-call person of the deviation.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Job  Name                  Database                    Table Name
# MAGIC                  ----------------------------       ----------------------------       ---------------------------------------------------------------------
# MAGIC                                                     IDS_PROD                 W_BAL_IDS_CLM_LN
# MAGIC                                                     IDS_PROD                 CLM_LN
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED: None
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: None
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  ../balancing/IDS_CLM_LN_MISSING.dat.#RunID#
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset, just restart
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-12-27      Brent Leland             Original Programming.
# MAGIC                         
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC © Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsBalSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Using data created during processing, this job balances Facets inputs to IDS.  
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  FctsClmRunAllJc
# MAGIC 
# MAGIC 
# MAGIC INPUTS: See "Parameters" tab
# MAGIC               UV LoadDates Entry:  None
# MAGIC               UV RunDate Entry:     None
# MAGIC                
# MAGIC 
# MAGIC EMAIL FILES:  ../balancing/FctsIdsClmLnNotify.dat
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Extract jobs for a IDS target table create a list of records extracted from the source.  This sequencer loads that data and compares those records with the rows loaded into the target table.  Any deviation from perfect causes an email to be sent to the warehouse on-call account.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Job  Name                  Database                    Table Name
# MAGIC                  ----------------------------       ----------------------------       ---------------------------------------------------------------------
# MAGIC                                                     IDS_PROD                 W_TASK_EXCL
# MAGIC                                                     Universe                     TASK_EXCL
# MAGIC                                                     Universe                      RUN_CYCLE
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED: WriteRunID
# MAGIC                                                                          RunCycExctnSel
# MAGIC                                                                          GetExclusionList
# MAGIC                                                                          GetRunInfo
# MAGIC 
# MAGIC HASH FILES: None
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  
# MAGIC                  Job  Name                   Table Data Created
# MAGIC                  ----------------------------        -----------------------------------------------------------------------------------------------------------
# MAGIC                  FctsIdsClmLnBal         IDS_CLM_LN_MISSING.dat.#RunID#
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Load files will have to be recreated. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-12-27      Brent Leland             Original Programming.

# MAGIC Facets - IDS
# MAGIC Claim Line Balancing
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_w_bal_ids_clm_ln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO FROM {IDSOwner}.W_BAL_IDS_CLM_LN")
    .load()
)

df_clm_ln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO FROM {IDSOwner}.CLM_LN")
    .load()
)

df_not_exist = df_w_bal_ids_clm_ln.alias("w").join(
    df_clm_ln.alias("c"),
    (
        (F.col("w.SRC_SYS_CD_SK") == F.col("c.SRC_SYS_CD_SK"))
        & (F.col("w.CLM_ID") == F.col("c.CLM_ID"))
        & (F.col("w.CLM_LN_SEQ_NO") == F.col("c.CLM_LN_SEQ_NO"))
    ),
    "left_anti"
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.FctsIdsClmLnBal_IDS_CLM_LN_temp", jdbc_url, jdbc_props)

df_not_exist.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsClmLnBal_IDS_CLM_LN_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.CLM_LN AS T
USING STAGING.FctsIdsClmLnBal_IDS_CLM_LN_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
AND T.CLM_ID = S.CLM_ID
AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN NOT MATCHED THEN
INSERT (SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO)
VALUES (S.SRC_SYS_CD_SK, S.CLM_ID, S.CLM_LN_SEQ_NO);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_IDS_CLM_LN = df_not_exist

df_Missing_Clm_Ln = df_IDS_CLM_LN.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO")
)

df_Missing_Clm_Ln = df_Missing_Clm_Ln.withColumn("CLM_ID", F.rpad("CLM_ID", 12, " "))
df_Missing_Clm_Ln = df_Missing_Clm_Ln.select("SRC_SYS_CD", "CLM_ID", "CLM_LN_SEQ_NO")

write_files(
    df_Missing_Clm_Ln,
    f"{adls_path}/balancing/IDS_CLM_LN_MISSING.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 04/18/07 14:50:09 Batch  14353_53412 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_4 06/05/06 10:28:06 Batch  14036_37701 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_4 06/05/06 10:23:52 Batch  14036_37451 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_6 05/19/06 09:50:58 Batch  14019_35469 PROMOTE bckcett testIDS30 u10913 Ollie moving KCREE to test
# MAGIC ^1_6 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test
# MAGIC ^1_5 04/24/06 11:17:23 Batch  13994_40653 INIT bckcett devlIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_4 04/07/06 12:18:30 Batch  13977_44314 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 03/29/06 06:58:50 Batch  13968_25134 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 03/28/06 13:27:31 Batch  13967_48459 PROMOTE bckcetl devlIDS30 dsadm Brent
# MAGIC ^1_2 03/28/06 13:23:55 Batch  13967_48241 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 01/26/06 07:54:36 Batch  13906_28483 INIT bckcetl ids20 dsadm Gina
# MAGIC ^1_1 01/19/06 15:18:08 Batch  13899_55093 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 01/19/06 15:15:01 Batch  13899_54905 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_2 01/18/06 14:46:30 Batch  13898_53195 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 01/18/06 14:45:42 Batch  13898_53145 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 01/06/06 12:49:19 Batch  13886_46164 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  IdsDMClmLnBal
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: Using data created during processing, this job balances IDS inputs to Claim Datamart.  
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsDMBalSeq
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
# MAGIC PROCESSING:   Compare list of Claim Lines created in IdsClmMartClmLnExtr to the Datamart Claim Line table.  Any rows in the list but not on the Datamart CLM_DM_CLM_LN table are written to the output file.  The link to the output file is checked in the sequencer job.  If the link has a non-zero value an email notifies the warehouse on-call person of the deviation.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Job  Name                  Database                    Table Name
# MAGIC                  ----------------------------       ----------------------------       ---------------------------------------------------------------------
# MAGIC                                                     Claim_Datamart           W_BAL_DM_CLM_LN
# MAGIC                                                     Claim_Datamart          CLM_DM_CLM_LN
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED: None
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: None
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:  ../balancing/DM_CLM_LN_MISSING.dat.#RunID#
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset, just restart
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2005-12-29      Brent Leland             Original Programming.
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
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
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

# MAGIC IDS - Claim Datamart
# MAGIC Claim Line Balancing
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


RunID = get_widget_value('RunID','20051215')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
extract_query = f"""SELECT SRC_SYS_CD,
       CLM_ID,
       CLM_LN_SEQ_NO
  FROM {ClmMartOwner}.W_BAL_DM_CLM_LN w
 WHERE NOT EXISTS (
   SELECT *
     FROM {ClmMartOwner}.CLM_DM_CLM_LN cl,
          {ClmMartOwner}.CLM_DM_CLM c,
          {ClmMartOwner}.CLM_DM_INIT_CLM ci
    WHERE cl.SRC_SYS_CD = w.SRC_SYS_CD
      AND cl.CLM_ID = w.CLM_ID
      AND cl.CLM_LN_SEQ_NO = w.CLM_LN_SEQ_NO
      AND cl.SRC_SYS_CD = c.SRC_SYS_CD
      AND cl.CLM_ID = c.CLM_ID
      AND cl.SRC_SYS_CD = ci.SRC_SYS_CD
      AND cl.CLM_ID = ci.CLM_ID
 )
"""

df_CLM_DM_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = df_CLM_DM_CLM_LN.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

df_Trans1 = df_Trans1.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))

write_files(
    df_Trans1,
    f"{adls_path}/balancing/DM_CLM_LN_MISSING.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    '"',
    None
)
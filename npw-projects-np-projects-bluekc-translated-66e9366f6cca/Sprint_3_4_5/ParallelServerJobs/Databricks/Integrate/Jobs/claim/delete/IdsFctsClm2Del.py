# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_6 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_5 04/30/08 13:35:18 Batch  14731_48922 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_5 04/30/08 13:24:16 Batch  14731_48258 INIT bckcett testIDScur dsadm bls for sa
# MAGIC ^1_3 04/29/08 12:49:20 Batch  14730_46164 PROMOTE bckcett testIDScur u03651 steph for Sharon
# MAGIC ^1_3 04/29/08 12:46:03 Batch  14730_45966 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_2 04/22/08 07:40:35 Batch  14723_27640 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_4 04/15/08 09:55:11 Batch  14716_35803 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 04/15/08 09:39:45 Batch  14716_34789 INIT bckcett testIDScur dsadm bls for sa
# MAGIC ^1_3 04/15/08 09:23:54 Batch  14716_33837 INIT bckcett testIDScur dsadm bls for sa
# MAGIC ^1_1 04/05/08 11:18:21 Batch  14706_40710 PROMOTE bckcett testIDScur u03651 steph for Sharon - ITS Home
# MAGIC ^1_1 04/05/08 11:08:45 Batch  14706_40128 INIT bckcett devlIDScur u03651 steffy
# MAGIC ^1_1 01/29/08 09:53:09 Batch  14639_35596 PROMOTE bckcett devlIDScur dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 08/30/07 07:19:35 Batch  14487_26382 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/27/07 10:06:05 Batch  14484_36371 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 06/21/07 14:03:38 Batch  14417_50622 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_7 05/25/07 11:20:32 Batch  14390_40859 PROMOTE bckcetl ids20 dsadm rc for steph 
# MAGIC ^1_7 05/25/07 11:11:59 Batch  14390_40332 INIT bckcett testIDS30 dsadm rc for steph 
# MAGIC ^1_1 05/23/07 07:48:45 Batch  14388_28129 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 05/23/07 07:47:40 Batch  14388_28063 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 03/07/07 15:56:27 Batch  14311_57391 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_6 12/18/06 11:16:18 Batch  14232_40614 INIT bckcetl ids20 dsadm Backup for 12/18 install
# MAGIC ^1_5 08/29/06 09:06:44 Batch  14121_32814 PROMOTE bckcetl ids20 dsadm Keith for Brent
# MAGIC ^1_5 08/29/06 09:04:56 Batch  14121_32705 INIT bckcett testIDS30 dsadm Keith for Brent
# MAGIC ^1_2 08/24/06 14:37:08 Batch  14116_52633 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 08/24/06 14:36:15 Batch  14116_52577 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_4 04/17/06 10:35:58 Batch  13987_38166 PROMOTE bckcetl ids20 dsadm J. mahaffey
# MAGIC ^1_4 04/17/06 10:24:16 Batch  13987_37468 INIT bckcett testIDS30 dsadm J. Mahaffey for B. Leland
# MAGIC ^1_2 04/13/06 08:47:39 Batch  13983_31666 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_2 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_2 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_1 03/29/06 05:18:35 Batch  13968_19121 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Using the  X_RCRD_DEL table as a driver table delete claims no longer in IDS that are no longer in the source system.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   After data is loaded delete claim records matching the driver table where the run cycle update is older than the current run.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            04-06-2005                                      Initial programming.
# MAGIC Brent Leland            08/15/2006                                    Changed to select records then delete.
# MAGIC Steph Goddard        5/21/07            3256 HDMS          Added CLM_LN_SAV                                                                  devlIDS30                      Brent Leland              05-23-2007
# MAGIC Bhoomi D                03/26/2008       3255 ITS/Home   Added CLM_PCA, CLM_LN_PCA                                                 devlIDSCur                    Steph Goddard          04/08/2008
# MAGIC SAndrew                 04/17/2008       3255 ITSHome       Added ITS_CLM_MSG.   Fixed CLM_LN_PCA                           devlIDScur                    Steph Goddard          04/21/2008
# MAGIC                                                                                        Added hash files to eliminated the select blocking the delete.

# MAGIC Delete claim records that are no longer in the source system.
# MAGIC The CLM_COB delete is done in the extract job to remove historical records where the code type in the natural key has changed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value('RunCycle','0')
SourceSK = get_widget_value('SourceSK','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Select_CLM_LN
extract_query_Select_CLM_LN = f"""
SELECT CLM_LN_SK
FROM {IDSOwner}.CLM_LN c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_LN)
        .load()
)

# CLM_LN (Delete)
temp_table_CLM_LN = "STAGING.IdsFctsClm2Del_CLM_LN_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_LN}", jdbc_url, jdbc_props)
df_Select_CLM_LN.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_LN).mode("overwrite").save()
merge_sql_CLM_LN = f"""
MERGE {IDSOwner}.CLM_LN AS T
USING {temp_table_CLM_LN} AS S
ON T.CLM_LN_SK = S.CLM_LN_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_LN, jdbc_url, jdbc_props)

# Select_CLM_DIAG
extract_query_Select_CLM_DIAG = f"""
SELECT CLM_DIAG_SK
FROM {IDSOwner}.CLM_DIAG c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_DIAG = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_DIAG)
        .load()
)

# CLM_DIAG (Delete)
temp_table_CLM_DIAG = "STAGING.IdsFctsClm2Del_CLM_DIAG_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_DIAG}", jdbc_url, jdbc_props)
df_Select_CLM_DIAG.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_DIAG).mode("overwrite").save()
merge_sql_CLM_DIAG = f"""
MERGE {IDSOwner}.CLM_DIAG AS T
USING {temp_table_CLM_DIAG} AS S
ON T.CLM_DIAG_SK = S.CLM_DIAG_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_DIAG, jdbc_url, jdbc_props)

# Select_CLM_PROV
extract_query_Select_CLM_PROV = f"""
SELECT CLM_PROV_SK
FROM {IDSOwner}.CLM_PROV c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_PROV = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_PROV)
        .load()
)

# CLM_PROV (Delete)
temp_table_CLM_PROV = "STAGING.IdsFctsClm2Del_CLM_PROV_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_PROV}", jdbc_url, jdbc_props)
df_Select_CLM_PROV.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_PROV).mode("overwrite").save()
merge_sql_CLM_PROV = f"""
MERGE {IDSOwner}.CLM_PROV AS T
USING {temp_table_CLM_PROV} AS S
ON T.CLM_PROV_SK = S.CLM_PROV_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_PROV, jdbc_url, jdbc_props)

# Select_FCLTY_CLM_PROC
extract_query_Select_FCLTY_CLM_PROC = f"""
SELECT FCLTY_CLM_PROC_SK
FROM {IDSOwner}.FCLTY_CLM_PROC c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_FCLTY_CLM_PROC = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_FCLTY_CLM_PROC)
        .load()
)

# FCLTY_CLM_PROC (Delete)
temp_table_FCLTY_CLM_PROC = "STAGING.IdsFctsClm2Del_FCLTY_CLM_PROC_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_FCLTY_CLM_PROC}", jdbc_url, jdbc_props)
df_Select_FCLTY_CLM_PROC.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_FCLTY_CLM_PROC).mode("overwrite").save()
merge_sql_FCLTY_CLM_PROC = f"""
MERGE {IDSOwner}.FCLTY_CLM_PROC AS T
USING {temp_table_FCLTY_CLM_PROC} AS S
ON T.FCLTY_CLM_PROC_SK = S.FCLTY_CLM_PROC_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_FCLTY_CLM_PROC, jdbc_url, jdbc_props)

# Select_CLM_ATCHMT
extract_query_Select_CLM_ATCHMT = f"""
SELECT CLM_ATCHMT_SK
FROM {IDSOwner}.CLM_ATCHMT c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_ATCHMT = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_ATCHMT)
        .load()
)

# CLM_ATCHMT (Delete)
temp_table_CLM_ATCHMT = "STAGING.IdsFctsClm2Del_CLM_ATCHMT_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_ATCHMT}", jdbc_url, jdbc_props)
df_Select_CLM_ATCHMT.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_ATCHMT).mode("overwrite").save()
merge_sql_CLM_ATCHMT = f"""
MERGE {IDSOwner}.CLM_ATCHMT AS T
USING {temp_table_CLM_ATCHMT} AS S
ON T.CLM_ATCHMT_SK = S.CLM_ATCHMT_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_ATCHMT, jdbc_url, jdbc_props)

# Select_CLM_ALT_PAYE
extract_query_Select_CLM_ALT_PAYE = f"""
SELECT CLM_ALT_PAYE_SK
FROM {IDSOwner}.CLM_ALT_PAYE c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_ALT_PAYE = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_ALT_PAYE)
        .load()
)

# CLM_ALT_PAYE (Delete)
temp_table_CLM_ALT_PAYE = "STAGING.IdsFctsClm2Del_CLM_ALT_PAYE_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_ALT_PAYE}", jdbc_url, jdbc_props)
df_Select_CLM_ALT_PAYE.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_ALT_PAYE).mode("overwrite").save()
merge_sql_CLM_ALT_PAYE = f"""
MERGE {IDSOwner}.CLM_ALT_PAYE AS T
USING {temp_table_CLM_ALT_PAYE} AS S
ON T.CLM_ALT_PAYE_SK = S.CLM_ALT_PAYE_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_ALT_PAYE, jdbc_url, jdbc_props)

# Select_CLM_OVRD
extract_query_Select_CLM_OVRD = f"""
SELECT CLM_OVRD_SK
FROM {IDSOwner}.CLM_OVRD c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_OVRD = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_OVRD)
        .load()
)

# CLM_OVRD (Delete)
temp_table_CLM_OVRD = "STAGING.IdsFctsClm2Del_CLM_OVRD_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_OVRD}", jdbc_url, jdbc_props)
df_Select_CLM_OVRD.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_OVRD).mode("overwrite").save()
merge_sql_CLM_OVRD = f"""
MERGE {IDSOwner}.CLM_OVRD AS T
USING {temp_table_CLM_OVRD} AS S
ON T.CLM_OVRD_SK = S.CLM_OVRD_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_OVRD, jdbc_url, jdbc_props)

# Select_CLM_LN_PROC_CD_MOD
extract_query_Select_CLM_LN_PROC_CD_MOD = f"""
SELECT CLM_LN_PROC_CD_MOD_SK
FROM {IDSOwner}.CLM_LN_PROC_CD_MOD c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_PROC_CD_MOD = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_LN_PROC_CD_MOD)
        .load()
)

# CLM_LN_PROC_CD_MOD (Delete)
temp_table_CLM_LN_PROC_CD_MOD = "STAGING.IdsFctsClm2Del_CLM_LN_PROC_CD_MOD_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_LN_PROC_CD_MOD}", jdbc_url, jdbc_props)
df_Select_CLM_LN_PROC_CD_MOD.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_LN_PROC_CD_MOD).mode("overwrite").save()
merge_sql_CLM_LN_PROC_CD_MOD = f"""
MERGE {IDSOwner}.CLM_LN_PROC_CD_MOD AS T
USING {temp_table_CLM_LN_PROC_CD_MOD} AS S
ON T.CLM_LN_PROC_CD_MOD_SK = S.CLM_LN_PROC_CD_MOD_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_LN_PROC_CD_MOD, jdbc_url, jdbc_props)

# Select_P_CLM_LN_DSALW
extract_query_Select_P_CLM_LN_DSALW = f"""
SELECT SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DSALW_TYP_CD
FROM {IDSOwner}.P_CLM_LN_DSALW c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_P_CLM_LN_DSALW = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_P_CLM_LN_DSALW)
        .load()
)

# hf_pclmlndsalw_del (Scenario A - intermediate hashed file). Deduplicate by key columns, then pass to next stage.
df_hf_pclmlndsalw_del = dedup_sort(
    df_Select_P_CLM_LN_DSALW,
    ["SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DSALW_TYP_CD"],
    []
)

# P_CLM_LN_DSALW (Delete)
temp_table_P_CLM_LN_DSALW = "STAGING.IdsFctsClm2Del_P_CLM_LN_DSALW_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_P_CLM_LN_DSALW}", jdbc_url, jdbc_props)
df_hf_pclmlndsalw_del.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_P_CLM_LN_DSALW).mode("overwrite").save()
merge_sql_P_CLM_LN_DSALW = f"""
MERGE {IDSOwner}.P_CLM_LN_DSALW AS T
USING {temp_table_P_CLM_LN_DSALW} AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
 AND T.CLM_ID = S.CLM_ID
 AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
 AND T.CLM_LN_DSALW_TYP_CD = S.CLM_LN_DSALW_TYP_CD
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_P_CLM_LN_DSALW, jdbc_url, jdbc_props)

# Select_CLM_PCA
extract_query_Select_CLM_PCA = f"""
SELECT CLM_SK
FROM {IDSOwner}.CLM_PCA c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_PCA = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_PCA)
        .load()
)

# CLM_PCA (Delete)
temp_table_CLM_PCA = "STAGING.IdsFctsClm2Del_CLM_PCA_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_PCA}", jdbc_url, jdbc_props)
df_Select_CLM_PCA.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_PCA).mode("overwrite").save()
merge_sql_CLM_PCA = f"""
MERGE {IDSOwner}.CLM_PCA AS T
USING {temp_table_CLM_PCA} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_PCA, jdbc_url, jdbc_props)

# Select_CLM_LN_PCA
extract_query_Select_CLM_LN_PCA = f"""
SELECT CLM_LN_SK
FROM {IDSOwner}.CLM_LN_PCA c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_PCA = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_LN_PCA)
        .load()
)

# CLM_LN_PCA (Delete)
temp_table_CLM_LN_PCA = "STAGING.IdsFctsClm2Del_CLM_LN_PCA_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_LN_PCA}", jdbc_url, jdbc_props)
df_Select_CLM_LN_PCA.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_LN_PCA).mode("overwrite").save()
merge_sql_CLM_LN_PCA = f"""
MERGE {IDSOwner}.CLM_LN_PCA AS T
USING {temp_table_CLM_LN_PCA} AS S
ON T.CLM_LN_SK = S.CLM_LN_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_LN_PCA, jdbc_url, jdbc_props)

# Select_CLM_LN_SAV
extract_query_Select_CLM_LN_SAV = f"""
SELECT CLM_LN_SAV_SK
FROM {IDSOwner}.CLM_LN_SAV c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_SAV = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_LN_SAV)
        .load()
)

# hf_cmlnsav_del (Scenario A)
df_hf_cmlnsav_del = dedup_sort(
    df_Select_CLM_LN_SAV,
    ["CLM_LN_SAV_SK"],
    []
)

# CLM_LN_SAV (Delete)
temp_table_CLM_LN_SAV = "STAGING.IdsFctsClm2Del_CLM_LN_SAV_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_LN_SAV}", jdbc_url, jdbc_props)
df_hf_cmlnsav_del.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_LN_SAV).mode("overwrite").save()
merge_sql_CLM_LN_SAV = f"""
MERGE {IDSOwner}.CLM_LN_SAV AS T
USING {temp_table_CLM_LN_SAV} AS S
ON T.CLM_LN_SAV_SK = S.CLM_LN_SAV_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_LN_SAV, jdbc_url, jdbc_props)

# Select_CLM_LN_DSALW
extract_query_Select_CLM_LN_DSALW = f"""
SELECT CLM_LN_DSALW_SK
FROM {IDSOwner}.CLM_LN_DSALW c,
     {IDSOwner}.W_FCTS_RCRD_DEL d
WHERE d.SUBJ_NM = 'CLM_ID'
  AND c.SRC_SYS_CD_SK = {SourceSK}
  AND d.KEY_VAL = c.CLM_ID
  AND c.LAST_UPDT_RUN_CYC_EXCTN_SK <> {RunCycle}
"""
df_Select_CLM_LN_DSALW = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", extract_query_Select_CLM_LN_DSALW)
        .load()
)

# hf_cmlndsalw_del (Scenario A)
df_hf_cmlndsalw_del = dedup_sort(
    df_Select_CLM_LN_DSALW,
    ["CLM_LN_DSALW_SK"],
    []
)

# CLM_LN_DSALW (Delete)
temp_table_CLM_LN_DSALW = "STAGING.IdsFctsClm2Del_CLM_LN_DSALW_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_CLM_LN_DSALW}", jdbc_url, jdbc_props)
df_hf_cmlndsalw_del.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_CLM_LN_DSALW).mode("overwrite").save()
merge_sql_CLM_LN_DSALW = f"""
MERGE {IDSOwner}.CLM_LN_DSALW AS T
USING {temp_table_CLM_LN_DSALW} AS S
ON T.CLM_LN_DSALW_SK = S.CLM_LN_DSALW_SK
WHEN MATCHED THEN DELETE;
"""
execute_dml(merge_sql_CLM_LN_DSALW, jdbc_url, jdbc_props)
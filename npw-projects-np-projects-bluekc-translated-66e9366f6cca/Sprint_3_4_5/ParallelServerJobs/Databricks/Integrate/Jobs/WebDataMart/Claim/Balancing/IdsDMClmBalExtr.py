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
# MAGIC ^1_2 08/14/06 15:26:11 Batch  14106_55599 PROMOTE bckcetl ids20 dsadm Keith for Steph
# MAGIC ^1_2 08/14/06 15:19:34 Batch  14106_55199 INIT bckcett testIDS30 dsadm Keith for Steph
# MAGIC ^1_1 08/08/06 08:45:57 Batch  14100_31562 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 08/08/06 08:45:01 Batch  14100_31503 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 06/05/06 10:28:06 Batch  14036_37701 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_1 06/05/06 10:23:52 Batch  14036_37451 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_4 05/19/06 09:50:58 Batch  14019_35469 PROMOTE bckcett testIDS30 u10913 Ollie moving KCREE to test
# MAGIC ^1_4 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test
# MAGIC ^1_3 04/24/06 11:17:23 Batch  13994_40653 INIT bckcett devlIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_2 04/07/06 12:18:30 Batch  13977_44314 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 03/29/06 06:58:50 Batch  13968_25134 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_2 01/18/06 14:45:42 Batch  13898_53145 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 01/06/06 12:49:19 Batch  13886_46164 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  IdsDMClmBalExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION: This job extracts data from IDS W_CLM_BAL and applies transformations as necessary to convert the data to Integrated Codes (i.e. Claim Status of A02 insead of 02). This data is loaded to DM W_CLM_BAL and later used for Balancing the Claims Datamart Claim table.
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
# MAGIC PROCESSING:   Extract all records from the IDS W_CLM_BAL table.  Lookup converted Claim Status and Type in the cd_mppng table.  Apply transformations as necessary to convert the data to Integrated Codes.  Set dates of UNK or NA to NULL to match Claims DataMart.  Load the data to Claims Datamart W_CLM_BAL.
# MAGIC 
# MAGIC 
# MAGIC UNIX SCRIPTS USED:  None
# MAGIC 
# MAGIC 
# MAGIC DATABASE TABLES USED:
# MAGIC                  Job  Name                  Database                    Table Name
# MAGIC                  ----------------------------       ----------------------------       ---------------------------------------------------------------------
# MAGIC                                                     IDS                            W_CLM_BAL
# MAGIC                                                                                       CD_MPPNG
# MAGIC                                                     Claims Datamart         W_CLM_BAL
# MAGIC 
# MAGIC 
# MAGIC BCBSKC TRANSFORMS/ROUTINES USED: None
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: hf_clm_dm_clm_status, hf_clm_dm_clm_typ, hf_clm_dm_clm_srcsys
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:   Claims Datamart W_CLM_BAL
# MAGIC         
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset, just restart
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-01-22      Gina Harris                Original Programming.
# MAGIC 2006-04-06      Brent Leland             Removed date "where" clause from IDS driver table.  All records are extracted each time.               
# MAGIC 2006-08-07      Steph Goddard         Added TwoYearsAgo parameter, use in extract from IDS
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
# MAGIC Extract Claim Balance Table to Claim 
# MAGIC Datamart
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','20051215')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
TwoYearsAgo = get_widget_value('TwoYearsAgo','')

# Stage: IDS_W_CLM_BAL (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_IDS_W_CLM_BAL = f"""
SELECT 
  SRC_SYS_CD_SK as SRC_SYS_CD_SK,
  CLM_ID as CLM_ID,
  CLM_STTUS_CD as CLM_STTUS_CD,
  CLM_TYP_CD as CLM_TYP_CD,
  CLM_SUBTYP_CD as CLM_SUBTYP_CD,
  CLM_PD_DT_SK as CLM_PD_DT_SK,
  CLM_REMIT_HIST_CHK_PD_DT_SK as CLM_REMIT_HIST_CHK_PD_DT_SK,
  CLM_STTUS_DT_SK as CLM_STTUS_DT_SK,
  CLM_PAYBL_AMT as CLM_PAYBL_AMT,
  CLM_ACTL_PD_AMT as CLM_ACTL_PD_AMT,
  CLM_CHRG_AMT as CLM_CHRG_AMT,
  CLM_REMIT_HIST_CHK_NET_PAY_AMT as CLM_REMIT_HIST_CHK_NET_PAY_AMT,
  CLM_REMIT_HIST_CHK_NO as CLM_REMIT_HIST_CHK_NO,
  MBR_UNIQ_KEY as MBR_UNIQ_KEY,
  PD_PROV_ID as PD_PROV_ID,
  CLM_PAYE_CD,
  FCLTY_CLM_BILL_CLS_CD,
  FCLTY_CLM_BILL_TYP_CD
FROM {IDSOwner}.W_CLM_BAL
WHERE CLM_STTUS_DT_SK >= '{TwoYearsAgo}'
"""
df_IDS_W_CLM_BAL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_W_CLM_BAL)
    .load()
)

# Stage: IDS_CLM_TYPE (DB2Connector)
extract_query_IDS_CLM_TYPE = f"""
SELECT 
  SRC_CD,
  TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_SYS_CD = 'FACETS'
  AND SRC_DOMAIN_NM = 'CLAIM TYPE'
  AND TRGT_DOMAIN_NM = 'CLAIM TYPE'
"""
df_IDS_CLM_TYPE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_CLM_TYPE)
    .load()
)
# Hashed file hf_clm_dm_clm_typ (Scenario A) -> deduplicate on key column SRC_CD
df_hf_clm_dm_clm_typ = dedup_sort(df_IDS_CLM_TYPE, partition_cols=["SRC_CD"], sort_cols=[])

# Stage: IDS_CLM_STATUS (DB2Connector)
extract_query_IDS_CLM_STATUS = f"""
SELECT 
  SRC_CD,
  TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_SYS_CD = 'FACETS'
  AND SRC_DOMAIN_NM = 'CLAIM STATUS'
  AND TRGT_DOMAIN_NM = 'CLAIM STATUS'
"""
df_IDS_CLM_STATUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_CLM_STATUS)
    .load()
)
# Hashed file hf_clm_dm_clm_status (Scenario A) -> deduplicate on key column SRC_CD
df_hf_clm_dm_clm_status = dedup_sort(df_IDS_CLM_STATUS, partition_cols=["SRC_CD"], sort_cols=[])

# Stage: IDS_SRC_SYS (DB2Connector)
extract_query_IDS_SRC_SYS = f"""
SELECT 
  CD_MPPNG_SK,
  TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_SYS_CD = 'FACETS'
  AND SRC_DOMAIN_NM = 'SOURCE SYSTEM'
  AND TRGT_DOMAIN_NM = 'SOURCE SYSTEM'
"""
df_IDS_SRC_SYS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_SRC_SYS)
    .load()
)
# Hashed file hf_clm_dm_clm_srcsys (Scenario A) -> deduplicate on key column CD_MPPNG_SK
df_hf_clm_dm_clm_srcsys = dedup_sort(df_IDS_SRC_SYS, partition_cols=["CD_MPPNG_SK"], sort_cols=[])

# Transformer Stage: Trans1
df_trans1_pre = (
    df_IDS_W_CLM_BAL.alias("Facets_Codes")
    .join(df_hf_clm_dm_clm_typ.alias("Type"), F.col("Facets_Codes.CLM_TYP_CD") == F.col("Type.SRC_CD"), "left")
    .join(df_hf_clm_dm_clm_status.alias("Status"), F.col("Facets_Codes.CLM_STTUS_CD") == F.col("Status.SRC_CD"), "left")
    .join(df_hf_clm_dm_clm_srcsys.alias("SrcSysCd"), F.col("Facets_Codes.SRC_SYS_CD_SK") == F.col("SrcSysCd.CD_MPPNG_SK"), "left")
)

df_trans1 = df_trans1_pre.select(
    F.when(F.col("SrcSysCd.TRGT_CD").isNull(), F.col("Facets_Codes.SRC_SYS_CD_SK")).otherwise(F.col("SrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Facets_Codes.CLM_ID").alias("CLM_ID"),
    F.when(F.col("Status.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Status.TRGT_CD")).alias("CLM_STTUS_CD"),
    F.when(F.col("Type.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Type.TRGT_CD")).alias("CLM_TYP_CD"),
    F.when(
        (F.col("Facets_Codes.CLM_SUBTYP_CD") == "M") | (F.col("Facets_Codes.CLM_SUBTYP_CD") == "D"),
        "PR"
    ).when(
        F.col("Facets_Codes.CLM_SUBTYP_CD") == "H",
        F.when(F.col("Facets_Codes.FCLTY_CLM_BILL_TYP_CD") > 6, "OP").otherwise(
            F.when(
                (F.col("Facets_Codes.FCLTY_CLM_BILL_TYP_CD") < 7)
                & (
                    (F.col("Facets_Codes.FCLTY_CLM_BILL_CLS_CD") == 3)
                    | (F.col("Facets_Codes.FCLTY_CLM_BILL_CLS_CD") == 4)
                ),
                "OP"
            ).otherwise("IP")
        )
    ).otherwise("UNK").alias("CLM_SUBTYP_CD"),
    F.when(
        (trim(F.col("Facets_Codes.CLM_PD_DT_SK")) == "UNK")
        | (trim(F.col("Facets_Codes.CLM_PD_DT_SK")) == "NA"),
        F.lit(None)
    ).otherwise(F.col("Facets_Codes.CLM_PD_DT_SK")).alias("CLM_PD_DT"),
    F.when(
        (trim(F.col("Facets_Codes.CLM_REMIT_HIST_CHK_PD_DT_SK")) == "UNK")
        | (trim(F.col("Facets_Codes.CLM_REMIT_HIST_CHK_PD_DT_SK")) == "NA"),
        F.lit(None)
    ).otherwise(F.col("Facets_Codes.CLM_REMIT_HIST_CHK_PD_DT_SK")).alias("CLM_REMIT_HIST_CHK_PD_DT"),
    F.when(
        (trim(F.col("Facets_Codes.CLM_STTUS_DT_SK")) == "UNK")
        | (trim(F.col("Facets_Codes.CLM_STTUS_DT_SK")) == "NA"),
        F.lit(None)
    ).otherwise(F.col("Facets_Codes.CLM_STTUS_DT_SK")).alias("CLM_STTUS_DT"),
    F.col("Facets_Codes.CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    F.col("Facets_Codes.CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    F.col("Facets_Codes.CLM_CHRG_AMT").alias("CLM_CHRG_AMT"),
    F.col("Facets_Codes.CLM_REMIT_HIST_CHK_NET_PAY_AMT").alias("CLM_REMIT_HIST_CHK_NET_PAY_AMT"),
    F.col("Facets_Codes.CLM_REMIT_HIST_CHK_NO").alias("CLM_REMIT_HIST_CHK_NO"),
    F.col("Facets_Codes.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(
        (F.col("Facets_Codes.CLM_PAYE_CD").isin("S"," "))
        | (F.col("Facets_Codes.CLM_SUBTYP_CD") == " ")
        | F.col("Facets_Codes.CLM_SUBTYP_CD").isNull(),
        "NA"
    ).otherwise(F.col("Facets_Codes.PD_PROV_ID")).alias("PD_PROV_ID")
)

df_DM_W_CLM_BAL = df_trans1.select(
    rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    rpad(F.col("CLM_STTUS_CD"), <...>, " ").alias("CLM_STTUS_CD"),
    rpad(F.col("CLM_TYP_CD"), <...>, " ").alias("CLM_TYP_CD"),
    rpad(F.col("CLM_SUBTYP_CD"), <...>, " ").alias("CLM_SUBTYP_CD"),
    F.col("CLM_PD_DT").alias("CLM_PD_DT"),
    F.col("CLM_REMIT_HIST_CHK_PD_DT").alias("CLM_REMIT_HIST_CHK_PD_DT"),
    F.col("CLM_STTUS_DT").alias("CLM_STTUS_DT"),
    F.col("CLM_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    F.col("CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT"),
    F.col("CLM_CHRG_AMT").alias("CLM_CHRG_AMT"),
    F.col("CLM_REMIT_HIST_CHK_NET_PAY_AMT").alias("CLM_REMIT_HIST_CHK_NET_PAY_AMT"),
    rpad(F.col("CLM_REMIT_HIST_CHK_NO"), <...>, " ").alias("CLM_REMIT_HIST_CHK_NO"),
    rpad(F.col("MBR_UNIQ_KEY"), <...>, " ").alias("MBR_UNIQ_KEY"),
    rpad(F.col("PD_PROV_ID"), <...>, " ").alias("PD_PROV_ID")
)

# Stage: DM_W_CLM_BAL (CODBCStage) -> merge into #$ClmMartOwner#.W_CLM_BAL
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsDMClmBalExtr_DM_W_CLM_BAL_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

(
    df_DM_W_CLM_BAL.write.format("jdbc")
    .option("url", jdbc_url_clmmart)
    .options(**jdbc_props_clmmart)
    .option("dbtable", "STAGING.IdsDMClmBalExtr_DM_W_CLM_BAL_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.W_CLM_BAL AS T
USING STAGING.IdsDMClmBalExtr_DM_W_CLM_BAL_temp AS S
ON (T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN UPDATE SET
  T.CLM_STTUS_CD = S.CLM_STTUS_CD,
  T.CLM_TYP_CD = S.CLM_TYP_CD,
  T.CLM_SUBTYP_CD = S.CLM_SUBTYP_CD,
  T.CLM_PD_DT = S.CLM_PD_DT,
  T.CLM_REMIT_HIST_CHK_PD_DT = S.CLM_REMIT_HIST_CHK_PD_DT,
  T.CLM_STTUS_DT = S.CLM_STTUS_DT,
  T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
  T.CLM_ACTL_PD_AMT = S.CLM_ACTL_PD_AMT,
  T.CLM_CHRG_AMT = S.CLM_CHRG_AMT,
  T.CLM_REMIT_HIST_CHK_NET_PAY_AMT = S.CLM_REMIT_HIST_CHK_NET_PAY_AMT,
  T.CLM_REMIT_HIST_CHK_NO = S.CLM_REMIT_HIST_CHK_NO,
  T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
  T.PD_PROV_ID = S.PD_PROV_ID
WHEN NOT MATCHED THEN INSERT 
  (SRC_SYS_CD, CLM_ID, CLM_STTUS_CD, CLM_TYP_CD, CLM_SUBTYP_CD, CLM_PD_DT, CLM_REMIT_HIST_CHK_PD_DT, CLM_STTUS_DT, CLM_PAYBL_AMT, CLM_ACTL_PD_AMT, CLM_CHRG_AMT, CLM_REMIT_HIST_CHK_NET_PAY_AMT, CLM_REMIT_HIST_CHK_NO, MBR_UNIQ_KEY, PD_PROV_ID)
VALUES
  (S.SRC_SYS_CD, S.CLM_ID, S.CLM_STTUS_CD, S.CLM_TYP_CD, S.CLM_SUBTYP_CD, S.CLM_PD_DT, S.CLM_REMIT_HIST_CHK_PD_DT, S.CLM_STTUS_DT, S.CLM_PAYBL_AMT, S.CLM_ACTL_PD_AMT, S.CLM_CHRG_AMT, S.CLM_REMIT_HIST_CHK_NET_PAY_AMT, S.CLM_REMIT_HIST_CHK_NO, S.MBR_UNIQ_KEY, S.PD_PROV_ID);
"""

execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)
# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     EdwMedMgtExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                 Extracts from IDS and pulls into flat file to be bulk loaded into EDW.  Do nothing with the data.  Use lookups to retrieve data from the hash files.
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                 Change Description                                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                 -----------------------------------------------------------------------                                              --------------------------------       -------------------------------   ----------------------------       
# MAGIC Tao Luo                  2006-03-06                                                Original Programming.            
# MAGIC Brent Leland           2008-01-08        IAD Prod. Sup.                 Added check for NULL hash file lookups.                                                    devlEDW                        Steph Goddard          01/09/2008  
# MAGIC Bhoomi Dasari        2/08/2007        MedMgmt/                        Added LAST_UPDT_RUN_CYC_EXCTN_SK  >= ExtractRunCycle            devlEDW                        Steph Goddard          01/23/2008
# MAGIC                                                                                                  rule based upon which we extract records now.  
# MAGIC Ralph Tucker         09/15/2009     TTR-583                            Added EdwRunCycle to last updt run cyc                                                     devlEDW                        Steph Goddard          09/16/2009
# MAGIC  Ralph Tucker        5/14/2012       4896 Edw Remediation      Added new Diag_Cd_Typ fields                                                                   IntegrateNewDevl             SAndrew                   2012-05-17
# MAGIC HarikrishnaraoYadav 1/31/2024    US 610136                        Added RFRL_TYP_CD_SK field and hash file                                             EnterpriseDev2                 Jeyaprasanna            2024-02-09        
# MAGIC                                                                                                  Um_Svc_Rfrl_Typ_Cd_Sk_Lookup stage to get
# MAGIC                                                                                                  RFRL_TYP_CD, RFRL_TYP_NM fields

# MAGIC Created by EdwUmDExtr
# MAGIC Created by EdwUmDExtr
# MAGIC Created by EdwUmIpFExtr
# MAGIC Created by EdwUmIpFExtr
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
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve job parameters
CurrRunDt = get_widget_value("CurrRunDt","2012-05-14")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
EdwRunCycle = get_widget_value("EdwRunCycle","")
ExtractRunCycle = get_widget_value("ExtractRunCycle","")

# --------------------------------------------------------------------------------
# DB2Connector Stage: "IDS" - Main Extract
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_extract = f"""
SELECT UM_SVC.UM_SVC_SK as UM_SVC_SK,
       UM_SVC.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
       UM_SVC.UM_REF_ID as UM_REF_ID,
       UM_SVC.UM_SVC_SEQ_NO as UM_SVC_SEQ_NO,
       UM_SVC.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,
       UM_SVC.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,
       UM_SVC.DSALW_EXCD_SK as DSALW_EXCD_SK,
       UM_SVC.INPT_USER_SK as INPT_USER_SK,
       UM_SVC.MBR_SK as MBR_SK,
       UM_SVC.PCP_PROV_SK as PCP_PROV_SK,
       UM_SVC.PRI_DIAG_CD_SK as PRI_DIAG_CD_SK,
       UM_SVC.PROC_CD_SK as PROC_CD_SK,
       UM_SVC.RQST_PROV_SK as RQST_PROV_SK,
       UM_SVC.SVC_PROV_SK as SVC_PROV_SK,
       UM_SVC.UM_SK as UM_SK,
       UM_SVC.UM_SVC_AUTH_POS_CAT_CD_SK as UM_SVC_AUTH_POS_CAT_CD_SK,
       UM_SVC.UM_SVC_AUTH_POS_TYP_CD_SK as UM_SVC_AUTH_POS_TYP_CD_SK,
       UM_SVC.UM_SVC_DENIAL_RSN_CD_SK as UM_SVC_DENIAL_RSN_CD_SK,
       UM_SVC.UM_SVC_RQST_POS_TYP_CD_SK as UM_SVC_RQST_POS_TYP_CD_SK,
       UM_SVC.UM_SVC_STTUS_CD_SK as UM_SVC_STTUS_CD_SK,
       UM_SVC.UM_SVC_TREAT_CAT_CD_SK as UM_SVC_TREAT_CAT_CD_SK,
       UM_SVC.UM_SVC_TYP_CD_SK as UM_SVC_TYP_CD_SK,
       UM_SVC.UM_SVC_TOS_CD_SK as UM_SVC_TOS_CD_SK,
       UM_SVC.PREAUTH_IN as PREAUTH_IN,
       UM_SVC.RFRL_IN as RFRL_IN,
       UM_SVC.INPT_DT_SK as INPT_DT_SK,
       UM_SVC.RCVD_DT_SK as RCVD_DT_SK,
       UM_SVC.ATCHMT_SRC_DTM as ATCHMT_SRC_DTM,
       UM_SVC.AUTH_DT_SK as AUTH_DT_SK,
       UM_SVC.DENIAL_DT_SK as DENIAL_DT_SK,
       UM_SVC.SVC_END_DT_SK as SVC_END_DT_SK,
       UM_SVC.NEXT_RVW_DT_SK as NEXT_RVW_DT_SK,
       UM_SVC.SVC_STRT_DT_SK as SVC_STRT_DT_SK,
       UM_SVC.ALW_AMT as ALW_AMT,
       UM_SVC.PD_AMT as PD_AMT,
       UM_SVC.ALW_UNIT_CT as ALW_UNIT_CT,
       UM_SVC.AUTH_UNIT_CT as AUTH_UNIT_CT,
       UM_SVC.MBR_AGE as MBR_AGE,
       UM_SVC.PD_UNIT_CT as PD_UNIT_CT,
       UM_SVC.RQST_UNIT_CT as RQST_UNIT_CT,
       UM_SVC.STTUS_SEQ_NO as STTUS_SEQ_NO,
       EXCD.EXCD_ID as EXCD_ID,
       PROC_CD.PROC_CD as PROC_CD,
       UM_SVC.PROC_CD_MOD_TX as PROC_CD_MOD_TX,
       UM_SVC.SVC_RULE_TYP_TX as SVC_RULE_TYP_TX,
       UM_SVC.SVC_GRP_ID as SVC_GRP_ID,
       UM_SVC.RFRL_TYP_CD_SK as RFRL_TYP_CD_SK
FROM {IDSOwner}.UM_SVC UM_SVC,
     {IDSOwner}.EXCD EXCD,
     {IDSOwner}.PROC_CD PROC_CD
WHERE UM_SVC.DSALW_EXCD_SK = EXCD.EXCD_SK
  AND UM_SVC.PROC_CD_SK = PROC_CD.PROC_CD_SK
  AND UM_SVC.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""

df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_extract)
    .load()
)

extract_query_diag_cd = f"""
SELECT DIAG_CD.DIAG_CD_SK as DIAG_CD_SK,
       DIAG_CD.DIAG_CD as DIAG_CD,
       DIAG_CD_TYP_CD as DIAG_CD_TYP_CD,
       map.TRGT_CD_NM as DIAG_CD_TYP_CD_NM,
       DIAG_CD_TYP_CD_SK as DIAG_CD_TYP_CD_SK
FROM {IDSOwner}.DIAG_CD DIAG_CD,
     {IDSOwner}.CD_MPPNG map
WHERE DIAG_CD_TYP_CD_SK = map.CD_MPPNG_SK
"""

df_Extract_Diag_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_diag_cd)
    .load()
)

extract_query_proc_cd = f"""
SELECT PROC_CD_SK,
       PROC_CD,
       PROC_CD_TYP_CD_SK,
       MAP.TRGT_CD as PROC_CD_TYP_CD,
       MAP.TRGT_CD_NM as PROC_CD_TYP_CD_NM
FROM {IDSOwner}.PROC_CD PROC_CD,
     {IDSOwner}.CD_MPPNG map
WHERE PROC_CD_TYP_CD_SK = map.CD_MPPNG_SK
"""

df_Extract_Proc_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_proc_cd)
    .load()
)

# --------------------------------------------------------------------------------
# CHashedFileStage with no input pins => Scenario C: Read from parquet
# --------------------------------------------------------------------------------
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet") 
df_hf_um_app_user = spark.read.parquet(f"{adls_path}/hf_um_app_user.parquet") 
df_hf_um_d = spark.read.parquet(f"{adls_path}/hf_um_d.parquet") 
df_hf_um_diag_cd = spark.read.parquet(f"{adls_path}/hf_um_diag_cd.parquet") 
df_hf_um_prov = spark.read.parquet(f"{adls_path}/hf_um_prov.parquet") 

# --------------------------------------------------------------------------------
# CHashedFileStage => Scenario A (Intermediate hashed file) 
# hf_um_svc_diag_cd is fed by "Extract_Diag_Cd" => deduplicate on key: DIAG_CD_SK
# --------------------------------------------------------------------------------
df_hf_um_svc_diag_cd = dedup_sort(
    df_Extract_Diag_Cd,
    ["DIAG_CD_SK"],
    [("DIAG_CD_SK", "A")]
)

# --------------------------------------------------------------------------------
# CHashedFileStage => Scenario A (Intermediate hashed file)
# hf_um_svc_proc_cd is fed by "Extract_Proc_Cd" => deduplicate on key: PROC_CD_SK
# --------------------------------------------------------------------------------
df_hf_um_svc_proc_cd = dedup_sort(
    df_Extract_Proc_Cd,
    ["PROC_CD_SK"],
    [("PROC_CD_SK", "A")]
)

# --------------------------------------------------------------------------------
# Business_Rules Transformer
# Perform left joins on all lookup data sources
# --------------------------------------------------------------------------------
df_Business_Rules = (
    df_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("Src_Sys_Cd_Sk_Lookup"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("Src_Sys_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Auth_Pos_Cat_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_AUTH_POS_CAT_CD_SK")
        == F.col("Um_Svc_Auth_Pos_Cat_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Auth_Pos_Typ_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_AUTH_POS_TYP_CD_SK")
        == F.col("Um_Svc_Auth_Pos_Typ_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Denial_Rsn_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_DENIAL_RSN_CD_SK")
        == F.col("Um_Svc_Denial_Rsn_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Rqst_Pos_Typ_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_RQST_POS_TYP_CD_SK")
        == F.col("Um_Svc_Rqst_Pos_Typ_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Sttus_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_STTUS_CD_SK")
        == F.col("Um_Svc_Sttus_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Treat_Cat_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_TREAT_CAT_CD_SK")
        == F.col("Um_Svc_Treat_Cat_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Typ_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_TYP_CD_SK")
        == F.col("Um_Svc_Typ_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Tos_Cd_Sk_Lookup"),
        F.col("Extract.UM_SVC_TOS_CD_SK")
        == F.col("Um_Svc_Tos_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_um_app_user.alias("Inpt_User_Sk_Lookup"),
        F.col("Extract.INPT_USER_SK") == F.col("Inpt_User_Sk_Lookup.USER_SK"),
        "left",
    )
    .join(
        df_hf_um_d.alias("Um_Lookup"),
        F.col("Extract.UM_SK") == F.col("Um_Lookup.UM_SK"),
        "left",
    )
    .join(
        df_hf_um_diag_cd.alias("Pri_Diag_Cd_Sk_Lookup"),
        F.col("Extract.PRI_DIAG_CD_SK") == F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_SK"),
        "left",
    )
    .join(
        df_hf_um_prov.alias("Pcp_Prov_Sk_Lookup"),
        F.col("Extract.PCP_PROV_SK") == F.col("Pcp_Prov_Sk_Lookup.PROV_SK"),
        "left",
    )
    .join(
        df_hf_um_prov.alias("Rqst_Prov_Sk_Lookup"),
        F.col("Extract.RQST_PROV_SK") == F.col("Rqst_Prov_Sk_Lookup.PROV_SK"),
        "left",
    )
    .join(
        df_hf_um_prov.alias("Svc_Prov_Sk_Lookup"),
        F.col("Extract.SVC_PROV_SK") == F.col("Svc_Prov_Sk_Lookup.PROV_SK"),
        "left",
    )
    .join(
        df_hf_um_svc_diag_cd.alias("Diag_Cd_Sk_Lookup"),
        F.col("Extract.PRI_DIAG_CD_SK") == F.col("Diag_Cd_Sk_Lookup.DIAG_CD_SK"),
        "left",
    )
    .join(
        df_hf_um_svc_proc_cd.alias("refProcCd"),
        F.col("Extract.PROC_CD_SK") == F.col("refProcCd.PROC_CD_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Svc_Rfrl_Typ_Cd_Sk_Lookup"),
        F.col("Extract.RFRL_TYP_CD_SK")
        == F.col("Um_Svc_Rfrl_Typ_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
)

# --------------------------------------------------------------------------------
# Create final dataframe with columns in DataStage order, applying all expressions
# and rpad for char/varchar columns with given lengths
# --------------------------------------------------------------------------------
df_UM_SVC_F_dat = df_Business_Rules.select(
    F.col("Extract.UM_SVC_SK").alias("UM_SVC_SK"),
    F.when(F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Extract.UM_REF_ID").alias("UM_REF_ID"),
    F.col("Extract.UM_SVC_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    F.rpad(F.lit(CurrRunDt), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    F.rpad(F.lit(CurrRunDt), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),  # char(10)
    F.when(F.col("Um_Lookup.CASE_MGT_SK").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.CASE_MGT_SK")).alias("CASE_MGT_SK"),
    F.when(F.col("Um_Lookup.CRT_USER_SK").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.CRT_USER_SK")).alias("CRT_USER_SK"),
    F.col("Extract.DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    F.when(F.col("Um_Lookup.GRP_SK").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.GRP_SK")).alias("GRP_SK"),
    F.col("Extract.INPT_USER_SK").alias("INPT_USER_SK"),
    F.col("Extract.MBR_SK").alias("MBR_SK"),
    F.col("Extract.PCP_PROV_SK").alias("PCP_PROV_SK"),
    F.col("Extract.PRI_DIAG_CD_SK").alias("PRI_DIAG_CD_SK"),
    F.col("Extract.PROC_CD_SK").alias("PROC_CD_SK"),
    F.when(F.col("Um_Lookup.PROD_SK").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.PROD_SK")).alias("PROD_SK"),
    F.col("Extract.RQST_PROV_SK").alias("RQST_PROV_SK"),
    F.col("Extract.SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.when(F.col("Um_Lookup.SUBGRP_SK").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.SUBGRP_SK")).alias("SUBGRP_SK"),
    F.when(F.col("Um_Lookup.SUB_SK").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.SUB_SK")).alias("SUB_SK"),
    F.col("Extract.UM_SK").alias("UM_SK"),
    F.when(F.col("Um_Svc_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_AUTH_POS_CAT_CD"),
    F.when(F.col("Um_Svc_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_AUTH_POS_CAT_NM"),
    F.when(F.col("Um_Svc_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_AUTH_POS_TYP_CD"),
    F.when(F.col("Um_Svc_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_AUTH_POS_TYP_NM"),
    F.when(F.col("Um_Svc_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_DENIAL_RSN_CD"),
    F.when(F.col("Um_Svc_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_DENIAL_RSN_NM"),
    F.when(F.col("Um_Svc_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_RQST_POS_TYP_CD"),
    F.when(F.col("Um_Svc_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_RQST_POS_TYP_NM"),
    F.when(F.col("Um_Svc_Sttus_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Sttus_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_STTUS_CD"),
    F.when(F.col("Um_Svc_Sttus_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Sttus_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_STTUS_NM"),
    F.when(F.col("Um_Svc_Treat_Cat_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Treat_Cat_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_TREAT_CAT_CD"),
    F.when(F.col("Um_Svc_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_TREAT_CAT_NM"),
    F.when(F.col("Um_Svc_Typ_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Typ_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_TYP_CD"),
    F.when(F.col("Um_Svc_Typ_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Typ_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_TYP_NM"),
    F.when(F.col("Um_Svc_Tos_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Tos_Cd_Sk_Lookup.TRGT_CD")).alias("UM_SVC_TOS_CD"),
    F.when(F.col("Um_Svc_Tos_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("Um_Svc_Tos_Cd_Sk_Lookup.TRGT_CD_NM")).alias("UM_SVC_TOS_NM"),
    F.rpad(F.col("Extract.PREAUTH_IN"), 1, " ").alias("UM_SVC_PREAUTH_IN"),   # char(1)
    F.rpad(F.col("Extract.RFRL_IN"), 1, " ").alias("UM_SVC_RFRL_IN"),         # char(1)
    F.when(F.col("Um_Lookup.MED_MGT_NOTE_DTM").isNull(), F.lit("1753-01-01 00:00:00.000000"))
     .otherwise(F.col("Um_Lookup.MED_MGT_NOTE_DTM")).alias("MED_MGT_NOTE_DTM"),
    F.rpad(F.col("Extract.INPT_DT_SK"), 10, " ").alias("UM_SVC_INPT_DT_SK"),  # char(10)
    F.rpad(F.col("Extract.RCVD_DT_SK"), 10, " ").alias("UM_SVC_RCVD_DT_SK"),  # char(10)
    F.col("Extract.ATCHMT_SRC_DTM").alias("UM_SVC_ATCHMT_SRC_DTM"),
    F.col("Extract.AUTH_DT_SK").alias("UM_SVC_AUTH_DT"),
    F.col("Extract.DENIAL_DT_SK").alias("UM_SVC_DENIAL_DT"),
    F.col("Extract.SVC_END_DT_SK").alias("UM_SVC_END_DT"),
    F.col("Extract.NEXT_RVW_DT_SK").alias("UM_SVC_NEXT_RVW_DT"),
    F.col("Extract.SVC_STRT_DT_SK").alias("UM_SVC_STRT_DT"),
    F.col("Extract.ALW_AMT").alias("UM_SVC_ALW_AMT"),
    F.col("Extract.PD_AMT").alias("UM_SVC_PD_AMT"),
    F.col("Extract.ALW_UNIT_CT").alias("UM_SVC_ALW_UNIT_CT"),
    F.col("Extract.AUTH_UNIT_CT").alias("UM_SVC_AUTH_UNIT_CT"),
    F.col("Extract.MBR_AGE").alias("UM_SVC_MBR_AGE"),
    F.col("Extract.PD_UNIT_CT").alias("UM_SVC_PD_UNIT_CT"),
    F.col("Extract.RQST_UNIT_CT").alias("UM_SVC_RQST_UNIT_CT"),
    F.col("Extract.STTUS_SEQ_NO").alias("UM_SVC_STTUS_SEQ_NO"),
    F.when(F.col("Um_Lookup.CRT_USER_ID").isNull(), F.lit("UNK")).otherwise(F.col("Um_Lookup.CRT_USER_ID")).alias("CRT_USER_ID"),
    F.col("Extract.EXCD_ID").alias("DSALW_EXCD_ID"),
    F.when(F.col("Um_Lookup.GRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("Um_Lookup.GRP_ID")).alias("GRP_ID"),
    F.when(F.col("Inpt_User_Sk_Lookup.USER_ID").isNull(), F.lit("UNK")).otherwise(F.col("Inpt_User_Sk_Lookup.USER_ID")).alias("INPT_USER_ID"),
    F.when(F.col("Um_Lookup.MBR_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    F.when(F.col("Pcp_Prov_Sk_Lookup.PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("Pcp_Prov_Sk_Lookup.PROV_ID")).alias("PCP_PROV_ID"),
    F.when(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD").isNull(), F.lit("UNK")).otherwise(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD")).alias("PRI_DIAG_CD"),
    F.when(F.col("Extract.PROC_CD").isNull(), F.lit("UNK")).otherwise(F.col("Extract.PROC_CD")).alias("PROC_CD"),
    F.rpad(
        F.col("Extract.PROC_CD_MOD_TX"), 2, " "
    ).alias("PROC_CD_MOD_TX"),  # char(2)
    F.when(F.col("Um_Lookup.PROD_ID").isNull(), F.lit("UNK")).otherwise(F.col("Um_Lookup.PROD_ID")).alias("PROD_ID"),
    F.when(F.col("Rqst_Prov_Sk_Lookup.PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("Rqst_Prov_Sk_Lookup.PROV_ID")).alias("RQST_PROV_ID"),
    F.when(F.col("Svc_Prov_Sk_Lookup.PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("Svc_Prov_Sk_Lookup.PROV_ID")).alias("SVC_PROV_ID"),
    F.when(F.col("Um_Lookup.SUBGRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("Um_Lookup.SUBGRP_ID")).alias("SUBGRP_ID"),
    F.when(F.col("Um_Lookup.SUB_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("Um_Lookup.SUB_UNIQ_KEY")).alias("SUB_UNIQ_KEY"),
    F.col("Extract.SVC_RULE_TYP_TX").alias("UM_SVC_SVC_RULE_TYP_TX"),
    F.when(F.col("Extract.SVC_GRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("Extract.SVC_GRP_ID")).alias("UM_SVC_SVC_GRP_ID"),
    F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.UM_SVC_AUTH_POS_CAT_CD_SK").alias("UM_SVC_AUTH_POS_CAT_CD_SK"),
    F.col("Extract.UM_SVC_AUTH_POS_TYP_CD_SK").alias("UM_SVC_AUTH_POS_TYP_CD_SK"),
    F.col("Extract.UM_SVC_DENIAL_RSN_CD_SK").alias("UM_SVC_DENIAL_RSN_CD_SK"),
    F.col("Extract.UM_SVC_RQST_POS_TYP_CD_SK").alias("UM_SVC_RQST_POS_TYP_CD_SK"),
    F.col("Extract.UM_SVC_STTUS_CD_SK").alias("UM_SVC_STTUS_CD_SK"),
    F.col("Extract.UM_SVC_TREAT_CAT_CD_SK").alias("UM_SVC_TREAT_CAT_CD_SK"),
    F.col("Extract.UM_SVC_TYP_CD_SK").alias("UM_SVC_TYP_CD_SK"),
    F.col("Extract.UM_SVC_TOS_CD_SK").alias("UM_SVC_TOS_CD_SK"),
    F.when(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD").isNull(), F.lit("UNK"))
     .otherwise(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD")).alias("PRI_DIAG_CD_TYP_CD"),
    F.when(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM").isNull(), F.lit("UNK"))
     .otherwise(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM")).alias("PRI_DIAG_CD_TYP_NM"),
    F.when(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK").isNull(), F.lit(0))
     .otherwise(F.col("Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK")).alias("PRI_DIAG_CD_TYP_CD_SK"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD").isNull(), F.lit("UNK")).otherwise(F.col("refProcCd.PROC_CD_TYP_CD")).alias("PROC_CD_TYP_CD"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD_NM").isNull(), F.lit("UNK")).otherwise(F.col("refProcCd.PROC_CD_TYP_CD_NM")).alias("PROC_CD_TYP_NM"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD_SK").isNull(), F.lit(0)).otherwise(F.col("refProcCd.PROC_CD_TYP_CD_SK")).alias("PROC_CD_TYP_CD_SK"),
    F.when(F.col("Um_Svc_Rfrl_Typ_Cd_Sk_Lookup.TRGT_CD").isNull(), F.lit("NA"))
     .otherwise(F.col("Um_Svc_Rfrl_Typ_Cd_Sk_Lookup.TRGT_CD")).alias("RFRL_TYP_CD"),
    F.when(F.col("Um_Svc_Rfrl_Typ_Cd_Sk_Lookup.TRGT_CD_NM").isNull(), F.lit("NA"))
     .otherwise(F.col("Um_Svc_Rfrl_Typ_Cd_Sk_Lookup.TRGT_CD_NM")).alias("RFRL_TYP_NM"),
    F.when(F.col("Extract.RFRL_TYP_CD_SK").isNull(), F.lit(1)).otherwise(F.col("Extract.RFRL_TYP_CD_SK")).alias("RFRL_TYP_CD_SK")
)

# --------------------------------------------------------------------------------
# CSeqFileStage: "UM_SVC_F_dat" - Write out as .dat with given properties
# --------------------------------------------------------------------------------
write_files(
    df_UM_SVC_F_dat,
    f"{adls_path}/load/UM_SVC_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)
# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwMedMgtExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Pulls UM IP data from IDS to EDW.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #                 Change Description                                                                                Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------        ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Tao Luo                          3/1/2006          MedMgmt/                       Original Programming.                                                                               devlEDW10
# MAGIC Bhoomi Dasari                2/08/2007        MedMgmt/                        Added LAST_UPDT_RUN_CYC_EXCTN_SK  >= ExtractRunCycle        devlEDW                     Steph Goddard            01/23/2008
# MAGIC                                                                                                          rule based upon which we extract records now.  
# MAGIC                                                                                                         Added Hash.Clear and checked null's for lkups
# MAGIC O. Nielsen                       08/20/2008        Facets 4.5.1                  removed post-job hash clear.  files are used in UM_SVC_F extract            devlIDSnew                Brent Leland                 08-20-2008
# MAGIC 
# MAGIC J Reynolds                      10/21/2010       Prod Support                   modified to correctly assign default values to SK fields and Uniq keys      EnterpriseWrhsDevl
# MAGIC Ralph Tucker                 5/11/2012       4896 Edw Remediation     Added new Diag_Cd_Typ fields                                                              IntegrateNewDevl           Sharon Andrew         2012-05-17
# MAGIC Raja Gummadi                01/10/2013     TTR-1399                         Added New Parameter Edw Run Cycle                                                    EnterpriseNewDevl       Bhoomi Dasari           2/25/2013  
# MAGIC Sravya Gorla                   10/02/2019     US# 140167                 Updated the datatype of DSCHG_DTM                                                    EnterpriseDev2                Jaideep Mankala      10/07/2019
# MAGIC                                                                                                       from Timestamp(26.3) to timestamp(26.6)

# MAGIC Do not clear hashed files.  They are used in UM_SVC_F extract job
# MAGIC Created by EdwUmDExtr
# MAGIC Created by EdwUmDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


CurrRunDt = get_widget_value('CurrRunDt','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
EdwRunCycle = get_widget_value('EdwRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
  UM_IP.UM_SK AS UM_SK,
  UM_IP.SRC_SYS_CD_SK AS SRC_SYS_CD_SK,
  UM_IP.UM_REF_ID AS UM_REF_ID,
  UM_IP.ADMS_PRI_DIAG_CD_SK AS ADMS_PRI_DIAG_CD_SK,
  UM_IP.ADMS_PROV_SK AS ADMS_PROV_SK,
  UM_IP.ATND_PROV_SK AS ATND_PROV_SK,
  UM_IP.DENIED_USER_SK AS DENIED_USER_SK,
  UM_IP.DSALW_EXCD_SK AS DSALW_EXCD_SK,
  UM_IP.FCLTY_PROV_SK AS FCLTY_PROV_SK,
  UM_IP.PRI_DIAG_CD_SK AS PRI_DIAG_CD_SK,
  UM_IP.INPT_USER_SK AS INPT_USER_SK,
  UM_IP.PCP_PROV_SK AS PCP_PROV_SK,
  UM_IP.PRI_SURG_PROC_CD_SK AS PRI_SURG_PROC_CD_SK,
  UM_IP.PROV_AGMNT_SK AS PROV_AGMNT_SK,
  UM_IP.RQST_PROV_SK AS RQST_PROV_SK,
  UM_IP.SURGEON_PROV_SK AS SURGEON_PROV_SK,
  UM_IP.UM_IP_ADMS_TREAT_CAT_CD_SK AS UM_IP_ADMS_TREAT_CAT_CD_SK,
  UM_IP.UM_IP_AUTH_POS_CAT_CD_SK AS UM_IP_AUTH_POS_CAT_CD_SK,
  UM_IP.UM_IP_AUTH_POS_TYP_CD_SK AS UM_IP_AUTH_POS_TYP_CD_SK,
  UM_IP.UM_IP_CARE_TYP_CD_SK AS UM_IP_CARE_TYP_CD_SK,
  UM_IP.UM_IP_CUR_TREAT_CAT_CD_SK AS UM_IP_CUR_TREAT_CAT_CD_SK,
  UM_IP.UM_IP_DENIAL_RSN_CD_SK AS UM_IP_DENIAL_RSN_CD_SK,
  UM_IP.UM_IP_DSCHG_STTUS_CD_SK AS UM_IP_DSCHG_STTUS_CD_SK,
  UM_IP.UM_IP_FCLTY_NTWK_STTUS_CD_SK AS UM_IP_FCLTY_NTWK_STTUS_CD_SK,
  UM_IP.UM_IP_RQST_POS_CAT_CD_SK AS UM_IP_RQST_POS_CAT_CD_SK,
  UM_IP.UM_IP_RQST_POS_TYP_CD_SK AS UM_IP_RQST_POS_TYP_CD_SK,
  UM_IP.UM_IP_STTUS_CD_SK AS UM_IP_STTUS_CD_SK,
  UM_IP.PREAUTH_IN AS PREAUTH_IN,
  UM_IP.RFRL_IN AS RFRL_IN,
  UM_IP.RQST_PROV_PCP_IN AS RQST_PROV_PCP_IN,
  UM_IP.ACTL_ADMS_DT_SK AS ACTL_ADMS_DT_SK,
  UM_IP.AUTH_ADMS_DT_SK AS AUTH_ADMS_DT_SK,
  UM_IP.UM_IP_AUTH_SURG_DT_SK AS UM_IP_AUTH_SURG_DT_SK,
  UM_IP.AUTH_DT_SK AS AUTH_DT_SK,
  UM_IP.ATCHMT_SRC_DTM AS ATCHMT_SRC_DTM,
  UM_IP.DENIAL_DT_SK AS DENIAL_DT_SK,
  UM_IP.DSCHG_DTM AS DSCHG_DTM,
  UM_IP.XPCT_DSCHG_DT_SK AS XPCT_DSCHG_DT_SK,
  UM_IP.INPT_DT_SK AS INPT_DT_SK,
  UM_IP.NEXT_RVW_DT_SK AS NEXT_RVW_DT_SK,
  UM_IP.RCVD_DT_SK AS RCVD_DT_SK,
  UM_IP.RQST_ADMS_DT_SK AS RQST_ADMS_DT_SK,
  UM_IP.UM_IP_RQST_SURG_DT_SK AS UM_IP_RQST_SURG_DT_SK,
  UM_IP.STTUS_DT_SK AS STTUS_DT_SK,
  UM_IP.ACTL_LOS_DAYS_QTY AS ACTL_LOS_DAYS_QTY,
  UM_IP.ALW_TOT_LOS_DAYS_QTY AS ALW_TOT_LOS_DAYS_QTY,
  UM_IP.AUTH_TOT_LOS_DAYS_QTY AS AUTH_TOT_LOS_DAYS_QTY,
  UM_IP.BRTH_WT_QTY AS BRTH_WT_QTY,
  UM_IP.MBR_AGE AS MBR_AGE,
  UM_IP.PRI_GOAL_END_LOS_DAYS_QTY AS PRI_GOAL_END_LOS_DAYS_QTY,
  UM_IP.PRI_GOAL_STRT_LOS_DAYS_QTY AS PRI_GOAL_STRT_LOS_DAYS_QTY,
  UM_IP.RQST_PREOP_DAYS_QTY AS RQST_PREOP_DAYS_QTY,
  UM_IP.RQST_TOT_LOS_DAYS_QTY AS RQST_TOT_LOS_DAYS_QTY,
  EXCD.EXCD_ID AS DSALW_EXCD_ID,
  PROC_CD.PROC_CD AS PROC_CD,
  UM_IP.PRI_SURG_PROC_CD_MOD_TX AS PRI_SURG_PROC_CD_MOD_TX,
  UM_IP.CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK,
  UM_IP.LAST_UPDT_RUN_CYC_EXCTN_SK AS LAST_UPDT_RUN_CYC_EXCTN_SK,
  UM_IP.DSCHG_DIAG_CD_SK AS DSCHG_DIAG_CD_SK
FROM
  {IDSOwner}.UM_IP UM_IP,
  {IDSOwner}.EXCD EXCD,
  {IDSOwner}.PROC_CD PROC_CD
WHERE
  UM_IP.DSALW_EXCD_SK = EXCD.EXCD_SK
  AND UM_IP.PRI_SURG_PROC_CD_SK = PROC_CD.PROC_CD_SK
  AND UM_IP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
        """.strip()
    )
    .load()
)

df_IDS_Extract_Diag_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
  DIAG_CD.DIAG_CD_SK AS DIAG_CD_SK,
  DIAG_CD.DIAG_CD AS DIAG_CD,
  DIAG_CD_TYP_CD AS DIAG_CD_TYP_CD,
  map.TRGT_CD_NM AS DIAG_CD_TYP_CD_NM,
  DIAG_CD_TYP_CD_SK AS DIAG_CD_TYP_CD_SK
FROM
  {IDSOwner}.DIAG_CD DIAG_CD,
  {IDSOwner}.CD_MPPNG map
WHERE
  DIAG_CD_TYP_CD_SK = map.CD_MPPNG_SK
        """.strip()
    )
    .load()
)

df_IDS_Extract_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
  PROV.PROV_SK AS PROV_SK,
  PROV.PROV_ID AS PROV_ID
FROM
  {IDSOwner}.PROV PROV
        """.strip()
    )
    .load()
)

df_IDS_Extract_Proc_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
  PROC_CD_SK,
  PROC_CD,
  PROC_CD_TYP_CD_SK,
  MAP.TRGT_CD AS PROC_CD_TYP_CD,
  MAP.TRGT_CD_NM AS PROC_CD_TYP_CD_NM
FROM
  {IDSOwner}.PROC_CD PROC_CD,
  {IDSOwner}.CD_MPPNG map
WHERE
  PROC_CD_TYP_CD_SK = map.CD_MPPNG_SK
        """.strip()
    )
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")
df_hf_um_app_user = spark.read.parquet(f"{adls_path}/hf_um_app_user.parquet")
df_hf_um_d = spark.read.parquet(f"{adls_path}/hf_um_d.parquet")

df_hf_um_diag_cd = df_IDS_Extract_Diag_Cd.dropDuplicates(["DIAG_CD_SK"])
df_hf_um_prov = df_IDS_Extract_Prov.dropDuplicates(["PROV_SK"])
df_hf_um_proc_cd = df_IDS_Extract_Proc_Cd.dropDuplicates(["PROC_CD_SK"])

df_br = (
    df_IDS_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("Src_Sys_Cd_Sk_Lookup"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("Src_Sys_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_ADMS_TREAT_CAT_CD_SK")
        == F.col("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_AUTH_POS_CAT_CD_SK")
        == F.col("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_AUTH_POS_TYP_CD_SK")
        == F.col("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Care_Typ_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_CARE_TYP_CD_SK")
        == F.col("Um_Ip_Care_Typ_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_CUR_TREAT_CAT_CD_SK")
        == F.col("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Denial_Rsn_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_DENIAL_RSN_CD_SK")
        == F.col("Um_Ip_Denial_Rsn_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_DSCHG_STTUS_CD_SK")
        == F.col("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_FCLTY_NTWK_STTUS_CD_SK")
        == F.col("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_RQST_POS_CAT_CD_SK")
        == F.col("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_RQST_POS_TYP_CD_SK")
        == F.col("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("Um_Ip_Sttus_Cd_Sk_Lookup"),
        F.col("Extract.UM_IP_STTUS_CD_SK") == F.col("Um_Ip_Sttus_Cd_Sk_Lookup.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_um_d.alias("Um_Lookup"),
        F.col("Extract.UM_SK") == F.col("Um_Lookup.UM_SK"),
        "left",
    )
    .join(
        df_hf_um_prov.alias("Adms_Prov_Sk_Lookup"),
        F.col("Extract.ADMS_PROV_SK") == F.col("Adms_Prov_Sk_Lookup.PROV_SK"),
        "left",
    )
    .join(
        df_hf_um_prov.alias("Atnd_Prov_Sk_Lookup"),
        F.col("Extract.ATND_PROV_SK") == F.col("Atnd_Prov_Sk_Lookup.PROV_SK"),
        "left",
    )
    .join(
        df_hf_um_prov.alias("Fclty_Prov_Sk_Lookup"),
        F.col("Extract.FCLTY_PROV_SK") == F.col("Fclty_Prov_Sk_Lookup.PROV_SK"),
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
        df_hf_um_prov.alias("Surgeon_Prov_Sk_Lookup"),
        F.col("Extract.SURGEON_PROV_SK") == F.col("Surgeon_Prov_Sk_Lookup.PROV_SK"),
        "left",
    )
    .join(
        df_hf_um_app_user.alias("Denied_User_Sk_Lookup"),
        F.col("Extract.DENIED_USER_SK") == F.col("Denied_User_Sk_Lookup.USER_SK"),
        "left",
    )
    .join(
        df_hf_um_app_user.alias("Inpt_User_Sk_Lookup"),
        F.col("Extract.INPT_USER_SK") == F.col("Inpt_User_Sk_Lookup.USER_SK"),
        "left",
    )
    .join(
        df_hf_um_diag_cd.alias("Adms_Pri_Diag_Cd_Sk_Lookup"),
        F.col("Extract.ADMS_PRI_DIAG_CD_SK")
        == F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD_SK"),
        "left",
    )
    .join(
        df_hf_um_diag_cd.alias("Pri_Diag_Cd_Sk_Lookup"),
        F.col("Extract.PRI_DIAG_CD_SK") == F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_SK"),
        "left",
    )
    .join(
        df_hf_um_proc_cd.alias("refProcCd"),
        F.col("Extract.PRI_SURG_PROC_CD_SK") == F.col("refProcCd.PROC_CD_SK"),
        "left",
    )
    .join(
        df_hf_um_diag_cd.alias("dschg_lookup"),
        F.col("Extract.DSCHG_DIAG_CD_SK") == F.col("dschg_lookup.DIAG_CD_SK"),
        "left",
    )
)

df_final = df_br.select(
    F.col("Extract.UM_SK").alias("UM_SK"),
    F.when(
        F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Src_Sys_Cd_Sk_Lookup.TRGT_CD"))
    .alias("SRC_SYS_CD"),
    F.col("Extract.UM_REF_ID").alias("UM_REF_ID"),
    rpad(F.lit(CurrRunDt), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.lit(CurrRunDt), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Extract.ADMS_PRI_DIAG_CD_SK").alias("ADMS_PRI_DIAG_CD_SK"),
    F.col("Extract.ADMS_PROV_SK").alias("ADMS_PROV_SK"),
    F.col("Extract.ATND_PROV_SK").alias("ATND_PROV_SK"),
    F.when(
        F.col("Um_Lookup.CASE_MGT_SK").isNull()
        | (F.length(trim(F.col("Um_Lookup.CASE_MGT_SK"))) == 0),
        F.lit("1"),
    )
    .otherwise(F.col("Um_Lookup.CASE_MGT_SK"))
    .alias("CASE_MGT_SK"),
    F.when(
        F.col("Um_Lookup.CRT_USER_SK").isNull()
        | (F.length(trim(F.col("Um_Lookup.CRT_USER_SK"))) == 0),
        F.lit("1"),
    )
    .otherwise(F.col("Um_Lookup.CRT_USER_SK"))
    .alias("CRT_USER_SK"),
    F.col("Extract.DENIED_USER_SK").alias("DENIED_USER_SK"),
    F.col("Extract.DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    F.col("Extract.FCLTY_PROV_SK").alias("FCLTY_PROV_SK"),
    F.when(
        F.col("Um_Lookup.GRP_SK").isNull() | (F.length(trim(F.col("Um_Lookup.GRP_SK"))) == 0),
        F.lit("1"),
    )
    .otherwise(F.col("Um_Lookup.GRP_SK"))
    .alias("GRP_SK"),
    F.col("Extract.PRI_DIAG_CD_SK").alias("IP_PRI_DIAG_CD_SK"),
    F.col("Extract.INPT_USER_SK").alias("INPT_USER_SK"),
    F.when(
        F.col("Um_Lookup.MBR_SK").isNull() | (F.length(trim(F.col("Um_Lookup.MBR_SK"))) == 0),
        F.lit("0"),
    )
    .otherwise(F.col("Um_Lookup.MBR_SK"))
    .alias("MBR_SK"),
    F.col("Extract.PCP_PROV_SK").alias("PCP_PROV_SK"),
    F.when(
        F.col("Um_Lookup.PRI_RESP_USER_SK").isNull()
        | (F.length(trim(F.col("Um_Lookup.PRI_RESP_USER_SK"))) == 0),
        F.lit("1"),
    )
    .otherwise(F.col("Um_Lookup.PRI_RESP_USER_SK"))
    .alias("PRI_RESP_USER_SK"),
    F.col("Extract.PRI_SURG_PROC_CD_SK").alias("PRI_SURG_PROC_CD_SK"),
    F.when(
        F.col("Um_Lookup.PROD_SK").isNull() | (F.length(trim(F.col("Um_Lookup.PROD_SK"))) == 0),
        F.lit("1"),
    )
    .otherwise(F.col("Um_Lookup.PROD_SK"))
    .alias("PROD_SK"),
    F.col("Extract.PROV_AGMNT_SK").alias("PROV_AGMNT_SK"),
    F.col("Extract.RQST_PROV_SK").alias("RQST_PROV_SK"),
    F.when(
        F.col("Um_Lookup.SUBGRP_SK").isNull()
        | (F.length(trim(F.col("Um_Lookup.SUBGRP_SK"))) == 0),
        F.lit("1"),
    )
    .otherwise(F.col("Um_Lookup.SUBGRP_SK"))
    .alias("SUBGRP_SK"),
    F.when(
        F.col("Um_Lookup.SUB_SK").isNull() | (F.length(trim(F.col("Um_Lookup.SUB_SK"))) == 0),
        F.lit("1"),
    )
    .otherwise(F.col("Um_Lookup.SUB_SK"))
    .alias("SUB_SK"),
    F.col("Extract.SURGEON_PROV_SK").alias("SURGEON_PROV_SK"),
    F.when(
        F.col("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_ADMS_TREAT_CAT_CD"),
    F.when(
        F.col("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Adms_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_ADMS_TREAT_CAT_NM"),
    F.when(
        F.col("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_AUTH_POS_CAT_CD"),
    F.when(
        F.col("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Auth_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_AUTH_POS_CAT_NM"),
    F.when(
        F.col("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_AUTH_POS_TYP_CD"),
    F.when(
        F.col("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Auth_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_AUTH_POS_TYP_NM"),
    F.when(
        F.col("Um_Ip_Care_Typ_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Care_Typ_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Care_Typ_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_CARE_TYP_CD"),
    F.when(
        F.col("Um_Ip_Care_Typ_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Care_Typ_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Care_Typ_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_CARE_TYP_NM"),
    F.when(
        F.col("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_CUR_TREAT_CAT_CD"),
    F.when(
        F.col("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Cur_Treat_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_CUR_TREAT_CAT_NM"),
    F.when(
        F.col("Um_Ip_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_DENIAL_RSN_CD"),
    F.when(
        F.col("Um_Ip_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Denial_Rsn_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_DENIAL_RSN_NM"),
    F.when(
        F.col("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_DSCHG_STTUS_CD"),
    F.when(
        F.col("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Dschg_Sttus_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_DSCHG_STTUS_NM"),
    F.when(
        F.col("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_FCLTY_NTWK_STTUS_CD"),
    F.when(
        F.col("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Fclty_Ntwk_Sttus_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_FCLTY_NTWK_STTUS_NM"),
    F.when(
        F.col("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_RQST_POS_CAT_CD"),
    F.when(
        F.col("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Rqst_Pos_Cat_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_RQST_POS_CAT_NM"),
    F.when(
        F.col("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_RQST_POS_TYP_CD"),
    F.when(
        F.col("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Rqst_Pos_Typ_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_RQST_POS_TYP_NM"),
    F.when(
        F.col("Um_Ip_Sttus_Cd_Sk_Lookup.TRGT_CD").isNull()
        | (F.length(trim(F.col("Um_Ip_Sttus_Cd_Sk_Lookup.TRGT_CD"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Sttus_Cd_Sk_Lookup.TRGT_CD"))
    .alias("UM_IP_STTUS_CD"),
    F.when(
        F.col("Um_Ip_Sttus_Cd_Sk_Lookup.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("Um_Ip_Sttus_Cd_Sk_Lookup.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Ip_Sttus_Cd_Sk_Lookup.TRGT_CD_NM"))
    .alias("UM_IP_STTUS_NM"),
    rpad(F.col("Extract.PREAUTH_IN"), 1, " ").alias("UM_IP_PREAUTH_IN"),
    rpad(F.col("Extract.RFRL_IN"), 1, " ").alias("UM_IP_RFRL_IN"),
    rpad(F.col("Extract.RQST_PROV_PCP_IN"), 1, " ").alias("UM_IP_RQST_PROV_PCP_IN"),
    F.when(
        F.col("Um_Lookup.MED_MGT_NOTE_DTM").isNull()
        | (F.length(F.col("Um_Lookup.MED_MGT_NOTE_DTM")) == 0),
        F.lit("1753-01-01 00:00:00.000000"),
    )
    .otherwise(F.col("Um_Lookup.MED_MGT_NOTE_DTM"))
    .alias("MED_MGT_NOTE_DTM"),
    rpad(F.col("Extract.ACTL_ADMS_DT_SK"), 10, " ").alias("UM_IP_ACTL_ADMS_DT_SK"),
    rpad(F.col("Extract.AUTH_ADMS_DT_SK"), 10, " ").alias("UM_IP_AUTH_ADMS_DT_SK"),
    rpad(F.col("Extract.UM_IP_AUTH_SURG_DT_SK"), 10, " ").alias("UM_IP_AUTH_SURG_DT_SK"),
    rpad(F.col("Extract.AUTH_DT_SK"), 10, " ").alias("UM_IP_AUTH_DT_SK"),
    F.col("Extract.ATCHMT_SRC_DTM").alias("UM_IP_ATCHMT_SRC_DTM"),
    rpad(F.col("Extract.DENIAL_DT_SK"), 10, " ").alias("UM_IP_DENIAL_DT_SK"),
    rpad(
        F.date_format(F.col("Extract.DSCHG_DTM"), "yyyy-MM-dd"), 10, " "
    ).alias("UM_IP_DSCHG_DT_SK"),
    F.col("Extract.DSCHG_DTM").alias("UM_IP_DSCHG_DTM"),
    F.col("Extract.XPCT_DSCHG_DT_SK").alias("UM_IP_XPCT_DSCHG_DT"),
    rpad(F.col("Extract.INPT_DT_SK"), 10, " ").alias("UM_IP_INPT_DT_SK"),
    rpad(F.col("Extract.NEXT_RVW_DT_SK"), 10, " ").alias("UM_IP_NEXT_RVW_DT_SK"),
    rpad(F.col("Extract.RCVD_DT_SK"), 10, " ").alias("UM_IP_RCVD_DT_SK"),
    rpad(F.col("Extract.RQST_ADMS_DT_SK"), 10, " ").alias("UM_IP_RQST_ADMS_DT_SK"),
    rpad(F.col("Extract.UM_IP_RQST_SURG_DT_SK"), 10, " ").alias("UM_IP_RQST_SURG_DT_SK"),
    rpad(F.col("Extract.STTUS_DT_SK"), 10, " ").alias("UM_IP_STTUS_DT_SK"),
    F.col("Extract.ACTL_LOS_DAYS_QTY").alias("UM_IP_ACTL_LOS_DAYS_QTY"),
    F.col("Extract.ALW_TOT_LOS_DAYS_QTY").alias("UM_IP_ALW_TOT_LOS_DAYS_QTY"),
    F.col("Extract.AUTH_TOT_LOS_DAYS_QTY").alias("UM_IP_AUTH_TOT_LOS_DAYS_QTY"),
    F.col("Extract.BRTH_WT_QTY").alias("UM_IP_BRTH_WT_QTY"),
    F.col("Extract.MBR_AGE").alias("UM_IP_MBR_AGE"),
    F.col("Extract.PRI_GOAL_END_LOS_DAYS_QTY").alias("UM_IP_PRI_GOAL_END_LOS_QTY"),
    F.col("Extract.PRI_GOAL_STRT_LOS_DAYS_QTY").alias("UM_IP_PRI_GOAL_STRT_LOS_QTY"),
    F.col("Extract.RQST_PREOP_DAYS_QTY").alias("UM_IP_RQST_PREOP_DAYS_QTY"),
    F.col("Extract.RQST_TOT_LOS_DAYS_QTY").alias("UM_IP_RQST_TOT_LOS_DAYS_QTY"),
    F.when(
        F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD").isNull()
        | (F.length(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD"))
    .alias("ADMS_PRI_DIAG_CD"),
    F.when(
        F.col("Adms_Prov_Sk_Lookup.PROV_ID").isNull()
        | (F.length(F.col("Adms_Prov_Sk_Lookup.PROV_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Adms_Prov_Sk_Lookup.PROV_ID"))
    .alias("ADMS_PROV_ID"),
    F.when(
        F.col("Atnd_Prov_Sk_Lookup.PROV_ID").isNull()
        | (F.length(F.col("Atnd_Prov_Sk_Lookup.PROV_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Atnd_Prov_Sk_Lookup.PROV_ID"))
    .alias("ATND_PROV_ID"),
    F.when(
        F.col("Um_Lookup.CRT_USER_ID").isNull()
        | (F.length(F.col("Um_Lookup.CRT_USER_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Lookup.CRT_USER_ID"))
    .alias("CRT_USER_ID"),
    F.when(
        F.col("Denied_User_Sk_Lookup.USER_ID").isNull()
        | (F.length(F.col("Denied_User_Sk_Lookup.USER_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Denied_User_Sk_Lookup.USER_ID"))
    .alias("DENIED_USER_ID"),
    F.col("Extract.DSALW_EXCD_ID").alias("DSALW_EXCD_ID"),
    F.when(
        F.col("Fclty_Prov_Sk_Lookup.PROV_ID").isNull()
        | (F.length(F.col("Fclty_Prov_Sk_Lookup.PROV_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Fclty_Prov_Sk_Lookup.PROV_ID"))
    .alias("FCLTY_PROV_ID"),
    F.when(
        F.col("Um_Lookup.GRP_ID").isNull() | (F.length(F.col("Um_Lookup.GRP_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Lookup.GRP_ID"))
    .alias("GRP_ID"),
    F.when(
        F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD").isNull()
        | (F.length(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD"))
    .alias("IP_PRI_DIAG_CD"),
    F.when(
        F.col("Inpt_User_Sk_Lookup.USER_ID").isNull()
        | (F.length(F.col("Inpt_User_Sk_Lookup.USER_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Inpt_User_Sk_Lookup.USER_ID"))
    .alias("INPT_USER_ID"),
    F.when(
        F.col("Um_Lookup.MBR_UNIQ_KEY").isNull()
        | (F.length(F.col("Um_Lookup.MBR_UNIQ_KEY")) == 0),
        F.lit("0"),
    )
    .otherwise(F.col("Um_Lookup.MBR_UNIQ_KEY"))
    .alias("MBR_UNIQ_KEY"),
    F.when(
        F.col("Pcp_Prov_Sk_Lookup.PROV_ID").isNull()
        | (F.length(F.col("Pcp_Prov_Sk_Lookup.PROV_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Pcp_Prov_Sk_Lookup.PROV_ID"))
    .alias("PCP_PROV_ID"),
    F.when(
        F.col("Um_Lookup.PRI_RESP_USER_ID").isNull()
        | (F.length(F.col("Um_Lookup.PRI_RESP_USER_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Lookup.PRI_RESP_USER_ID"))
    .alias("PRI_RESP_USER_ID"),
    F.col("Extract.PROC_CD").alias("PRI_SURG_PROC_CD"),
    rpad(F.col("Extract.PRI_SURG_PROC_CD_MOD_TX"), 2, " ").alias("PRI_SURG_PROC_CD_MOD_TX"),
    F.when(
        F.col("Um_Lookup.PROD_ID").isNull() | (F.length(F.col("Um_Lookup.PROD_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Lookup.PROD_ID"))
    .alias("PROD_ID"),
    F.when(
        F.col("Rqst_Prov_Sk_Lookup.PROV_ID").isNull()
        | (F.length(F.col("Rqst_Prov_Sk_Lookup.PROV_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Rqst_Prov_Sk_Lookup.PROV_ID"))
    .alias("RQST_PROV_ID"),
    F.when(
        F.col("Um_Lookup.SUBGRP_ID").isNull()
        | (F.length(F.col("Um_Lookup.SUBGRP_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Um_Lookup.SUBGRP_ID"))
    .alias("SUBGRP_ID"),
    F.when(
        F.col("Um_Lookup.SUB_UNIQ_KEY").isNull()
        | (F.length(F.col("Um_Lookup.SUB_UNIQ_KEY")) == 0),
        F.lit("0"),
    )
    .otherwise(F.col("Um_Lookup.SUB_UNIQ_KEY"))
    .alias("SUB_UNIQ_KEY"),
    F.when(
        F.col("Surgeon_Prov_Sk_Lookup.PROV_ID").isNull()
        | (F.length(F.col("Surgeon_Prov_Sk_Lookup.PROV_ID")) == 0),
        F.lit("NA"),
    )
    .otherwise(F.col("Surgeon_Prov_Sk_Lookup.PROV_ID"))
    .alias("SURGEON_PROV_ID"),
    F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.UM_IP_ADMS_TREAT_CAT_CD_SK").alias("UM_IP_ADMS_TREAT_CAT_CD_SK"),
    F.col("Extract.UM_IP_AUTH_POS_CAT_CD_SK").alias("UM_IP_AUTH_POS_CAT_CD_SK"),
    F.col("Extract.UM_IP_AUTH_POS_TYP_CD_SK").alias("UM_IP_AUTH_POS_TYP_CD_SK"),
    F.col("Extract.UM_IP_CARE_TYP_CD_SK").alias("UM_IP_CARE_TYP_CD_SK"),
    F.col("Extract.UM_IP_CUR_TREAT_CAT_CD_SK").alias("UM_IP_CUR_TREAT_CAT_CD_SK"),
    F.col("Extract.UM_IP_DENIAL_RSN_CD_SK").alias("UM_IP_DENIAL_RSN_CD_SK"),
    F.col("Extract.UM_IP_DSCHG_STTUS_CD_SK").alias("UM_IP_DSCHG_STTUS_CD_SK"),
    F.col("Extract.UM_IP_FCLTY_NTWK_STTUS_CD_SK").alias("UM_IP_FCLTY_NTWK_STTUS_CD_SK"),
    F.col("Extract.UM_IP_RQST_POS_CAT_CD_SK").alias("UM_IP_RQST_POS_CAT_CD_SK"),
    F.col("Extract.UM_IP_RQST_POS_TYP_CD_SK").alias("UM_IP_RQST_POS_TYP_CD_SK"),
    F.col("Extract.UM_IP_STTUS_CD_SK").alias("UM_IP_STTUS_CD_SK"),
    F.when(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD").isNull(), F.lit("UNK"))
    .otherwise(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD"))
    .alias("IP_PRI_DIAG_CD_TYP_CD"),
    F.when(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM").isNull(), F.lit("UNK"))
    .otherwise(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM"))
    .alias("IP_PRI_DIAG_CD_TYP_NM"),
    F.when(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK").isNull(), F.lit("0"))
    .otherwise(F.col("Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK"))
    .alias("IP_PRI_DIAG_CD_TYP_CD_SK"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD").isNull(), F.lit("UNK"))
    .otherwise(F.col("refProcCd.PROC_CD_TYP_CD"))
    .alias("PRI_SURG_PROC_CD_TYP_CD"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD_NM").isNull(), F.lit("UNK"))
    .otherwise(F.col("refProcCd.PROC_CD_TYP_CD_NM"))
    .alias("PRI_SURG_PROC_CD_TYP_NM"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD_SK").isNull(), F.lit("0"))
    .otherwise(F.col("refProcCd.PROC_CD_TYP_CD_SK"))
    .alias("PRI_SURG_PROC_CD_TYP_CD_SK"),
    F.when(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD").isNull(), F.lit("UNK"))
    .otherwise(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD"))
    .alias("ADMS_PRI_DIAG_CD_TYP_CD"),
    F.when(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM").isNull(), F.lit("UNK"))
    .otherwise(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_NM"))
    .alias("ADMS_PRI_DIAG_CD_TYP_NM"),
    F.when(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK").isNull(), F.lit("0"))
    .otherwise(F.col("Adms_Pri_Diag_Cd_Sk_Lookup.DIAG_CD_TYP_CD_SK"))
    .alias("ADMS_PRI_DIAG_CD_TYP_CD_SK"),
    F.when(
        F.col("dschg_lookup.DIAG_CD_SK").isNull(), F.lit("NA")
    ).otherwise(F.col("dschg_lookup.DIAG_CD")).alias("DSCHG_DIAG_CD"),
    F.col("Extract.DSCHG_DIAG_CD_SK").alias("DSCHG_DIAG_CD_SK"),
    F.when(
        F.col("dschg_lookup.DIAG_CD_SK").isNull(), F.lit("NA")
    ).otherwise(F.col("dschg_lookup.DIAG_CD_TYP_CD")).alias("DSCHG_DIAG_CD_TYP_CD"),
    F.when(
        F.col("dschg_lookup.DIAG_CD_SK").isNull(), F.lit("NA")
    ).otherwise(F.col("dschg_lookup.DIAG_CD_TYP_CD_NM")).alias("DSCHG_DIAG_CD_TYP_NM"),
    F.when(
        F.col("dschg_lookup.DIAG_CD_SK").isNull(), F.lit("NA")
    ).otherwise(F.col("dschg_lookup.DIAG_CD_TYP_CD_SK")).alias("DSCHG_DIAG_CD_TYP_CD_SK"),
)

write_files(
    df_final,
    f"{adls_path}/load/UM_IP_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)
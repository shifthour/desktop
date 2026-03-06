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
# MAGIC JOB NAME:     IdsDataMartMedMgtDmUmIpExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                 Pulls data from IDS to create medical management data mart  Extract is based on BeginRunCycle.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - UM_IP
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
# MAGIC                     direct update of MED_MGT_DM_UM_IP
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen  03/22/2005 -   Originally Programmed
# MAGIC               Tao Luo          06/23/2006 -    Added Run Cycle Logic and Joins to UM

# MAGIC Use SK's to perform all mappings with any Lookup MappingType.
# MAGIC Transform and output link names are used by job control to get link count.
# MAGIC Remove NullToZero's
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval (including creation of corresponding secret_name parameters)
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','0')
BeginRunCycle = get_widget_value('BeginRunCycle','30')

# --------------------------------------------------------------------------------
# STAGE: IDSUmIPSet (DB2Connector)
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_idsumipset = (
    f"SELECT UM_IP.UM_SK as UM_SK,UM_IP.SRC_SYS_CD_SK as SRC_SYS_CD_SK,UM_IP.UM_REF_ID as UM_REF_ID,"
    f"UM_IP.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,UM_IP.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,"
    f"UM_IP.ADMS_PRI_DIAG_CD_SK as ADMS_PRI_DIAG_CD_SK,UM_IP.ADMS_PROV_SK as ADMS_PROV_SK,UM_IP.ATND_PROV_SK as ATND_PROV_SK,"
    f"UM_IP.DENIED_USER_SK as DENIED_USER_SK,UM_IP.DSALW_EXCD_SK as DSALW_EXCD_SK,UM_IP.FCLTY_PROV_SK as FCLTY_PROV_SK,"
    f"UM_IP.INPT_USER_SK as INPT_USER_SK,UM_IP.PCP_PROV_SK as PCP_PROV_SK,UM_IP.PRI_DIAG_CD_SK as PRI_DIAG_CD_SK,"
    f"UM_IP.PRI_SURG_PROC_CD_SK as PRI_SURG_PROC_CD_SK,UM_IP.PROV_AGMNT_SK as PROV_AGMNT_SK,UM_IP.RQST_PROV_SK as RQST_PROV_SK,"
    f"UM_IP.SURGEON_PROV_SK as SURGEON_PROV_SK,UM_IP.UM_IP_ADMS_TREAT_CAT_CD_SK as UM_IP_ADMS_TREAT_CAT_CD_SK,"
    f"UM_IP.UM_IP_AUTH_POS_CAT_CD_SK as UM_IP_AUTH_POS_CAT_CD_SK,UM_IP.UM_IP_AUTH_POS_TYP_CD_SK as UM_IP_AUTH_POS_TYP_CD_SK,"
    f"UM_IP.UM_IP_CARE_TYP_CD_SK as UM_IP_CARE_TYP_CD_SK,UM_IP.UM_IP_CUR_TREAT_CAT_CD_SK as UM_IP_CUR_TREAT_CAT_CD_SK,"
    f"UM_IP.UM_IP_DENIAL_RSN_CD_SK as UM_IP_DENIAL_RSN_CD_SK,UM_IP.UM_IP_DSCHG_STTUS_CD_SK as UM_IP_DSCHG_STTUS_CD_SK,"
    f"UM_IP.UM_IP_FCLTY_NTWK_STTUS_CD_SK as UM_IP_FCLTY_NTWK_STTUS_CD_SK,UM_IP.UM_IP_RQST_POS_CAT_CD_SK as UM_IP_RQST_POS_CAT_CD_SK,"
    f"UM_IP.UM_IP_RQST_POS_TYP_CD_SK as UM_IP_RQST_POS_TYP_CD_SK,UM_IP.UM_IP_STTUS_CD_SK as UM_IP_STTUS_CD_SK,"
    f"UM_IP.PREAUTH_IN as PREAUTH_IN,UM_IP.RFRL_IN as RFRL_IN,UM_IP.RQST_PROV_PCP_IN as RQST_PROV_PCP_IN,"
    f"UM_IP.ACTL_ADMS_DT_SK as ACTL_ADMS_DT_SK,UM_IP.ATCHMT_SRC_DTM as ATCHMT_SRC_DTM,UM_IP.AUTH_ADMS_DT_SK as AUTH_ADMS_DT_SK,"
    f"UM_IP.AUTH_DT_SK as AUTH_DT_SK,UM_IP.DENIAL_DT_SK as DENIAL_DT_SK,UM_IP.DSCHG_DTM as DSCHG_DTM,"
    f"UM_IP.XPCT_DSCHG_DT_SK as XPCT_DSCHG_DT_SK,UM_IP.INPT_DT_SK as INPT_DT_SK,UM_IP.RCVD_DT_SK as RCVD_DT_SK,"
    f"UM_IP.RQST_ADMS_DT_SK as RQST_ADMS_DT_SK,UM_IP.NEXT_RVW_DT_SK as NEXT_RVW_DT_SK,UM_IP.STTUS_DT_SK as STTUS_DT_SK,"
    f"UM_IP.UM_IP_RQST_SURG_DT_SK as UM_IP_RQST_SURG_DT_SK,UM_IP.UM_IP_AUTH_SURG_DT_SK as UM_IP_AUTH_SURG_DT_SK,"
    f"UM_IP.ACTL_LOS_DAYS_QTY as ACTL_LOS_DAYS_QTY,UM_IP.ALW_TOT_LOS_DAYS_QTY as ALW_TOT_LOS_DAYS_QTY,"
    f"UM_IP.AUTH_TOT_LOS_DAYS_QTY as AUTH_TOT_LOS_DAYS_QTY,UM_IP.BRTH_WT_QTY as BRTH_WT_QTY,UM_IP.MBR_AGE as MBR_AGE,"
    f"UM_IP.PRI_GOAL_END_LOS_DAYS_QTY as PRI_GOAL_END_LOS_DAYS_QTY,UM_IP.PRI_GOAL_STRT_LOS_DAYS_QTY as PRI_GOAL_STRT_LOS_DAYS_QTY,"
    f"UM_IP.RQST_PREOP_DAYS_QTY as RQST_PREOP_DAYS_QTY,UM_IP.RQST_TOT_LOS_DAYS_QTY as RQST_TOT_LOS_DAYS_QTY,"
    f"UM_IP.PRI_SURG_PROC_CD_MOD_TX as PRI_SURG_PROC_CD_MOD_TX "
    f"FROM {IDSOwner}.UM_IP UM_IP, {IDSOwner}.UM UM "
    f"WHERE UM_IP.UM_REF_ID = UM.UM_REF_ID "
    f"  AND UM_IP.SRC_SYS_CD_SK = UM.SRC_SYS_CD_SK "
    f"  AND UM_IP.LAST_UPDT_RUN_CYC_EXCTN_SK >= {BeginRunCycle} "
    f"  AND UM_IP.UM_SK NOT IN (0,1)"
)
df_lnkUmIP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_idsumipset)
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: hf_etrnl_cd_mppng (CHashedFileStage) => Scenario C => read as parquet
# --------------------------------------------------------------------------------
schema_hf_etrnl_cd_mppng = StructType([
    StructField("CD_MPPNG_SK", IntegerType(), True),
    StructField("TRGT_CD", StringType(), True),
    StructField("TRGT_CD_NM", StringType(), True),
    StructField("SRC_CD", StringType(), True),
    StructField("SRC_CD_NM", StringType(), True)
])
df_hf_etrnl_cd_mppng = spark.read.schema(schema_hf_etrnl_cd_mppng).parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# --------------------------------------------------------------------------------
# STAGE: IDSDiag (DB2Connector) with multiple reference queries "WHERE ... = ?"
# => Use staging table + join approach
# --------------------------------------------------------------------------------
# 1) Create STAGING table for primary link data
jdbc_url_ids_diag, jdbc_props_ids_diag = get_db_config(ids_secret_name)
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp",
    jdbc_url_ids_diag,
    jdbc_props_ids_diag
)
(
    df_lnkUmIP
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option("dbtable", "STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp")
    .mode("overwrite")
    .save()
)

# 2) Build lookups for each reference link:
# lnkDiag => DIAG_CD
df_diag = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT d.DIAG_CD_SK AS DIAG_CD_SK_lnkDiag, d.DIAG_CD AS DIAG_CD_lnkDiag "
        f"FROM {IDSOwner}.DIAG_CD d "
        f"JOIN STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp t "
        f"ON d.DIAG_CD_SK = t.ADMS_PRI_DIAG_CD_SK"
    )
    .load()
)

# lnkExCd => EXCD
df_excd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT x.EXCD_SK AS EXCD_SK_lnkExCd, x.EXCD_ID AS EXCD_ID_lnkExCd "
        f"FROM {IDSOwner}.EXCD x "
        f"JOIN STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp t "
        f"ON x.EXCD_SK = t.DSALW_EXCD_SK"
    )
    .load()
)

# lnkProv => PROV
df_prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT p.PROV_SK AS PROV_SK_lnkProv, p.PROV_ID AS PROV_ID_lnkProv "
        f"FROM {IDSOwner}.PROV p "
        f"JOIN STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp t "
        f"ON p.PROV_SK = t.FCLTY_PROV_SK"
    )
    .load()
)

# lnkDiag2 => DIAG_CD
df_diag2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT d.DIAG_CD_SK AS DIAG_CD_SK_lnkDiag2, d.DIAG_CD AS DIAG_CD_lnkDiag2 "
        f"FROM {IDSOwner}.DIAG_CD d "
        f"JOIN STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp t "
        f"ON d.DIAG_CD_SK = t.PRI_DIAG_CD_SK"
    )
    .load()
)

# lnkProc => PROC_CD
df_proc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT pr.PROC_CD_SK AS PROC_CD_SK_lnkProc, pr.PROC_CD AS PROC_CD_lnkProc "
        f"FROM {IDSOwner}.PROC_CD pr "
        f"JOIN STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp t "
        f"ON pr.PROC_CD_SK = t.PRI_SURG_PROC_CD_SK"
    )
    .load()
)

# lnkProv2 => PROV
df_prov2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT p.PROV_SK AS PROV_SK_lnkProv2, p.PROV_ID AS PROV_ID_lnkProv2 "
        f"FROM {IDSOwner}.PROV p "
        f"JOIN STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp t "
        f"ON p.PROV_SK = t.RQST_PROV_SK"
    )
    .load()
)

# lnkAppUser => APP_USER
df_appuser = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_diag)
    .options(**jdbc_props_ids_diag)
    .option(
        "query",
        f"SELECT u.USER_SK AS USER_SK_lnkAppUser, u.USER_ID AS USER_ID_lnkAppUser "
        f"FROM {IDSOwner}.APP_USER u "
        f"JOIN STAGING.IdsDataMartMedMgtDmUmIpExtr_IDSDiag_temp t "
        f"ON u.USER_SK = t.DENIED_USER_SK"
    )
    .load()
)

# --------------------------------------------------------------------------------
# STAGE: Trans1 (CTransformerStage)
# Perform the multiple left joins from hashed file references + IDSDiag references.
# Then apply the output column logic.
# --------------------------------------------------------------------------------

# 1) Join with all hashed-file references from df_hf_etrnl_cd_mppng for each link
#    We'll reuse df_hf_etrnl_cd_mppng by aliasing multiple times with left joins.

df_refSrcSys = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refSrcSys"),
    F.col("TRGT_CD").alias("TRGT_CD_refSrcSys"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refSrcSys"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refSrcSys")

df_refAdmsTreat = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refAdmsTreat"),
    F.col("TRGT_CD").alias("TRGT_CD_refAdmsTreat"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refAdmsTreat"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refAdmsTreat")

df_refAuthPosCatCd = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refAuthPosCatCd"),
    F.col("TRGT_CD").alias("TRGT_CD_refAuthPosCatCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refAuthPosCatCd"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refAuthPosCatCd")

df_refAuthPosTypCd = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refAuthPosTypCd"),
    F.col("TRGT_CD").alias("TRGT_CD_refAuthPosTypCd"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refAuthPosTypCd"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refAuthPosTypCd")

df_refCareTyp = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refCareTyp"),
    F.col("TRGT_CD").alias("TRGT_CD_refCareTyp"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refCareTyp"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refCareTyp")

df_refCurTreat = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refCurTreat"),
    F.col("TRGT_CD").alias("TRGT_CD_refCurTreat"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refCurTreat"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refCurTreat")

df_refDenialRsn = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refDenialRsn"),
    F.col("TRGT_CD").alias("TRGT_CD_refDenialRsn"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refDenialRsn"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refDenialRsn")

df_refRqstPosCat = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refRqstPosCat"),
    F.col("TRGT_CD").alias("TRGT_CD_refRqstPosCat"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refRqstPosCat"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refRqstPosCat")

df_refRqstPosTyp = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refRqstPosTyp"),
    F.col("TRGT_CD").alias("TRGT_CD_refRqstPosTyp"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refRqstPosTyp"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refRqstPosTyp")

df_refSttus = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK_refSttus"),
    F.col("TRGT_CD").alias("TRGT_CD_refSttus"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM_refSttus"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM")
).alias("refSttus")

df_enriched = df_lnkUmIP.alias("lnkUmIP")

df_enriched = df_enriched.join(
    df_refSrcSys,
    (trim(F.col("lnkUmIP.SRC_SYS_CD_SK")) == F.col("refSrcSys.CD_MPPNG_SK_refSrcSys")),
    "left"
)
df_enriched = df_enriched.join(
    df_refAdmsTreat,
    (trim(F.col("lnkUmIP.UM_IP_ADMS_TREAT_CAT_CD_SK")) == F.col("refAdmsTreat.CD_MPPNG_SK_refAdmsTreat")),
    "left"
)
df_enriched = df_enriched.join(
    df_refAuthPosCatCd,
    (trim(F.col("lnkUmIP.UM_IP_AUTH_POS_CAT_CD_SK")) == F.col("refAuthPosCatCd.CD_MPPNG_SK_refAuthPosCatCd")),
    "left"
)
df_enriched = df_enriched.join(
    df_refAuthPosTypCd,
    (trim(F.col("lnkUmIP.UM_IP_AUTH_POS_TYP_CD_SK")) == F.col("refAuthPosTypCd.CD_MPPNG_SK_refAuthPosTypCd")),
    "left"
)
df_enriched = df_enriched.join(
    df_refCareTyp,
    (trim(F.col("lnkUmIP.UM_IP_CARE_TYP_CD_SK")) == F.col("refCareTyp.CD_MPPNG_SK_refCareTyp")),
    "left"
)
df_enriched = df_enriched.join(
    df_refCurTreat,
    (trim(F.col("lnkUmIP.UM_IP_CUR_TREAT_CAT_CD_SK")) == F.col("refCurTreat.CD_MPPNG_SK_refCurTreat")),
    "left"
)
df_enriched = df_enriched.join(
    df_refDenialRsn,
    (trim(F.col("lnkUmIP.UM_IP_DENIAL_RSN_CD_SK")) == F.col("refDenialRsn.CD_MPPNG_SK_refDenialRsn")),
    "left"
)
df_enriched = df_enriched.join(
    df_refRqstPosCat,
    (trim(F.col("lnkUmIP.UM_IP_RQST_POS_CAT_CD_SK")) == F.col("refRqstPosCat.CD_MPPNG_SK_refRqstPosCat")),
    "left"
)
df_enriched = df_enriched.join(
    df_refRqstPosTyp,
    (trim(F.col("lnkUmIP.UM_IP_RQST_POS_TYP_CD_SK")) == F.col("refRqstPosTyp.CD_MPPNG_SK_refRqstPosTyp")),
    "left"
)
df_enriched = df_enriched.join(
    df_refSttus,
    (trim(F.col("lnkUmIP.UM_IP_STTUS_CD_SK")) == F.col("refSttus.CD_MPPNG_SK_refSttus")),
    "left"
)

# 2) Join with the 7 dataframes from IDSDiag references
df_enriched = df_enriched.join(
    df_diag.alias("lnkDiag"),
    (F.col("lnkUmIP.ADMS_PRI_DIAG_CD_SK") == F.col("lnkDiag.DIAG_CD_SK_lnkDiag")),
    "left"
)
df_enriched = df_enriched.join(
    df_excd.alias("lnkExCd"),
    (F.col("lnkUmIP.DSALW_EXCD_SK") == F.col("lnkExCd.EXCD_SK_lnkExCd")),
    "left"
)
df_enriched = df_enriched.join(
    df_prov.alias("lnkProv"),
    (F.col("lnkUmIP.FCLTY_PROV_SK") == F.col("lnkProv.PROV_SK_lnkProv")),
    "left"
)
df_enriched = df_enriched.join(
    df_diag2.alias("lnkDiag2"),
    (F.col("lnkUmIP.PRI_DIAG_CD_SK") == F.col("lnkDiag2.DIAG_CD_SK_lnkDiag2")),
    "left"
)
df_enriched = df_enriched.join(
    df_proc.alias("lnkProc"),
    (F.col("lnkUmIP.PRI_SURG_PROC_CD_SK") == F.col("lnkProc.PROC_CD_SK_lnkProc")),
    "left"
)
df_enriched = df_enriched.join(
    df_prov2.alias("lnkProv2"),
    (F.col("lnkUmIP.RQST_PROV_SK") == F.col("lnkProv2.PROV_SK_lnkProv2")),
    "left"
)
df_enriched = df_enriched.join(
    df_appuser.alias("lnkAppUser"),
    (F.col("lnkUmIP.DENIED_USER_SK") == F.col("lnkAppUser.USER_SK_lnkAppUser")),
    "left"
)

# 3) Apply the transformation logic for output columns
#    Using DataStage expressions to handle 'NA'/'UNK' => null, etc.
#    Also handle FORMAT.DATE(DSCHG_DTM, 'DB2','TIMESTAMP','SYBTIMESTAMP') with a placeholder user-defined function if needed.

df_enriched = df_enriched.withColumn(
    "UM_IP_ACTL_ADMS_DT",
    F.when(
        (trim(F.col("ACTL_ADMS_DT_SK")) == "NA") | (trim(F.col("ACTL_ADMS_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("ACTL_ADMS_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_AUTH_ADMS_DT",
    F.when(
        (trim(F.col("AUTH_ADMS_DT_SK")) == "NA") | (trim(F.col("AUTH_ADMS_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("AUTH_ADMS_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_AUTH_SURG_DT",
    F.when(
        (trim(F.col("UM_IP_AUTH_SURG_DT_SK")) == "NA") | (trim(F.col("UM_IP_AUTH_SURG_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("UM_IP_AUTH_SURG_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_XPCT_DSCHG_DT",
    F.when(
        (trim(F.col("XPCT_DSCHG_DT_SK")) == "NA") | (trim(F.col("XPCT_DSCHG_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("XPCT_DSCHG_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_DENIAL_DT",
    F.when(
        (trim(F.col("DENIAL_DT_SK")) == "NA") | (trim(F.col("DENIAL_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("DENIAL_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_DSCHG_DTM",
    F.when(
        (trim(F.col("DSCHG_DTM")) == "NA") | (trim(F.col("DSCHG_DTM")) == "UNK"),
        F.lit(None)
    ).otherwise(
        # Assuming "FORMAT.DATE(...)" is some user-defined function. We just call it:
        FORMAT_DATE(F.col("DSCHG_DTM"), 'DB2', 'TIMESTAMP', 'SYBTIMESTAMP')  
    )
)

df_enriched = df_enriched.withColumn(
    "UM_IP_RQST_ADMS_DT",
    F.when(
        (trim(F.col("RQST_ADMS_DT_SK")) == "NA") | (trim(F.col("RQST_ADMS_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("RQST_ADMS_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_RQST_SURG_DT",
    F.when(
        (trim(F.col("UM_IP_RQST_SURG_DT_SK")) == "NA") | (trim(F.col("UM_IP_RQST_SURG_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("UM_IP_RQST_SURG_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_NEXT_RVW_DT",
    F.when(
        (trim(F.col("NEXT_RVW_DT_SK")) == "NA") | (trim(F.col("NEXT_RVW_DT_SK")) == "UNK"),
        F.lit(None)
    ).otherwise(F.col("NEXT_RVW_DT_SK"))
)

df_enriched = df_enriched.withColumn(
    "UM_IP_DSCHG_DTM",
    F.col("UM_IP_DSCHG_DTM").cast(TimestampType())
)

# Build final columns using the output-expression mapping
df_final = df_enriched.select(
    # 1) SRC_SYS_CD (char/varchar => length unknown => use rpad(...,<...>,' '))
    F.rpad(F.col("refSrcSys.TRGT_CD_refSrcSys"), <...>, " ").alias("SRC_SYS_CD"),
    # 2) UM_REF_ID
    F.rpad(F.col("lnkUmIP.UM_REF_ID"), <...>, " ").alias("UM_REF_ID"),
    # 3) UM_IP_ADMS_TREAT_CAT_CD
    F.rpad(F.col("refAdmsTreat.TRGT_CD_refAdmsTreat"), <...>, " ").alias("UM_IP_ADMS_TREAT_CAT_CD"),
    # 4) UM_IP_ADMS_TREAT_CAT_NM
    F.rpad(F.col("refAdmsTreat.TRGT_CD_NM_refAdmsTreat"), <...>, " ").alias("UM_IP_ADMS_TREAT_CAT_NM"),
    # 5) UM_IP_AUTH_POS_CAT_CD
    F.rpad(F.col("refAuthPosCatCd.TRGT_CD_refAuthPosCatCd"), <...>, " ").alias("UM_IP_AUTH_POS_CAT_CD"),
    # 6) UM_IP_AUTH_POS_CAT_NM
    F.rpad(F.col("refAuthPosCatCd.TRGT_CD_NM_refAuthPosCatCd"), <...>, " ").alias("UM_IP_AUTH_POS_CAT_NM"),
    # 7) UM_IP_AUTH_POS_TYP_CD
    F.rpad(F.col("refAuthPosTypCd.TRGT_CD_refAuthPosTypCd"), <...>, " ").alias("UM_IP_AUTH_POS_TYP_CD"),
    # 8) UM_IP_AUTH_POS_TYP_NM
    F.rpad(F.col("refAuthPosTypCd.TRGT_CD_NM_refAuthPosTypCd"), <...>, " ").alias("UM_IP_AUTH_POS_TYP_NM"),
    # 9) UM_IP_CARE_TYP_CD
    F.rpad(F.col("refCareTyp.TRGT_CD_refCareTyp"), <...>, " ").alias("UM_IP_CARE_TYP_CD"),
    # 10) UM_IP_CARE_TYP_NM
    F.rpad(F.col("refCareTyp.TRGT_CD_NM_refCareTyp"), <...>, " ").alias("UM_IP_CARE_TYP_NM"),
    # 11) UM_IP_CUR_TREAT_CAT_CD
    F.rpad(F.col("refCurTreat.TRGT_CD_refCurTreat"), <...>, " ").alias("UM_IP_CUR_TREAT_CAT_CD"),
    # 12) UM_IP_CUR_TREAT_CAT_NM
    F.rpad(F.col("refCurTreat.TRGT_CD_NM_refCurTreat"), <...>, " ").alias("UM_IP_CUR_TREAT_CAT_NM"),
    # 13) UM_IP_DENIAL_RSN_CD
    F.rpad(F.col("refDenialRsn.TRGT_CD_refDenialRsn"), <...>, " ").alias("UM_IP_DENIAL_RSN_CD"),
    # 14) UM_IP_DENIAL_RSN_NM
    F.rpad(F.col("refDenialRsn.TRGT_CD_NM_refDenialRsn"), <...>, " ").alias("UM_IP_DENIAL_RSN_NM"),
    # 15) UM_IP_RQST_POS_CAT_CD
    F.rpad(F.col("refRqstPosCat.TRGT_CD_refRqstPosCat"), <...>, " ").alias("UM_IP_RQST_POS_CAT_CD"),
    # 16) UM_IP_RQST_POS_CAT_NM
    F.rpad(F.col("refRqstPosCat.TRGT_CD_NM_refRqstPosCat"), <...>, " ").alias("UM_IP_RQST_POS_CAT_NM"),
    # 17) UM_IP_RQST_POS_TYP_CD
    F.rpad(F.col("refRqstPosTyp.TRGT_CD_refRqstPosTyp"), <...>, " ").alias("UM_IP_RQST_POS_TYP_CD"),
    # 18) UM_IP_RQST_POS_TYP_NM
    F.rpad(F.col("refRqstPosTyp.TRGT_CD_NM_refRqstPosTyp"), <...>, " ").alias("UM_IP_RQST_POS_TYP_NM"),
    # 19) UM_IP_STTUS_CD
    F.rpad(F.col("refSttus.TRGT_CD_refSttus"), <...>, " ").alias("UM_IP_STTUS_CD"),
    # 20) UM_IP_STTUS_NM
    F.rpad(F.col("refSttus.TRGT_CD_NM_refSttus"), <...>, " ").alias("UM_IP_STTUS_NM"),
    # 21) UM_IP_RFRL_IN (char(1))
    F.rpad(F.col("lnkUmIP.RFRL_IN"), 1, " ").alias("UM_IP_RFRL_IN"),
    # 22) UM_IP_PREAUTH_IN (char(1))
    F.rpad(F.col("lnkUmIP.PREAUTH_IN"), 1, " ").alias("UM_IP_PREAUTH_IN"),
    # 23) UM_IP_ACTL_ADMS_DT
    F.col("UM_IP_ACTL_ADMS_DT"),
    # 24) UM_IP_AUTH_ADMS_DT
    F.col("UM_IP_AUTH_ADMS_DT"),
    # 25) UM_IP_AUTH_SURG_DT
    F.col("UM_IP_AUTH_SURG_DT"),
    # 26) UM_IP_XPCT_DSCHG_DT
    F.col("UM_IP_XPCT_DSCHG_DT"),
    # 27) UM_IP_DENIAL_DT
    F.col("UM_IP_DENIAL_DT"),
    # 28) UM_IP_DSCHG_DTM
    F.col("UM_IP_DSCHG_DTM"),
    # 29) UM_IP_RQST_ADMS_DT
    F.col("UM_IP_RQST_ADMS_DT"),
    # 30) UM_IP_RQST_SURG_DT
    F.col("UM_IP_RQST_SURG_DT"),
    # 31) UM_IP_NEXT_RVW_DT
    F.col("UM_IP_NEXT_RVW_DT"),
    # 32) UM_IP_ACTL_LOS_DAYS_QTY
    F.col("ACTL_LOS_DAYS_QTY"),
    # 33) UM_IP_ALW_TOT_LOS_DAYS_QTY
    F.col("ALW_TOT_LOS_DAYS_QTY"),
    # 34) UM_IP_AUTH_TOT_LOS_DAYS_QTY
    F.col("AUTH_TOT_LOS_DAYS_QTY"),
    # 35) UM_IP_BRTH_WT_QTY
    F.col("BRTH_WT_QTY"),
    # 36) UM_IP_RQST_PREOP_DAYS_QTY
    F.col("RQST_PREOP_DAYS_QTY"),
    # 37) UM_IP_RQST_TOT_LOS_DAYS_QTY
    F.col("RQST_TOT_LOS_DAYS_QTY"),
    # 38) ADMS_PRI_DIAG_CD
    F.col("lnkDiag.DIAG_CD_lnkDiag").alias("ADMS_PRI_DIAG_CD"),
    # 39) DSALW_EXCD_ID
    F.col("lnkExCd.EXCD_ID_lnkExCd").alias("DSALW_EXCD_ID"),
    # 40) FCLTY_PROV_ID
    F.expr("CASE WHEN lnkProv.PROV_ID_lnkProv IS NULL THEN 0 ELSE lnkProv.PROV_ID_lnkProv END").alias("FCLTY_PROV_ID"),
    # 41) IP_PRI_DIAG_CD
    F.col("lnkDiag2.DIAG_CD_lnkDiag2").alias("IP_PRI_DIAG_CD"),
    # 42) PRI_SURG_PROC_CD
    F.col("lnkProc.PROC_CD_lnkProc").alias("PRI_SURG_PROC_CD"),
    # 43) RQST_PROV_ID
    F.expr("CASE WHEN lnkProv2.PROV_ID_lnkProv2 IS NULL THEN 0 ELSE lnkProv2.PROV_ID_lnkProv2 END").alias("RQST_PROV_ID"),
    # 44) UM_IP_DENIAL_USER_ID
    F.col("lnkAppUser.USER_ID_lnkAppUser").alias("UM_IP_DENIAL_USER_ID"),
    # 45) LAST_UPDT_RUN_CYC_NO
    F.rpad(F.lit(CurrRunCycle), <...>, " ").alias("LAST_UPDT_RUN_CYC_NO")
)

# --------------------------------------------------------------------------------
# STAGE: MED_MGT_DM_UM_IP (CODBCStage) => Merge (Upsert) into #$ClmMartOwner#.MED_MGT_DM_UM_IP
# --------------------------------------------------------------------------------
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDataMartMedMgtDmUmIpExtr_MED_MGT_DM_UM_IP_temp",
    jdbc_url_clmmart,
    jdbc_props_clmmart
)

(
    df_final
    .write
    .format("jdbc")
    .option("url", jdbc_url_clmmart)
    .options(**jdbc_props_clmmart)
    .option("dbtable", "STAGING.IdsDataMartMedMgtDmUmIpExtr_MED_MGT_DM_UM_IP_temp")
    .mode("overwrite")
    .save()
)

merge_sql = (
    f"MERGE INTO {ClmMartOwner}.MED_MGT_DM_UM_IP as T "
    f"USING STAGING.IdsDataMartMedMgtDmUmIpExtr_MED_MGT_DM_UM_IP_temp as S "
    f"ON T.SRC_SYS_CD=S.SRC_SYS_CD AND T.UM_REF_ID=S.UM_REF_ID "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.UM_IP_ADMS_TREAT_CAT_CD=S.UM_IP_ADMS_TREAT_CAT_CD, "
    f"T.UM_IP_ADMS_TREAT_CAT_NM=S.UM_IP_ADMS_TREAT_CAT_NM, "
    f"T.UM_IP_AUTH_POS_CAT_CD=S.UM_IP_AUTH_POS_CAT_CD, "
    f"T.UM_IP_AUTH_POS_CAT_NM=S.UM_IP_AUTH_POS_CAT_NM, "
    f"T.UM_IP_AUTH_POS_TYP_CD=S.UM_IP_AUTH_POS_TYP_CD, "
    f"T.UM_IP_AUTH_POS_TYP_NM=S.UM_IP_AUTH_POS_TYP_NM, "
    f"T.UM_IP_CARE_TYP_CD=S.UM_IP_CARE_TYP_CD, "
    f"T.UM_IP_CARE_TYP_NM=S.UM_IP_CARE_TYP_NM, "
    f"T.UM_IP_CUR_TREAT_CAT_CD=S.UM_IP_CUR_TREAT_CAT_CD, "
    f"T.UM_IP_CUR_TREAT_CAT_NM=S.UM_IP_CUR_TREAT_CAT_NM, "
    f"T.UM_IP_DENIAL_RSN_CD=S.UM_IP_DENIAL_RSN_CD, "
    f"T.UM_IP_DENIAL_RSN_NM=S.UM_IP_DENIAL_RSN_NM, "
    f"T.UM_IP_RQST_POS_CAT_CD=S.UM_IP_RQST_POS_CAT_CD, "
    f"T.UM_IP_RQST_POS_CAT_NM=S.UM_IP_RQST_POS_CAT_NM, "
    f"T.UM_IP_RQST_POS_TYP_CD=S.UM_IP_RQST_POS_TYP_CD, "
    f"T.UM_IP_RQST_POS_TYP_NM=S.UM_IP_RQST_POS_TYP_NM, "
    f"T.UM_IP_STTUS_CD=S.UM_IP_STTUS_CD, "
    f"T.UM_IP_STTUS_NM=S.UM_IP_STTUS_NM, "
    f"T.UM_IP_RFRL_IN=S.UM_IP_RFRL_IN, "
    f"T.UM_IP_PREAUTH_IN=S.UM_IP_PREAUTH_IN, "
    f"T.UM_IP_ACTL_ADMS_DT=S.UM_IP_ACTL_ADMS_DT, "
    f"T.UM_IP_AUTH_ADMS_DT=S.UM_IP_AUTH_ADMS_DT, "
    f"T.UM_IP_AUTH_SURG_DT=S.UM_IP_AUTH_SURG_DT, "
    f"T.UM_IP_XPCT_DSCHG_DT=S.UM_IP_XPCT_DSCHG_DT, "
    f"T.UM_IP_DENIAL_DT=S.UM_IP_DENIAL_DT, "
    f"T.UM_IP_DSCHG_DTM=S.UM_IP_DSCHG_DTM, "
    f"T.UM_IP_RQST_ADMS_DT=S.UM_IP_RQST_ADMS_DT, "
    f"T.UM_IP_RQST_SURG_DT=S.UM_IP_RQST_SURG_DT, "
    f"T.UM_IP_NEXT_RVW_DT=S.UM_IP_NEXT_RVW_DT, "
    f"T.UM_IP_ACTL_LOS_DAYS_QTY=S.UM_IP_ACTL_LOS_DAYS_QTY, "
    f"T.UM_IP_ALW_TOT_LOS_DAYS_QTY=S.UM_IP_ALW_TOT_LOS_DAYS_QTY, "
    f"T.UM_IP_AUTH_TOT_LOS_DAYS_QTY=S.UM_IP_AUTH_TOT_LOS_DAYS_QTY, "
    f"T.UM_IP_BRTH_WT_QTY=S.UM_IP_BRTH_WT_QTY, "
    f"T.UM_IP_RQST_PREOP_DAYS_QTY=S.UM_IP_RQST_PREOP_DAYS_QTY, "
    f"T.UM_IP_RQST_TOT_LOS_DAYS_QTY=S.UM_IP_RQST_TOT_LOS_DAYS_QTY, "
    f"T.ADMS_PRI_DIAG_CD=S.ADMS_PRI_DIAG_CD, "
    f"T.DSALW_EXCD_ID=S.DSALW_EXCD_ID, "
    f"T.FCLTY_PROV_ID=S.FCLTY_PROV_ID, "
    f"T.IP_PRI_DIAG_CD=S.IP_PRI_DIAG_CD, "
    f"T.PRI_SURG_PROC_CD=S.PRI_SURG_PROC_CD, "
    f"T.RQST_PROV_ID=S.RQST_PROV_ID, "
    f"T.UM_IP_DENIAL_USER_ID=S.UM_IP_DENIAL_USER_ID, "
    f"T.LAST_UPDT_RUN_CYC_NO=S.LAST_UPDT_RUN_CYC_NO "
    f"WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, UM_REF_ID, UM_IP_ADMS_TREAT_CAT_CD, UM_IP_ADMS_TREAT_CAT_NM, "
    f"UM_IP_AUTH_POS_CAT_CD, UM_IP_AUTH_POS_CAT_NM, UM_IP_AUTH_POS_TYP_CD, UM_IP_AUTH_POS_TYP_NM, "
    f"UM_IP_CARE_TYP_CD, UM_IP_CARE_TYP_NM, UM_IP_CUR_TREAT_CAT_CD, UM_IP_CUR_TREAT_CAT_NM, "
    f"UM_IP_DENIAL_RSN_CD, UM_IP_DENIAL_RSN_NM, UM_IP_RQST_POS_CAT_CD, UM_IP_RQST_POS_CAT_NM, "
    f"UM_IP_RQST_POS_TYP_CD, UM_IP_RQST_POS_TYP_NM, UM_IP_STTUS_CD, UM_IP_STTUS_NM, "
    f"UM_IP_RFRL_IN, UM_IP_PREAUTH_IN, UM_IP_ACTL_ADMS_DT, UM_IP_AUTH_ADMS_DT, "
    f"UM_IP_AUTH_SURG_DT, UM_IP_XPCT_DSCHG_DT, UM_IP_DENIAL_DT, UM_IP_DSCHG_DTM, UM_IP_RQST_ADMS_DT, "
    f"UM_IP_RQST_SURG_DT, UM_IP_NEXT_RVW_DT, UM_IP_ACTL_LOS_DAYS_QTY, UM_IP_ALW_TOT_LOS_DAYS_QTY, "
    f"UM_IP_AUTH_TOT_LOS_DAYS_QTY, UM_IP_BRTH_WT_QTY, UM_IP_RQST_PREOP_DAYS_QTY, UM_IP_RQST_TOT_LOS_DAYS_QTY, "
    f"ADMS_PRI_DIAG_CD, DSALW_EXCD_ID, FCLTY_PROV_ID, IP_PRI_DIAG_CD, PRI_SURG_PROC_CD, RQST_PROV_ID, "
    f"UM_IP_DENIAL_USER_ID, LAST_UPDT_RUN_CYC_NO) "
    f"VALUES ( S.SRC_SYS_CD, S.UM_REF_ID, S.UM_IP_ADMS_TREAT_CAT_CD, S.UM_IP_ADMS_TREAT_CAT_NM, "
    f"S.UM_IP_AUTH_POS_CAT_CD, S.UM_IP_AUTH_POS_CAT_NM, S.UM_IP_AUTH_POS_TYP_CD, S.UM_IP_AUTH_POS_TYP_NM, "
    f"S.UM_IP_CARE_TYP_CD, S.UM_IP_CARE_TYP_NM, S.UM_IP_CUR_TREAT_CAT_CD, S.UM_IP_CUR_TREAT_CAT_NM, "
    f"S.UM_IP_DENIAL_RSN_CD, S.UM_IP_DENIAL_RSN_NM, S.UM_IP_RQST_POS_CAT_CD, S.UM_IP_RQST_POS_CAT_NM, "
    f"S.UM_IP_RQST_POS_TYP_CD, S.UM_IP_RQST_POS_TYP_NM, S.UM_IP_STTUS_CD, S.UM_IP_STTUS_NM, "
    f"S.UM_IP_RFRL_IN, S.UM_IP_PREAUTH_IN, S.UM_IP_ACTL_ADMS_DT, S.UM_IP_AUTH_ADMS_DT, "
    f"S.UM_IP_AUTH_SURG_DT, S.UM_IP_XPCT_DSCHG_DT, S.UM_IP_DENIAL_DT, S.UM_IP_DSCHG_DTM, S.UM_IP_RQST_ADMS_DT, "
    f"S.UM_IP_RQST_SURG_DT, S.UM_IP_NEXT_RVW_DT, S.UM_IP_ACTL_LOS_DAYS_QTY, S.UM_IP_ALW_TOT_LOS_DAYS_QTY, "
    f"S.UM_IP_AUTH_TOT_LOS_DAYS_QTY, S.UM_IP_BRTH_WT_QTY, S.UM_IP_RQST_PREOP_DAYS_QTY, S.UM_IP_RQST_TOT_LOS_DAYS_QTY, "
    f"S.ADMS_PRI_DIAG_CD, S.DSALW_EXCD_ID, S.FCLTY_PROV_ID, S.IP_PRI_DIAG_CD, S.PRI_SURG_PROC_CD, S.RQST_PROV_ID, "
    f"S.UM_IP_DENIAL_USER_ID, S.LAST_UPDT_RUN_CYC_NO"
    f");"
)
execute_dml(merge_sql, jdbc_url_clmmart, jdbc_props_clmmart)
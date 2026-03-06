# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  FctsIdsWorkCompClmSttusAuditExtr
# MAGIC CALLED BY:         FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM_STTUS_AUDIT table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-01-10\(9)5628 WORK_COMPNSTN_CLM_LN \(9)    Original Programming\(9)\(9)\(9)          IntegrateDev2                          Kalyan Neelam         2017-02-23
# MAGIC                                                                 (Facets to IDS) ETL Report
# MAGIC 
# MAGIC Akhila Manickavelu\(9)  2017-05-09\(9)5628 WORK_COMPNSTN_CLM \(9)    Reversal Logic updated\(9)\(9)\(9)          Integratedev2                             
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5\(9)Ken Bradmon\(9)2022-06-03

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file WORK_COMPNSTN_CLM_STTUS_AUDIT.dat to be send to IDS WORK_COMPNSTN_CLM_STTUS_AUDIT table
# MAGIC Join CMC_CLST_STATUS, CMC_MEME_MEMBER and CMC_GRGR_GROUP  for master extract and Look up on P_SEL_PRCS_CRITR for GRGR_ID
# MAGIC B_WORK_COMPNSTN_CLM_STTUS_AUDIT.dat load file have the key fields from WORK_COMPNSTN_CLM_STTUS_AUDIT for Balancing Report
# MAGIC get the field CRT_RUN_CYC_EXCTN_SK from 
# MAGIC WORK_COMPNSTN_CLM_STTUS_AUDIT
# MAGIC Reversal Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
WorkCompOwner = get_widget_value('WorkCompOwner','')
workcomp_secret_name = get_widget_value('workcomp_secret_name','')
RunCycle = get_widget_value('RunCycle','')
DrivTable = get_widget_value('DrivTable','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

# 1) WorkCompnstnClmSttusAudit (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_WorkCompnstnClmSttusAudit = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CLM_ID, CLM_STTUS_AUDIT_SEQ_NO, CRT_RUN_CYC_EXCTN_SK FROM {WorkCompOwner}.WORK_COMPNSTN_CLM_STTUS_AUDIT WHERE SRC_SYS_CD = 'FACETS'"
    )
    .load()
)

# 2) Ds_ReversalClaim (PxDataSet) => read from parquet
df_Ds_ReversalClaim = spark.read.parquet(f"{adls_path}/ds/Clm_Reversals.parquet")

# 3) FacetsDB_Input (ODBCConnectorPX)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
df_FacetsDB_Input = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT DISTINCT CMC_CLST_STATUS.CLCL_ID, CMC_CLST_STATUS.CLST_SEQ_NO, CMC_CLST_STATUS.CLST_STS, CMC_CLST_STATUS.CLMI_ITS_CUR_STS, CMC_CLST_STATUS.USUS_ID, CMC_CLST_STATUS.CLST_STS_DTM, CMC_CLST_STATUS.CLST_MCTR_REAS, CMC_CLST_STATUS.CLST_USID_ROUTE FROM {FacetsOwner}.CMC_CLST_STATUS CMC_CLST_STATUS INNER JOIN tempdb..{DrivTable} Drvr ON Drvr.CLM_ID=CMC_CLST_STATUS.CLCL_ID"
    )
    .load()
)

# 4) CD_MPPNG (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT DISTINCT SRC_CD, CD_MPPNG_SK, SRC_DOMAIN_NM, TRGT_DOMAIN_NM FROM {IDSOwner}.CD_MPPNG WHERE SRC_SYS_CD = 'FACETS' AND SRC_CLCTN_CD = 'FACETS DBO' AND SRC_DOMAIN_NM IN ('CLAIM STATUS REASON','CLAIM STATUS','TRANSMISSION STATUS') AND TRGT_CLCTN_CD = 'IDS' AND TRGT_DOMAIN_NM IN ('CLAIM STATUS REASON','CLAIM STATUS','TRANSMISSION STATUS')"
    )
    .load()
)

# 5) Flt_CdMppng (PxFilter)
df_Flt_CdMppng_SttusReason = df_CD_MPPNG.filter(
    "SRC_DOMAIN_NM = 'CLAIM STATUS REASON' AND TRGT_DOMAIN_NM = 'CLAIM STATUS REASON'"
)
df_Flt_CdMppng_Sttus = df_CD_MPPNG.filter(
    "SRC_DOMAIN_NM = 'CLAIM STATUS' AND TRGT_DOMAIN_NM = 'CLAIM STATUS'"
)
df_Flt_CdMppng_Transmission = df_CD_MPPNG.filter(
    "SRC_DOMAIN_NM = 'TRANSMISSION STATUS' AND TRGT_DOMAIN_NM = 'TRANSMISSION STATUS'"
)

df_SttusReason = df_Flt_CdMppng_SttusReason.select(
    F.col("SRC_CD"),
    F.col("CD_MPPNG_SK")
)
df_Sttus = df_Flt_CdMppng_Sttus.select(
    F.col("SRC_CD"),
    F.col("CD_MPPNG_SK")
)
df_Transmission = df_Flt_CdMppng_Transmission.select(
    F.col("SRC_CD"),
    F.col("CD_MPPNG_SK")
)

# 6) APP_USER (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_APP_USER = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT DISTINCT APP_USER.USER_ID, APP_USER.USER_SK FROM {IDSOwner}.APP_USER APP_USER WHERE APP_USER.SRC_SYS_CD_SK='{SrcSysCdSk}'"
    )
    .load()
)

# 7) Cpy_AppUser (PxCopy)
df_AppUser2 = df_APP_USER.select(
    F.col("USER_ID").alias("USER_ID"),
    F.col("USER_SK").alias("USER_SK")
)
df_AppUser1 = df_APP_USER.select(
    F.col("USER_ID").alias("USER_ID"),
    F.col("USER_SK").alias("USER_SK")
)

# 8) Lkp_twoDBs (PxLookup) - primary link df_FacetsDB_Input, lookups: Sttus, Transmission, SttusReason, AppUser1, AppUser2, WorkCompnstnClmSttusAudit, Ds_ReversalClaim
df_Lkp_twoDBs_temp = (
    df_FacetsDB_Input.alias("Lnk_Facets")
    .join(df_Sttus.alias("Sttus"), F.col("Lnk_Facets.CLST_STS") == F.col("Sttus.SRC_CD"), "left")
    .join(
        df_Transmission.alias("Transmission"),
        F.col("Lnk_Facets.CLMI_ITS_CUR_STS") == F.col("Transmission.SRC_CD"),
        "left"
    )
    .join(
        df_SttusReason.alias("SttusReason"),
        F.col("Lnk_Facets.CLST_MCTR_REAS") == F.col("SttusReason.SRC_CD"),
        "left"
    )
    .join(
        df_AppUser1.alias("AppUser1"),
        F.col("Lnk_Facets.USUS_ID") == F.col("AppUser1.USER_ID"),
        "left"
    )
    .join(
        df_AppUser2.alias("AppUser2"),
        F.col("Lnk_Facets.CLST_USID_ROUTE") == F.col("AppUser2.USER_ID"),
        "left"
    )
    .join(
        df_WorkCompnstnClmSttusAudit.alias("Lnk_WorkCompClmSttusAudit"),
        (
            (F.col("Lnk_Facets.CLCL_ID") == F.col("Lnk_WorkCompClmSttusAudit.CLM_ID"))
            & (F.col("Lnk_Facets.CLST_SEQ_NO") == F.col("Lnk_WorkCompClmSttusAudit.CLM_STTUS_AUDIT_SEQ_NO"))
        ),
        "left"
    )
    .join(
        df_Ds_ReversalClaim.alias("Lnk_ReversalDs"),
        F.col("Lnk_Facets.CLCL_ID") == F.col("Lnk_ReversalDs.CLCL_ID"),
        "left"
    )
)
df_Lkp_twoDBs = df_Lkp_twoDBs_temp.select(
    F.col("Lnk_Facets.CLCL_ID").alias("CLCL_ID"),
    F.col("Lnk_Facets.CLST_SEQ_NO").alias("CLST_SEQ_NO"),
    F.col("Sttus.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("Transmission.CD_MPPNG_SK").alias("CD_MPPNG_SK_1"),
    F.col("SttusReason.CD_MPPNG_SK").alias("CD_MPPNG_SK_2"),
    F.col("AppUser1.USER_SK").alias("USER_SK"),
    F.col("AppUser2.USER_SK").alias("USER_SK1"),
    F.col("Lnk_Facets.CLST_STS_DTM").alias("CLST_STS_DTM"),
    F.col("Lnk_WorkCompClmSttusAudit.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_ReversalDs.CLCL_ID").alias("CLCL_ID_Reverse")
)

# 9) Xfm_ (CTransformerStage) => split into df_all and df_reverseRecord
df_all = df_Lkp_twoDBs.select(
    F.col("CLCL_ID").alias("CLCL_ID"),
    F.col("CLST_SEQ_NO").alias("CLST_SEQ_NO"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("CD_MPPNG_SK_1").alias("CD_MPPNG_SK_1"),
    F.col("CD_MPPNG_SK_2").alias("CD_MPPNG_SK_2"),
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_SK1").alias("USER_SK1"),
    F.col("CLST_STS_DTM").alias("CLST_STS_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_reverse_temp = df_Lkp_twoDBs.filter(
    (F.col("CLCL_ID_Reverse").isNotNull()) & (trim(F.col("CLCL_ID_Reverse")) != '')
)

df_reverseRecord = df_reverse_temp.select(
    (trim(F.col("CLCL_ID")) + F.lit("R")).alias("CLCL_ID"),
    F.col("CLST_SEQ_NO").alias("CLST_SEQ_NO"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("CD_MPPNG_SK_1").alias("CD_MPPNG_SK_1"),
    F.col("CD_MPPNG_SK_2").alias("CD_MPPNG_SK_2"),
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_SK1").alias("USER_SK1"),
    F.col("CLST_STS_DTM").alias("CLST_STS_DTM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# 10) fnl_All_Reverse (PxFunnel) => union of df_all and df_reverseRecord
df_fnl_All_Reverse = df_all.unionByName(df_reverseRecord).select(
    "CLCL_ID",
    "CLST_SEQ_NO",
    "CD_MPPNG_SK",
    "CD_MPPNG_SK_1",
    "CD_MPPNG_SK_2",
    "USER_SK",
    "USER_SK1",
    "CLST_STS_DTM",
    "CRT_RUN_CYC_EXCTN_SK"
)

# 11) BusinessLogic (CTransformerStage)
df_Lnk_WorkCompClmSttusAudit = df_fnl_All_Reverse.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLST_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.when(F.col("CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(RunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("USER_SK").isNull(), F.lit(1))
     .when(trim(F.col("USER_SK")) == '', F.lit(1))
     .otherwise(F.col("USER_SK"))
     .alias("CRT_BY_APP_USER_SK"),
    F.when(F.col("USER_SK1").isNull(), F.lit(1))
     .when(trim(F.col("USER_SK1")) == '', F.lit(1))
     .otherwise(F.col("USER_SK1"))
     .alias("ROUT_TO_APP_USER_SK"),
    F.when(F.col("CD_MPPNG_SK_2").isNull() | (F.col("CD_MPPNG_SK_2") == 0), F.lit(1))
     .otherwise(F.col("CD_MPPNG_SK_2"))
     .alias("CLM_STTUS_CHG_RSN_CD_SK"),
    F.when(F.col("CD_MPPNG_SK").isNull() | (F.col("CD_MPPNG_SK") == 0), F.lit(1))
     .otherwise(F.col("CD_MPPNG_SK"))
     .alias("CLM_STTUS_CD_SK"),
    F.when(F.col("CD_MPPNG_SK_1").isNull() | (F.col("CD_MPPNG_SK_1") == 0), F.lit(1))
     .otherwise(F.col("CD_MPPNG_SK_1"))
     .alias("TRNSMSN_SRC_CD_SK"),
    F.col("CLST_STS_DTM").alias("CLM_STTUS_DTM")
)

df_Lnk_WorkCompnstnClmSttusAuditBalReport = df_fnl_All_Reverse.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLST_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD")
)

# 12) WORK_COMPNSTN_CLM_STTUS_AUDIT (PxSequentialFile) => write
df_Lnk_WorkCompClmSttusAudit_final = df_Lnk_WorkCompClmSttusAudit.select(
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_STTUS_AUDIT_SEQ_NO"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_BY_APP_USER_SK"),
    F.col("ROUT_TO_APP_USER_SK"),
    F.col("CLM_STTUS_CHG_RSN_CD_SK"),
    F.col("CLM_STTUS_CD_SK"),
    F.col("TRNSMSN_SRC_CD_SK"),
    F.col("CLM_STTUS_DTM")
)
write_files(
    df_Lnk_WorkCompClmSttusAudit_final,
    f"{adls_path}/load/WORK_COMPNSTN_CLM_STTUS_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 13) B_WORK_COMPNSTN_CLM_STTUS_AUDIT (PxSequentialFile) => write
df_Lnk_WorkCompnstnClmSttusAuditBalReport_final = df_Lnk_WorkCompnstnClmSttusAuditBalReport.select(
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_STTUS_AUDIT_SEQ_NO"),
    F.col("SRC_SYS_CD")
)
write_files(
    df_Lnk_WorkCompnstnClmSttusAuditBalReport_final,
    f"{adls_path}/load/B_WORK_COMPNSTN_CLM_STTUS_AUDIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)
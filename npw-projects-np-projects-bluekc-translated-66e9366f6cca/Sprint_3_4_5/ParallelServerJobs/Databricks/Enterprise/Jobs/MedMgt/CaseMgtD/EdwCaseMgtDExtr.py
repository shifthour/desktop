# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 1;hf_pri_case_mgr_user_id
# MAGIC 
# MAGIC JOB NAME:     EdwCaseMgtDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS CASE_MGT to flatfile CASE_MGT_D.dat
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC The following IDS tables are used:
# MAGIC CASE_MGT
# MAGIC MBR
# MAGIC DIAG_CD
# MAGIC GRP
# MAGIC APP_USER
# MAGIC PROC_CD
# MAGIC PROD
# MAGIC SUBGRP
# MAGIC SUB
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_pri_case_mgr_user_id - Stores USER_ID's for lookup
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: CASE_MGT_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------     ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Suzanne Saylor              2/23/2006          MedMgmt/                   Original Programming.                                                                               devlEDW10
# MAGIC Bhoomi Dasari                01/17/2008          MedMgmt/                 Added LAST_UPDT_RUN_CYC_EXCTN_SK  >= ExtractRunCycle        devlEDW                 Steph Goddard             01/23/2008  
# MAGIC                                                                                                       rule based upon which we extract records now.  
# MAGIC                                                                                                       Added Hash.Clear and checked null's for lkups
# MAGIC Ralph Tucker                 5/10/2012      4896 Edw Remediation   Adding new fields for Facets 5.0:                                                            EnterpriseNewDevl
# MAGIC                                                                                                                       DIAG_CD_TYP_CD
# MAGIC                                                                                                                       DIAG_CD_TYP_NM
# MAGIC                                                                                                                       DIAG_CD_TYP_CD_SK
# MAGIC                                                                                                                       PROC_CD_TYP_CD
# MAGIC                                                                                                                       PROC_CD_TYP_NM
# MAGIC                                                                                                                       PROC_CD_TYP_CD_SK
# MAGIC Sravya Gorla                   10/02/2019     US# 140167                 Updated the datatype of MED_MGT_NOTE_DTM                               EnterpriseDev2            Jaideep Mankala         10/07/2019
# MAGIC                                                                                                       from Timestamp(26.3) to timestamp(26.6)

# MAGIC Extract IDS Data
# MAGIC Apply business logic.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------

# All helper functions are assumed to be already defined in the environment:
# get_widget_value, get_db_config, dedup_sort, write_files, trim, current_date, current_timestamp, SurrogateKeyGen, execute_dml, db_stats, strip_field, fixed_file_read_write

# PARAMETERS
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')

# JDBC CONFIG
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# READ FROM IDS (DB2Connector) - Extract
extract_query = (
    f"SELECT mgt.CASE_MGT_SK as CASE_MGT_SK, mgt.SRC_SYS_CD_SK as SRC_SYS_CD_SK, "
    f"mgt.CASE_MGT_ID as CASE_MGT_ID, mgt.DIAG_CD_SK as DIAG_CD_SK, mgt.GRP_SK as GRP_SK, "
    f"mgt.INPT_USER_SK as INPT_USER_SK, mgt.MBR_SK as MBR_SK, mgt.PRI_CASE_MGR_USER_SK as PRI_CASE_MGR_USER_SK, "
    f"mgt.PROC_CD_SK as PROC_CD_SK, mgt.PROD_SK as PROD_SK, mgt.SUBGRP_SK as SUBGRP_SK, mgt.SUB_SK as SUB_SK, "
    f"mgt.CASE_MGT_CLS_PLN_PRODCAT_CD_SK as CASE_MGT_CLS_PLN_PRODCAT_CD_SK, mgt.CASE_MGT_CMPLXTY_LVL_CD_SK as CASE_MGT_CMPLXTY_LVL_CD_SK, "
    f"mgt.CASE_MGT_ORIG_TYP_CD_SK as CASE_MGT_ORIG_TYP_CD_SK, mgt.CASE_MGT_STTUS_CD_SK as CASE_MGT_STTUS_CD_SK, "
    f"mgt.CASE_MGT_TYP_CD_SK as CASE_MGT_TYP_CD_SK, mgt.END_DT_SK as END_DT_SK, mgt.INPT_DT_SK as INPT_DT_SK, "
    f"mgt.PRI_CASE_MGR_NEXT_RVW_DT_SK as PRI_CASE_MGR_NEXT_RVW_DT_SK, mgt.STRT_DT_SK as STRT_DT_SK, mgt.STTUS_DT_SK as STTUS_DT_SK, "
    f"mgt.MED_MGT_NOTE_DTM as MED_MGT_NOTE_DTM, mgt.MBR_AGE as MBR_AGE, mgt.PRI_CNTCT_SEQ_NO as PRI_CNTCT_SEQ_NO, "
    f"mgt.STTUS_SEQ_NO as STTUS_SEQ_NO, mbr.MBR_UNIQ_KEY as MBR_UNIQ_KEY, mgt.SUM_DESC as SUM_DESC, diag.DIAG_CD as DIAG_CD, "
    f"grp.GRP_ID as GRP_ID, app.USER_ID as USER_ID, proc.PROC_CD as PROC_CD, prod.PROD_ID as PROD_ID, subgrp.SUBGRP_ID as SUBGRP_ID, "
    f"sub.SUB_UNIQ_KEY as SUB_UNIQ_KEY, mgt.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK, "
    f"mgt.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK "
    f"FROM {IDSOwner}.CASE_MGT mgt, {IDSOwner}.MBR mbr, {IDSOwner}.DIAG_CD diag, {IDSOwner}.GRP grp, "
    f"{IDSOwner}.APP_USER app, {IDSOwner}.PROC_CD proc, {IDSOwner}.PROD prod, {IDSOwner}.SUBGRP subgrp, {IDSOwner}.SUB sub "
    f"WHERE mgt.MBR_SK = mbr.MBR_SK "
    f"and mgt.DIAG_CD_SK = diag.DIAG_CD_SK "
    f"and mgt.GRP_SK = grp.GRP_SK "
    f"and mgt.INPT_USER_SK = app.USER_SK "
    f"and mgt.PROC_CD_SK = proc.PROC_CD_SK "
    f"and mgt.PROD_SK = prod.PROD_SK "
    f"and mgt.SUBGRP_SK = subgrp.SUBGRP_SK "
    f"and mgt.SUB_SK = sub.SUB_SK "
    f"and mgt.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}"
)
df_IDS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# READ FROM IDS (DB2Connector) - PriCaseMgrUserId
pricmgr_query = (
    f"SELECT mgt.PRI_CASE_MGR_USER_SK as PRI_CASE_MGR_USER_SK, app.USER_ID as USER_ID "
    f"FROM {IDSOwner}.CASE_MGT mgt, {IDSOwner}.APP_USER app "
    f"WHERE mgt.PRI_CASE_MGR_USER_SK = app.USER_SK"
)
df_IDS_PriCaseMgrUserId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", pricmgr_query)
    .load()
)

# READ FROM IDS (DB2Connector) - lodDiagCd
lodDiagCd_query = (
    f"SELECT DIAG_CD_SK, TRGT_CD as DIAG_CD_TYP_CD, TRGT_CD_NM as DIAG_CD_TYP_NM, DIAG_CD_TYP_CD_SK as DIAG_CD_TYP_CD_SK "
    f"FROM {IDSOwner}.DIAG_CD, {IDSOwner}.CD_MPPNG "
    f"WHERE DIAG_CD_TYP_CD_SK = CD_MPPNG_SK "
    f"AND SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE' "
    f"AND (SRC_CLCTN_CD = 'FACETS DBO' or DIAG_CD_SK = 0 or DIAG_CD_SK = 1)"
)
df_IDS_lodDiagCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", lodDiagCd_query)
    .load()
)

# READ FROM IDS (DB2Connector) - lodProcCd
lodProcCd_query = (
    f"SELECT PROC_CD_SK as PROC_CD_SK, TRGT_CD as PROC_CD_TYP_CD, TRGT_CD_NM as PROC_CD_TYP_NM, PROC_CD_TYP_CD_SK as PROC_CD_TYP_CD_SK "
    f"FROM {IDSOwner}.PROC_CD, {IDSOwner}.CD_MPPNG "
    f"WHERE PROC_CD_TYP_CD_SK = CD_MPPNG_SK "
    f"AND (SRC_CLCTN_CD = 'FACETS DBO' or PROC_CD_SK = 0 or PROC_CD_SK = 1)"
)
df_IDS_lodProcCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", lodProcCd_query)
    .load()
)

# SCENARIO A FOR INTERMEDIATE HASHED FILES (hf_case_mgt_diag_cd, hf_case_mgt_proc_cd, hf_pri_case_mgr_user_id)
df_hf_case_mgt_diag_cd = dedup_sort(df_IDS_lodDiagCd, ["DIAG_CD_SK"], [])
df_hf_case_mgt_proc_cd = dedup_sort(df_IDS_lodProcCd, ["PROC_CD_SK"], [])
df_hf_pri_case_mgr_user_id = dedup_sort(df_IDS_PriCaseMgrUserId, ["PRI_CASE_MGR_USER_SK"], [])

# SCENARIO C FOR hf_cdma_codes (no input link populates it, so read from parquet)
df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

# BUSINESSRULES (CTransformerStage) - chain left-joins with all lookup dataframes
df_BusinessRules = (
    df_IDS_Extract.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("refSrcSysCd"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSysCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refCaseMgtClsPlnProdCat"),
        F.col("Extract.CASE_MGT_CLS_PLN_PRODCAT_CD_SK")
        == F.col("refCaseMgtClsPlnProdCat.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refCaseMgtCmplxtyLvl"),
        F.col("Extract.CASE_MGT_CMPLXTY_LVL_CD_SK")
        == F.col("refCaseMgtCmplxtyLvl.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refCaseMgtOrigTyp"),
        F.col("Extract.CASE_MGT_ORIG_TYP_CD_SK")
        == F.col("refCaseMgtOrigTyp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refCaseMgtSttus"),
        F.col("Extract.CASE_MGT_STTUS_CD_SK")
        == F.col("refCaseMgtSttus.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refCaseMgtTyp"),
        F.col("Extract.CASE_MGT_TYP_CD_SK")
        == F.col("refCaseMgtTyp.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_pri_case_mgr_user_id.alias("lkpPriUserId"),
        F.col("Extract.PRI_CASE_MGR_USER_SK") == F.col("lkpPriUserId.PRI_CASE_MGR_USER_SK"),
        "left",
    )
    .join(
        df_hf_case_mgt_diag_cd.alias("refDiagCd"),
        F.col("Extract.DIAG_CD_SK") == F.col("refDiagCd.DIAG_CD_SK"),
        "left",
    )
    .join(
        df_hf_case_mgt_proc_cd.alias("refProcCd"),
        F.col("Extract.PROC_CD_SK") == F.col("refProcCd.PROC_CD_SK"),
        "left",
    )
)

# SELECT AND TRANSFORM COLUMNS FOR FINAL OUTPUT (CASE_MGT_D)
df_final = df_BusinessRules.select(
    F.col("Extract.CASE_MGT_SK").alias("CASE_MGT_SK"),
    F.when(
        F.col("refSrcSysCd.TRGT_CD").isNull()
        | (F.length(trim(F.col("refSrcSysCd.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refSrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Extract.CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.rpad(F.lit(CurrentDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.lit(CurrentDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Extract.DIAG_CD_SK").alias("DIAG_CD_SK"),
    F.col("Extract.GRP_SK").alias("GRP_SK"),
    F.col("Extract.INPT_USER_SK").alias("INPT_USER_SK"),
    F.col("Extract.MBR_SK").alias("MBR_SK"),
    F.col("Extract.PRI_CASE_MGR_USER_SK").alias("PRI_CASE_MGR_USER_SK"),
    F.col("Extract.PROC_CD_SK").alias("PROC_CD_SK"),
    F.col("Extract.PROD_SK").alias("PROD_SK"),
    F.col("Extract.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("Extract.SUB_SK").alias("SUB_SK"),
    F.when(
        F.col("refCaseMgtClsPlnProdCat.TRGT_CD").isNull()
        | (F.length(trim(F.col("refCaseMgtClsPlnProdCat.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtClsPlnProdCat.TRGT_CD")).alias("CASE_MGT_CLS_PLN_PROD_CAT_CD"),
    F.when(
        F.col("refCaseMgtClsPlnProdCat.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("refCaseMgtClsPlnProdCat.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtClsPlnProdCat.TRGT_CD_NM")).alias("CASE_MGT_CLS_PLN_PROD_CAT_NM"),
    F.when(
        F.col("refCaseMgtCmplxtyLvl.TRGT_CD").isNull()
        | (F.length(trim(F.col("refCaseMgtCmplxtyLvl.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtCmplxtyLvl.TRGT_CD")).alias("CASE_MGT_CMPLXTY_LVL_CD"),
    F.when(
        F.col("refCaseMgtCmplxtyLvl.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("refCaseMgtCmplxtyLvl.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtCmplxtyLvl.TRGT_CD_NM")).alias("CASE_MGT_CMPLXTY_LVL_NM"),
    F.when(
        F.col("refCaseMgtOrigTyp.TRGT_CD").isNull()
        | (F.length(trim(F.col("refCaseMgtOrigTyp.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtOrigTyp.TRGT_CD")).alias("CASE_MGT_ORIG_TYP_CD"),
    F.when(
        F.col("refCaseMgtOrigTyp.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("refCaseMgtOrigTyp.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtOrigTyp.TRGT_CD_NM")).alias("CASE_MGT_ORIG_TYP_NM"),
    F.when(
        F.col("refCaseMgtSttus.TRGT_CD").isNull()
        | (F.length(trim(F.col("refCaseMgtSttus.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtSttus.TRGT_CD")).alias("CASE_MGT_STTUS_CD"),
    F.when(
        F.col("refCaseMgtSttus.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("refCaseMgtSttus.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtSttus.TRGT_CD_NM")).alias("CASE_MGT_STTUS_NM"),
    F.when(
        F.col("refCaseMgtTyp.TRGT_CD").isNull()
        | (F.length(trim(F.col("refCaseMgtTyp.TRGT_CD"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtTyp.TRGT_CD")).alias("CASE_MGT_TYP_CD"),
    F.when(
        F.col("refCaseMgtTyp.TRGT_CD_NM").isNull()
        | (F.length(trim(F.col("refCaseMgtTyp.TRGT_CD_NM"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("refCaseMgtTyp.TRGT_CD_NM")).alias("CASE_MGT_TYP_NM"),
    F.rpad(F.col("Extract.END_DT_SK").cast(StringType()), 10, " ").alias("CASE_MGT_END_DT_SK"),
    F.rpad(F.col("Extract.INPT_DT_SK").cast(StringType()), 10, " ").alias("CASE_MGT_INPT_DT_SK"),
    F.rpad(F.col("Extract.PRI_CASE_MGR_NEXT_RVW_DT_SK").cast(StringType()), 10, " ").alias("CASE_MGT_PRI_CASE_NEXT_RVW_DT_SK"),
    F.rpad(F.col("Extract.STRT_DT_SK").cast(StringType()), 10, " ").alias("CASE_MGT_STRT_DT_SK"),
    F.rpad(F.col("Extract.STTUS_DT_SK").cast(StringType()), 10, " ").alias("CASE_MGT_STTUS_DT_SK"),
    F.col("Extract.MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("Extract.MBR_AGE").alias("CASE_MGT_MBR_AGE"),
    F.col("Extract.PRI_CNTCT_SEQ_NO").alias("CASE_MGT_PRI_CNTCT_SEQ_NO"),
    F.col("Extract.STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Extract.SUM_DESC").alias("CASE_MGT_SUM_DESC"),
    F.col("Extract.DIAG_CD").alias("DIAG_CD"),
    F.col("Extract.GRP_ID").alias("GRP_ID"),
    F.col("Extract.USER_ID").alias("INPT_USER_ID"),
    F.when(
        F.col("lkpPriUserId.USER_ID").isNull()
        | (F.length(trim(F.col("lkpPriUserId.USER_ID"))) == 0),
        F.lit("NA"),
    ).otherwise(F.col("lkpPriUserId.USER_ID")).alias("PRI_CASE_MGR_USER_ID"),
    F.col("Extract.PROC_CD").alias("PROC_CD"),
    F.col("Extract.PROD_ID").alias("PROD_ID"),
    F.col("Extract.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Extract.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Extract.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.CASE_MGT_CLS_PLN_PRODCAT_CD_SK").alias("CASEMGT_CLS_PLN_PROD_CAT_CD_SK"),
    F.col("Extract.CASE_MGT_CMPLXTY_LVL_CD_SK").alias("CASE_MGT_CMPLXTY_LVL_CD_SK"),
    F.col("Extract.CASE_MGT_ORIG_TYP_CD_SK").alias("CASE_MGT_ORIG_TYP_CD_SK"),
    F.col("Extract.CASE_MGT_STTUS_CD_SK").alias("CASE_MGT_STTUS_CD_SK"),
    F.col("Extract.CASE_MGT_TYP_CD_SK").alias("CASE_MGT_TYP_CD_SK"),
    F.when(F.col("refDiagCd.DIAG_CD_TYP_CD").isNull(), F.lit("UNK"))
    .otherwise(F.col("refDiagCd.DIAG_CD_TYP_CD"))
    .alias("DIAG_CD_TYP_CD"),
    F.when(F.col("refDiagCd.DIAG_CD_TYP_NM").isNull(), F.lit("UNKNOWN"))
    .otherwise(F.col("refDiagCd.DIAG_CD_TYP_NM"))
    .alias("DIAG_CD_TYP_NM"),
    F.when(F.col("refDiagCd.DIAG_CD_TYP_CD_SK").isNull(), F.lit(0))
    .otherwise(F.col("refDiagCd.DIAG_CD_TYP_CD_SK"))
    .alias("DIAG_CD_TYP_CD_SK"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD").isNull(), F.lit("UNK"))
    .otherwise(F.col("refProcCd.PROC_CD_TYP_CD"))
    .alias("PROC_CD_TYP_CD"),
    F.when(F.col("refProcCd.PROC_CD_TYP_NM").isNull(), F.lit("UNKNOWN"))
    .otherwise(F.col("refProcCd.PROC_CD_TYP_NM"))
    .alias("PROC_CD_TYP_NM"),
    F.when(F.col("refProcCd.PROC_CD_TYP_CD_SK").isNull(), F.lit(0))
    .otherwise(F.col("refProcCd.PROC_CD_TYP_CD_SK"))
    .alias("PROC_CD_TYP_CD_SK"),
)

# WRITE TO SEQ FILE (CSeqFileStage) - CASE_MGT_D.dat
write_files(
    df_final,
    f"{adls_path}/load/CASE_MGT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)